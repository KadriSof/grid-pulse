import logging
from datetime import UTC, date, datetime, time, timedelta
from functools import reduce
from operator import or_

import pandas as pd
import pendulum
from airflow import DAG
from airflow.sdk import Asset, task

from gridpulse.ingestion.entsoe_service import EntsoeService
from gridpulse.storage.minio_client import MinioClient

logger = logging.getLogger(__name__)

ASSETS = {
    # P0: Core market fundamentals
    "day_ahead_prices": Asset("entsoe://bronze/day_ahead_prices"),
    "actual_load": Asset("entsoe://bronze/actual_load"),
    "load_forecast": Asset("entsoe://bronze/load_forecast"),
    "actual_generation": Asset("entsoe://bronze/actual_generation"),
    "renewable_forecast": Asset("entsoe://bronze/renewable_forecast"),
    # P1: Market coupling & cross-border
    "physical_cross_border_flows": Asset("entsoe://bronze/physical_cross_border_flows"),
    "scheduled_commercial_exchanges": Asset(
        "entsoe://bronze/scheduled_commercial_exchanges"
    ),
    # P2: Balancing & outages
    "imbalance_prices": Asset("entsoe://bronze/imbalance_prices"),
    "generation_outages": Asset("entsoe://bronze/generation_outages"),
}


# This allows an updateto any one of the assets to trigger the consumer
SCHEDULE_ASSETS = reduce(or_, ASSETS.values())

default_args = {
    "owner": "gridpulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="entsoe_silver_consumer",
    default_args=default_args,
    description="Transforms raw ENTSO-E Bronze data into structured Silver Parquet",
    start_date=pendulum.datetime(year=2026, month=11, day=6, tz="UTC"),
    schedule=SCHEDULE_ASSETS,
    catchup=False,
    tags=["entsoe", "silver", "transformation"],
    max_active_runs=5,
) as dag:

    @task
    def transform_to_silver(**context) -> str:
        """
        Consumes the triggered Asset metadata, fetches structured data,
        and saves it as partitioned Parquet in the silver layer.
        """
        # 1. Extract triggering asset events from Airflow context
        triggering_asset_events = context.get("triggering_asset_events", {})

        if not triggering_asset_events:
            raise ValueError(
                "[EntsoeSilverConsumer] No triggering asset events found. This task must be triggered by an Asset."
            )

        service = EntsoeService()
        storage = MinioClient()

        output_paths = []

        # 2. Iterate over all triggered assets (handles single or batch triggers)
        for asset_obj, events in triggering_asset_events.items():
            for event in events:
                extra = event.extra
                dataset_id = extra.get("dataset_id")
                country_code = extra.get("country_code")
                in_domain = extra.get("in_domain")
                out_domain = extra.get("out_domain")
                ds = extra.get("date")

                if not dataset_id or not ds:
                    logger.warning(
                        f"[EntsoeSilverConsumer] Skipping event with incomplete metadata: {extra}"
                    )
                    continue

                target_country = (
                    in_domain if (in_domain and out_domain) else country_code
                )
                geo_label = (
                    f"{in_domain} -> {out_domain}"
                    if (in_domain and out_domain)
                    else str(country_code)
                )

                try:
                    # 3. Fetch structured Dataframe using the SDK parser
                    execution_date = date.fromisoformat(ds)
                    start = datetime.combine(execution_date, time.min, tzinfo=UTC)
                    end = start + timedelta(days=1)

                    df = service.fetch_structured_data(
                        dataset_id=dataset_id,
                        country_code=target_country,
                        start=start,
                        end=end,
                        out_domain=out_domain,
                    )

                    # 4. Data Quality & Standardization for Silver Layer
                    if df.empty:
                        logger.warning(
                            f"[EntsoeSilverConsumer] Empty Dataframe for {dataset_id} / {geo_label} on {ds}"
                        )
                        continue

                    df = df.reset_index()
                    if "index" in df.columns:
                        df.rename(columns={"index": "datetime"}, inplace=True)
                    elif "level_0" in df.columns:
                        df.rename(columns={"level_0": "datetime"}, inplace=True)

                    # Add pipeline metadata columns for lineage and auditing
                    df["dataset_id"] = dataset_id
                    df["country_code"] = country_code
                    df["in_domain"] = in_domain
                    df["out_domain"] = out_domain
                    df["ingestion_timestamp"] = datetime.now(tz=UTC).isoformat()

                    # Ensure datetime column is timezone-aware UTC
                    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

                    # 5. Save to MinIO Silver Layer as Parquet
                    object_path = storage.build_path(
                        layer="silver",
                        source="entsoe",
                        dataset_id=dataset_id,
                        execution_date=ds,
                        country_code=country_code,
                        in_domain=in_domain,
                        out_domain=out_domain,
                        extension="parquet",
                    )

                    parquet_buffer = df.to_parquet(index=False, engine="pyarrow")

                    storage.upload_bytes(
                        object_name=object_path,
                        data=parquet_buffer,
                        content_type="application/vnd.apache.parquet",
                    )

                    logger.info(
                        f"[EntsoeSilverConsumer] Successfully stored Silver data at: {object_path}"
                    )
                    output_paths.append(object_path)

                except Exception as e:
                    logger.error(
                        f"[EntsoeSilverConsumer] Failed to process {dataset_id} / {country_code}:\n----{e}\n----"
                    )
                    raise

        if not output_paths:
            logger.warning(
                "[EntsoeSilverConsumer] No data was processed or saved in this run."
            )

        return ",".join(output_paths)


transform_to_silver()
