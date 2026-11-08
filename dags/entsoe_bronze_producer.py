import logging

import pendulum
from airflow import DAG
from airflow.sdk import Asset, CronDataIntervalTimetable, task
from airflow.sdk.exceptions import AirflowSkipException
from entsoe.exceptions import NoMatchingDataError

from gridpulse.config import entsoe_config

logger = logging.getLogger(__name__)

ASSETS_P0 = {
    "day_ahead_prices": Asset("entsoe://bronze/day_ahead_prices"),
    "actual_load": Asset("entsoe://bronze/actual_load"),
    "load_forecast": Asset("entsoe://bronze/load_forecast"),
    "actual_generation": Asset("entsoe://bronze/actual_generation"),
    "renewable_forecast": Asset("entsoe://bronze/renewable_forecast"),
}

default_args = {
    "owner": "gridpulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="entsoe_bronze_producer",
    default_args=default_args,
    description="Fetches raw ENTSO-E data and stores it in MinIO Bronze layer",
    start_date=pendulum.datetime(year=2026, month=11, day=6, tz="UTC"),
    # Schedule at 14:00 UTC daily, after Day-Ahead prices are reliably published
    schedule=CronDataIntervalTimetable("0 14 * * *", timezone="UTC"),
    catchup=False,
    tags=["entsoe", "bronze", "ingestion"],
    max_active_runs=3,
) as dag:

    @task(task_id="generate_extraction_params")
    def get_extraction_params(**context) -> list[dict]:
        """
        Generates the parameter list for dynamic task mapping.
        Reads directly from the YAML config to ensure a single source of truth.
        """
        ds = context["ds"]
        params = []
        # Calculate period: previous day relative to logical execution date (ds)
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        start_str = data_interval_start.strftime("%Y%m%d%H%M")
        end_str = data_interval_end.strftime("%Y%m%d%H%M")

        for dataset_id, cfg in entsoe_config.datasets.items():
            if cfg.priority == "P0":
                eic_codes = cfg.geography.sample_eic_codes or []
                for code in eic_codes:
                    params.append(
                        {
                            "dataset_id": dataset_id,
                            "country_code": code,
                            "start": start_str,
                            "end": end_str,
                            "ds": ds,
                        }
                    )

        return params

    @task
    def fetch_and_store_bronze(
        dataset_id: str,
        country_code: str,
        start: str,
        end: str,
        ds: str,
    ) -> Asset:
        """
        Dynamic task: fetches raw data (XML or ZIP) for one dataset/zone
        and stores it in MinIO. Returns an Asset with metadata to trigger
        downstream consumers.
        """
        from gridpulse.ingestion.entsoe_service import (
            EntsoeService,
            EntsoeServiceError,
        )
        from gridpulse.storage.minio_client import MinioClient

        service = EntsoeService()
        storage = MinioClient()
        target_asset = ASSETS_P0[dataset_id]

        # 0. Construct partitioned path (e.g., entsoe/day_ahead_prices/year=2023/...)
        object_path = storage.build_bronze_path(
            source="entsoe",
            dataset_id=dataset_id,
            country_code=country_code,
            execution_date=ds,
        )

        try:
            logger.info(
                f"[EntsoeBronzeProducer] Fetching {dataset_id} for {country_code} ({ds})"
            )

            # 1. Fetch raw data using the configuration-driven service
            raw_data = service.fetch_bronze_data(
                dataset_id=dataset_id,
                country_code=country_code,
                start=start,
                end=end,
            )

            # 2. Determine content type based on data type (bytes for ZIP, str for XML)
            content_type = (
                "application/zip" if isinstance(raw_data, bytes) else "application/xml"
            )
            data_to_upload = (
                raw_data if isinstance(raw_data, bytes) else raw_data.encode("utf-8")
            )

            # 3. Upload to MinIO Bronze layer
            storage.upload_bytes(
                object_name=object_path,
                data=data_to_upload,
                content_type=content_type,
            )

            logger.info(
                f"[EntsoeBronzeProducer] Successfully stored Bronze data at: {object_path}"
            )

            # 4. EMIT ASSET WITH METADATA
            # This is the magic that triggers the consumer and passes the exact date/path
            return Asset(
                target_asset.uri,
                extra={
                    "dataset_id": dataset_id,
                    "country_code": country_code,
                    "date": ds,
                    "object_path": object_path,
                },
            )

        except NoMatchingDataError:
            logger.warning(
                f"[EntsoeBronzeProducer] Skipped (No Data): {dataset_id} / {country_code} on {ds}"
            )
            # AirflowSkipException prevents the DAG from failing when data is legitimately absent
            raise AirflowSkipException(
                f"[EntsoeBronzeProducer] No data available for {country_code} in this period."
            )

        except EntsoeServiceError as e:
            logger.error(
                f"[EntsoeBronzeProducer] Service Error: {dataset_id} / {country_code} - {e}"
            )
            raise

        except Exception as e:
            logger.error(
                f"[EntsoeBronzeProducer] Unexpected Error: {dataset_id} / {country_code} - {e}"
            )
            raise

    # 1. Get the execution parameters (runs once per DAG run)
    params = get_extraction_params()

    # 2. Execute dynamically mapped tasks
    fetch_and_store_bronze.expand_kwargs(params)
