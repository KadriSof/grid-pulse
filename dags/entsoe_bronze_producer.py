import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.sdk import Asset, CronDataIntervalTimetable, task
from airflow.sdk.exceptions import AirflowSkipException
from entsoe.exceptions import NoMatchingDataError

from gridpulse.config import entsoe_config
from gridpulse.ingestion.entsoe_service import EntsoeService, EntsoeServiceError
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

ZIP_DATASETS = {"imbalance_prices", "generation_outages"}

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
    description="Fetches raw ENTSO-E data (P0/P1/P2) and stores it in MinIO Bronze layer",
    start_date=pendulum.datetime(year=2026, month=11, day=6, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 14 * * *", timezone="UTC"),
    catchup=False,
    tags=["entsoe", "bronze", "ingestion"],
    max_active_runs=3,
) as dag:

    @task(task_id="generate_extraction_params")
    def get_extraction_params(**context) -> list[dict]:
        """Generates the parameter list for dynamic task mapping."""
        ds = context["ds"]
        params = []
        start_dt = datetime.strptime(ds, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=1)
        start_str = start_dt.strftime("%Y%m%d%H%M")
        end_str = end_dt.strftime("%Y%m%d%H%M")

        for dataset_id, cfg in entsoe_config.datasets.items():
            geography = cfg.geography

            # P1 datasets use directed border pairs
            if geography.level == "directed_border_pair" and geography.sample_pairs:
                for pair in geography.sample_pairs:
                    if len(pair) != 2:
                        logger.warning(
                            f"[EntsoeBronzeProducer] Invalid pair for {dataset_id}: {pair}"
                        )
                        continue
                    in_domain, out_domain = pair
                    params.append(
                        {
                            "dataset_id": dataset_id,
                            "in_domain": in_domain,
                            "out_domain": out_domain,
                            "country_code": None,
                            "start": start_str,
                            "end": end_str,
                            "ds": ds,
                        }
                    )
            # P0/P2 datasets use single EIC codes
            elif geography.sample_eic_codes:
                for code in geography.sample_eic_codes:
                    params.append(
                        {
                            "dataset_id": dataset_id,
                            "country_code": code,
                            "in_domain": None,
                            "out_domain": None,
                            "start": start_str,
                            "end": end_str,
                            "ds": ds,
                        }
                    )
            else:
                logger.warning(
                    f"[EntsoeBronzeProducer] No geography configured for {dataset_id}, skipping"
                )

        logger.info(
            f"[EntsoeBronzeProducer] Generated {len(params)} extraction tasks for {ds}"
        )
        return params

    @task(outlets=list(ASSETS.values()))
    def fetch_and_store_bronze(
        dataset_id: str,
        start: str,
        end: str,
        ds: str,
        country_code: str,
        in_domain: str | None = None,
        out_domain: str | None = None,
        *,
        outlet_events: dict,
    ) -> str:
        """Dynamic task: fetches raw data and stores it in MinIO."""
        service = EntsoeService()
        storage = MinioClient()
        target_asset = ASSETS[dataset_id]

        geo_label = (
            f"{in_domain} -> {out_domain}"
            if (in_domain and out_domain)
            else str(country_code)
        )
        target_country = in_domain if (in_domain and out_domain) else country_code

        try:
            logger.info(
                f"[EntsoeBronzeProducer] Fetching {dataset_id} | geography:{geo_label} "
                f"| start:{start} | end: {end} | ds: {ds}"
            )

            # 1. Fetch raw data using the service
            raw_data = service.fetch_raw_data(
                dataset_id=dataset_id,
                country_code=target_country,
                start=start,
                end=end,
                out_domain=out_domain,
            )

            # 2. Determine content type and extension
            extension = "zip" if dataset_id in ZIP_DATASETS else "xml"
            if isinstance(raw_data, bytes):
                content_type = "application/zip"
                data_to_upload = raw_data
            else:
                content_type = "application/xml"
                data_to_upload = raw_data.encode("utf-8")

            # 3. Build path and upload
            object_path = storage.build_path(
                layer="bronze",
                source="entsoe",
                dataset_id=dataset_id,
                execution_date=ds,
                country_code=country_code,
                in_domain=in_domain,
                out_domain=out_domain,
                extension=extension,
            )

            storage.upload_bytes(
                object_name=object_path,
                data=data_to_upload,
                content_type=content_type,
                layer="bronze",
            )

            logger.info(
                f"[EntsoeBronzeProducer] Stored data at: {object_path} ({len(data_to_upload)} bytes)"
            )

            # 4. Emit the asset event with JSON-serializable metadata
            outlet_events[target_asset].extra = {
                "dataset_id": dataset_id,
                "country_code": country_code,
                "in_domain": in_domain,
                "out_domain": out_domain,
                "date": ds,
                "object_path": object_path,
                "content_type": content_type,
            }

            return object_path

        except NoMatchingDataError:
            logger.warning(
                f"[EntsoeBronzeProducer] Skipped (No Data): {dataset_id} / {geo_label} on {ds}"
            )
            raise AirflowSkipException(
                f"[EntsoeBronzeProducer] No data available for {geo_label} in this period."
            )
        except EntsoeServiceError as e:
            logger.error(
                f"[EntsoeBronzeProducer] Service Error: {dataset_id} / {geo_label} - {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"[EntsoeBronzeProducer] Unexpected Error: {dataset_id} / {geo_label} - {e}"
            )
            raise

    # Orchestration
    params = get_extraction_params()
    fetch_and_store_bronze.expand_kwargs(params)
