import logging
from io import BytesIO

from minio import Minio
from minio.error import S3Error

from gridpulse.config import settings

logger = logging.getLogger(__name__)


class MinioClient:
    """Production-grade client for S3-compatible MinIO storage."""

    def __init__(self):
        # TODO: change the endpoint argument to `minio:8333` in the settings
        endpoint = settings.minio_endpoint.replace("http://", "").replace(
            "https://", ""
        )
        self.client = Minio(
            endpoint="minio:8333",
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_endpoint.startswith("https"),
        )
        self.bucket_name = "gridpulse-bronze"
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Creates the bronze bucket if it does not already exist."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"[MinioClient] Created MinIO bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"[MinioClient] Failed to create or check MinIO bucket: {e}")
            raise

    def upload_bytes(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Uploads raw bytes to MinIO."""
        try:
            self.client.put_object(
                self.bucket_name,
                object_name,
                BytesIO(data),
                length=len(data),
                content_type=content_type,
            )
            logger.info(
                f"[MinioClient] Uploaded {len(data)} bytes to {self.bucket_name}/{object_name}"
            )
        except S3Error as e:
            logger.error(f"[MinioClient] Failed to upload {object_name}: {e}")
            raise

    def download_bytes(self, object_name: str) -> bytes:
        """Downloads raw bytes from MinIO."""
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data

        except S3Error as e:
            logger.error(f"[MinioClient] Failed to download {object_name}: {e}")
            raise

    def build_bronze_path(
        self,
        source: str,
        dataset_id: str,
        country_code: str,
        execution_date: str,
        in_domain: str | None = None,
        out_domain: str | None = None,
        extension: str = "xml",
    ) -> str:
        """
        Constructs a standardized, Hive-style partitioned Bronze path.
        Supports both single-domain and directed-pair datasets.

        Examples:
            Single: entsoe/day_ahead_prices/year=2023/month=10/day=01/10Y1001A1001A82H.xml
            Pair:   entsoe/physical_flows/year=2023/month=10/day=01/10Y1001A1001A82H_to_10YFR-RTE------C.xml
        """
        year, month, day = execution_date.split("-")

        if in_domain and out_domain:
            filename = f"{in_domain}_to_{out_domain}.{extension}"
        elif country_code:
            filename = f"{country_code}.{extension}"
        else:
            raise ValueError(
                "[MinioClient] Must provide either country_code or (in_domain, out_domain)"
            )

        return f"{source}/{dataset_id}/year={year}/month={month}/day={day}/{filename}"

    def build_silver_path(
        self, source: str, dataset_id: str, country_code: str, execution_date: str
    ) -> str:
        """
        Constructs a standardized, Hive-style partitioned Silver path.
        Example: entsoe_silver/day_ahead_prices/year=2026/month=11/day=06/10Y1001A1001A82H.parquet
        """
        year, month, day = execution_date.split("-")
        return f"{source}/{dataset_id}/year={year}/month={month}/day={day}/{country_code}.parquet"
