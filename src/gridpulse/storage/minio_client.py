import logging
from io import BytesIO
from typing import Literal

from minio import Minio
from minio.error import S3Error

from gridpulse.config import settings

logger = logging.getLogger(__name__)


Layer = Literal["bronze", "silver", "gold"]


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

        self.buckets: dict[Layer, str] = {
            "bronze": "gridpulse-bronze",
            "silver": "gridpulse-silver",
            "gold": "gridpulse-gold",
        }

        for bucket_name in self.buckets.values():
            self._ensure_bucket_exists(bucket_name)

    def _get_bucket(self, layer: Layer) -> str:
        """Returns the bucket name and storage layer."""
        try:
            return self.buckets[layer]
        except KeyError as exc:
            raise ValueError(
                f"[MinioClient] Unknown storage layer: {layer}."
                f"(Expected on of {list(self.buckets)})"
            ) from exc

    def _ensure_bucket_exists(self, bucket_name: str) -> None:
        """Creates the bronze bucket if it does not already exist."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"[MinioClient] Created MinIO bucket: {bucket_name}")
        except S3Error as e:
            logger.error(f"[MinioClient] Failed to create or check MinIO bucket: {e}")
            raise

    def upload_bytes(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        layer: Layer = "bronze",
    ) -> None:
        """Uploads raw bytes to MinIO."""
        bucket_name = self._get_bucket(layer)

        try:
            self.client.put_object(
                bucket_name,
                object_name,
                BytesIO(data),
                length=len(data),
                content_type=content_type,
            )
            logger.info(
                f"[MinioClient] Uploaded {len(data)} bytes to {bucket_name}/{object_name}"
            )
        except S3Error as e:
            logger.error(
                f"[MinioClient] Failed to upload {object_name} to {bucket_name}: {e}"
            )
            raise

    def download_bytes(self, object_name: str, layer: Layer = "bronze") -> bytes:
        """Downloads raw bytes from MinIO."""
        bucket_name = self._get_bucket(layer)

        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data

        except S3Error as e:
            logger.error(
                f"[MinioClient] Failed to download {object_name} from {bucket_name}: {e}"
            )
            raise

    def build_path(
        self,
        layer: Layer,
        source: str,
        dataset_id: str,
        execution_date: str,
        country_code: str | None = None,
        in_domain: str | None = None,
        out_domain: str | None = None,
        extension: str = "xml",
    ) -> str:
        """
        Constructs a standardized, Hive-style partitioned object path for a given layer.
        Supports both single-domain and directed-pair datasets.

        The layer selects the target bucket (see ``upload_bytes`` / ``download_bytes``)
        and is validated here, but is NOT embedded in the object name — callers keep
        using the layer-specific `source` prefix (e.g. ``entsoe`` for bronze,
        ``entsoe_silver`` for silver).

        Examples:
            Bronze single: entsoe/day_ahead_prices/year=2023/month=10/day=01/10Y1001A1001A82H.xml
            Bronze pair:   entsoe/physical_flows/year=2023/month=10/day=01/10Y1001A1001A82H_to_10YFR-RTE------C.xml
            Silver single: entsoe_silver/day_ahead_prices/year=2026/month=11/day=06/10Y1001A1001A82H.parquet
        """
        self._get_bucket(layer)

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
