import logging
from typing import Any, NoReturn

import pandas as pd
import requests
from entsoe.exceptions import (
    InvalidBusinessParameterError,
    InvalidParameterError,
    InvalidPSRTypeError,
    NoMatchingDataError,
    PaginationError,
)

from gridpulse.api.entsoe_client import DateTimeLike, EntsoeClient
from gridpulse.config import DatasetConfig, entsoe_config

logger = logging.getLogger(__name__)


class EntsoeServiceError(Exception):
    """Custom exception for EntsoeService failures."""


class EntsoeService:
    """
    Business logic layer for querying ENTSO-E data.
    """

    def __init__(self, client: EntsoeClient | None = None) -> None:
        self.client = client if client is not None else EntsoeClient()

    # Configuration
    def _get_dataset(self, dataset_id: str):
        try:
            return entsoe_config.datasets[dataset_id]

        except KeyError as exc:
            raise ValueError(
                f"[EntsoeService] Unknown ENTSO-E dataset: {dataset_id}"
            ) from exc

    # Request validation
    def ensure_tz_aware(self, dt: DateTimeLike) -> pd.Timestamp:
        """Ensure the datetime is a timezone-aware pd.Timestamp in UTC."""
        ts = pd.Timestamp(dt)
        if not isinstance(ts, pd.Timestamp):  # excludes NaTType
            raise TypeError(f"[EntsoeClient] Invalid datetime input: {dt!r}")
        return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")

    def _normalize_time_range(
        self, start: DateTimeLike, end: DateTimeLike
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """
        Normalize timestamps to UTC and validate the requested interval.
        """
        start_ts = self.ensure_tz_aware(start)
        end_ts = self.ensure_tz_aware(end)

        if start_ts >= end_ts:
            raise ValueError(
                f"[EntsoeService] start must be earlier than end: [{start_ts}-{end_ts}]"
            )

        return start_ts, end_ts

    def _validate_geography(
        self,
        dataset_cfg: DatasetConfig,
        country_code: str | None = None,
        out_domain: str | None = None,
    ) -> None:
        """
        Validate geography based on dataset's declared geo level.
        """
        geography_level = dataset_cfg.geography.level

        if geography_level == "directed_border_pair":
            if not country_code or not out_domain:
                raise ValueError(
                    "[EntsoeService] Both country_code and out_domain are required for directed-pair datasets"
                )

        elif not country_code:
            raise ValueError(
                f"[EntsoeService] country_code is required for dataset geography {geography_level!r}"
            )

    # SDK request construction
    def _build_kwargs(
        self,
        dataset_cfg: DatasetConfig,
        start: pd.Timestamp,
        end: pd.Timestamp,
        country_code: str | None = None,
        out_domain: str | None = None,
        psr_type: str | None = None,
    ) -> dict:
        """Dynamically build kwargs based on the YAML contract."""
        self._validate_geography(
            dataset_cfg=dataset_cfg, country_code=country_code, out_domain=out_domain
        )

        kwargs: dict[str, Any] = {"start": start, "end": end}

        if dataset_cfg.geography.level == "directed_border_pair":
            kwargs["country_code_from"] = country_code
            kwargs["country_code_to"] = out_domain

        else:
            kwargs["country_code"] = country_code

        if psr_type is not None:
            kwargs["psr_type"] = psr_type

        return kwargs

    def _handle_sdk_exceptions(
        self,
        exc: Exception,
        dataset_id: str,
        country_code: str,
        out_domain: str | None = None,
    ) -> NoReturn:
        """Translate SDK/network errors into application-level errors."""
        context = f"dataset={dataset_id}, country={country_code}"

        if out_domain:
            context += f", out_domain={out_domain}"

        if isinstance(exc, NoMatchingDataError):
            raise exc

        sdk_errors = (
            PaginationError,
            InvalidPSRTypeError,
            InvalidBusinessParameterError,
            InvalidParameterError,
            requests.HTTPError,
            requests.RequestException,
        )

        if isinstance(exc, sdk_errors):
            raise EntsoeServiceError(
                f"[EntsoeService] ENTSO-E request failed ({context}): {exc}"
            ) from exc

        raise EntsoeServiceError(
            f"[EntsoeService] Unexpected ENTSO-E error ({context}: {exc})"
        ) from exc

    # Bronze retrieval
    def _fetch_bronze_via_base_request(
        self,
        dataset_cfg: DatasetConfig,
        dataset_id: str,
        country_code: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        out_domain: str | None,
    ) -> str | bytes:
        """
        Fetch raw XML/ZIP using the SDK's `_base_request` method.
        """
        params = dataset_cfg.api.base_request_params.copy()

        if dataset_cfg.geography.level == "directed_border_pair":
            if not out_domain:
                raise ValueError(
                    f"[EntsoeService] out_domain is required for dataset {dataset_id!r}"
                )

            params["in_Domain"] = country_code
            params["out_Domain"] = out_domain
        else:
            params["in_Domain"] = country_code

        try:
            response = self.client.raw_request(params=params, start=start, end=end)
            response.raise_for_status()

        except requests.HTTPError as exc:
            error_response = exc.response
            if (
                error_response is not None
                and "no matching data found" in error_response.text.lower()
            ):
                raise NoMatchingDataError(
                    f"[EntsoeService] No data for '{dataset_id}' in '{country_code}'"
                ) from exc

            self._handle_sdk_exceptions(
                exc=exc,
                dataset_id=dataset_id,
                country_code=country_code,
                out_domain=out_domain,
            )

        except Exception as exc:  # noqa: BLE001
            self._handle_sdk_exceptions(
                exc=exc,
                dataset_id=dataset_id,
                country_code=country_code,
                out_domain=out_domain,
            )

        content_type = response.headers.get("Content-Type", "").lower()

        if "application/zip" in content_type:
            return response.content

        return response.text

    def fetch_bronze_data(
        self,
        dataset_id: str,
        country_code: str,
        start: DateTimeLike,
        end: DateTimeLike,
        out_domain: str | None = None,
        psr_type: str | None = None,
    ) -> str | bytes:
        """Fetches raw data (XML string or ZIP bytes) for the Bronze layer."""
        dataset_cfg = self._get_dataset(dataset_id=dataset_id)
        start_ts, end_ts = self._normalize_time_range(start=start, end=end)

        if dataset_cfg.api.use_base_request:
            if not dataset_cfg.api.base_request_params:
                raise EntsoeServiceError(
                    f"[EntsoeService] Dataset '{dataset_id!r}' enables base_request_params "
                    f"but has no base_request_params configured."
                )

            logger.warning(
                f"[EntsoeService] Using ENTSO-E `_base_request` fallback for {dataset_id}",
            )

            return self._fetch_bronze_via_base_request(
                dataset_cfg=dataset_cfg,
                dataset_id=dataset_id,
                country_code=country_code,
                start=start_ts,
                end=end_ts,
                out_domain=out_domain,
            )

        method_name = dataset_cfg.api.entsoe_py_method
        api_method = getattr(self.client.raw_client, method_name, None)

        if api_method is None:
            raise EntsoeServiceError(
                f"[EntsoeService] raw SDK method {method_name!r} is not available."
            )

        kwargs = self._build_kwargs(
            dataset_cfg=dataset_cfg,
            start=start_ts,
            end=end_ts,
            country_code=country_code,
            out_domain=out_domain,
            psr_type=psr_type,
        )

        try:
            logger.info(
                f"[EntsoeService] Fetching Bronze data via {method_name} for {dataset_id}"
            )
            return api_method(**kwargs)

        except Exception as exc:  # noqa: BLE001
            self._handle_sdk_exceptions(
                exc=exc,
                dataset_id=dataset_id,
                country_code=country_code,
                out_domain=out_domain,
            )

    def fetch_silver_dataframe(
        self,
        dataset_id: str,
        country_code: str,
        start: DateTimeLike,
        end: DateTimeLike,
        out_domain: str | None = None,
        psr_type: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch structured Pandas data for Silver.
        """
        dataset_cfg = self._get_dataset(dataset_id)
        start_ts, end_ts = self._normalize_time_range(start, end)

        method_name = dataset_cfg.api.entsoe_py_method
        api_method = getattr(self.client.pandas_client, method_name, None)

        if api_method is None:
            raise EntsoeServiceError(
                f"Pandas SDK method {method_name!r} is not available."
            )

        kwargs = self._build_kwargs(
            dataset_cfg=dataset_cfg,
            start=start_ts,
            end=end_ts,
            country_code=country_code,
            out_domain=out_domain,
            psr_type=psr_type,
        )

        try:
            logger.info(
                "Fetching Silver DataFrame via %s for %s",
                method_name,
                dataset_id,
            )
            return api_method(**kwargs)

        except Exception as exc:  # noqa: BLE001
            self._handle_sdk_exceptions(
                exc,
                dataset_id,
                country_code,
                out_domain,
            )
