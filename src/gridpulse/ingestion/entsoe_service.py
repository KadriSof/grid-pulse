import logging

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
from gridpulse.config import entsoe_config

logger = logging.getLogger(__name__)


class EntsoeServiceError(Exception):
    """Custom exception for EntsoeService failures."""


class EntsoeService:
    """
    Business logic layer for querying ENTSO-E data.
    """

    def __init__(self) -> None:
        self.client = EntsoeClient()

    def _build_kwargs(
        self,
        dataset_id: str,
        country_code: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        out_domain: str | None = None,
        psr_type: str | None = None,
    ) -> dict:
        """Dynamically build kwargs based on the YAML contract."""
        dataset_cfg = entsoe_config.datasets[dataset_id]
        kwargs = {"start": start, "end": end}

        if dataset_cfg.api.requires_out_domain and out_domain:
            kwargs["country_code"] = country_code
            kwargs["country_code_neighbour"] = out_domain
        else:
            kwargs["country_code"] = country_code

        if psr_type:
            kwargs["psr_type"] = psr_type

        return kwargs

    def _handle_sdk_exceptions(self, e: Exception, dataset_id: str, country_code: str):
        """Map SDK exceptions to clean application-level errors."""
        if isinstance(e, NoMatchingDataError):
            # Re-raise as it's a valid business state (e.g., no data for this period)
            raise NoMatchingDataError(f"No data for {dataset_id} in {country_code}")
        elif isinstance(
            e,
            (
                PaginationError,
                InvalidPSRTypeError,
                InvalidBusinessParameterError,
                InvalidParameterError,
                requests.HTTPError,
            ),
        ):
            raise EntsoeServiceError(
                f"API Error for {dataset_id} in {country_code}: {e}"
            )
        else:
            raise EntsoeServiceError(
                f"Unexpected error for {dataset_id} in {country_code}: {e}"
            )

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
        dataset_cfg = entsoe_config.datasets[dataset_id]
        start_ts = self.client.ensure_tz_aware(start)
        end_ts = self.client.ensure_tz_aware(end)

        # FALLBACK: Use `_base_request` if explicitly configured in YAML
        if dataset_cfg.api.use_base_request and dataset_cfg.api.base_request_params:
            logger.warning(
                f"Using `_base_request` SDK method as fallback for {dataset_id}"
            )
            params = dataset_cfg.api.base_request_params.copy()

            if dataset_cfg.api.requires_out_domain and out_domain:
                params["in_Domain"] = country_code
                params["out_Domain"] = out_domain
            else:
                params["in_Domain"] = country_code

            try:
                response = self.client.raw_client._base_request(
                    params=params, start=start_ts, end=end_ts
                )
                response.raise_for_status()

                if response.headers.get("Content-type", "").startswith(
                    "application/zip"
                ):
                    return response.content

                return response.text

            except requests.exceptions.HTTPError as e:
                if "No matching data found" in response.text:
                    raise NoMatchingDataError(
                        f"No data for {dataset_id} in {country_code}"
                    )
                self._handle_sdk_exceptions(e, dataset_id, country_code)

            except Exception as e:
                self._handle_sdk_exceptions(e, dataset_id, country_code)

        method_name = dataset_cfg.api.entsoe_py_method
        api_method = getattr(self.client.raw_client, method_name, None)

        if not api_method:
            raise EntsoeServiceError(
                f"Method {method_name} is not provided by EntsoeRawClient"
            )

        kwargs = self._build_kwargs(
            dataset_id, country_code, start_ts, end_ts, out_domain, psr_type
        )

        try:
            logger.info(
                f"Fetching raw data via {method_name} for {dataset_id} in {country_code}"
            )
            return api_method(**kwargs)

        except Exception as e:
            self._handle_sdk_exceptions(e, dataset_id, country_code)

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
        Fetches structured Pandas DataFrame for the Silver layer analytics and validation.
        """
        dataset_cfg = entsoe_config.datasets[dataset_id]
        start_ts = self.client.ensure_tz_aware(start)
        end_ts = self.client.ensure_tz_aware(end)

        method_name = dataset_cfg.api.entsoe_py_method
        api_method = getattr(self.client.pandas_client, method_name, None)

        if not api_method:
            raise EntsoeServiceError(
                f"Method {method_name} not found on EntsoePandasClient"
            )

        kwargs = self._build_kwargs(
            dataset_id, country_code, start_ts, end_ts, out_domain, psr_type
        )

        try:
            logger.info(
                f"Fetching DataFrame via {method_name} for {dataset_id} in {country_code}"
            )
            return api_method(**kwargs)

        except Exception as e:
            self._handle_sdk_exceptions(e, dataset_id, country_code)
