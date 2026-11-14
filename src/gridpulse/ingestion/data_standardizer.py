import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DataStandardizer:
    """
    Normalizes raw entsoe-py Pandas outputs into strict, long-format DataFrames
    that match our PostgreSQL SQLAlchemy models.
    """

    @staticmethod
    def _standardize_prices(data: pd.Series, country_code: str) -> pd.DataFrame:
        df = data.to_frame(name="price").reset_index()
        # Safely rename whatever the first column is to datetime_utc
        df.rename(columns={df.columns[0]: "datetime_utc"}, inplace=True)
        df["country_code"] = country_code
        df["currency"] = "EUR"
        return df

    @staticmethod
    def _standardize_load(
        data: pd.DataFrame, country_code: str, dataset_id: str
    ) -> pd.DataFrame:
        df = data.reset_index()
        df.rename(columns={df.columns[0]: "datetime_utc"}, inplace=True)

        col_name = "Actual Load" if dataset_id == "actual_load" else "Forecasted Load"
        if col_name in df.columns:
            df.rename(columns={col_name: "load_mw"}, inplace=True)
        elif len(df.columns) == 2:
            df.rename(columns={df.columns[1]: "load_mw"}, inplace=True)

        df["country_code"] = country_code
        return df

    @staticmethod
    def _standardize_generation(data: pd.DataFrame, country_code: str) -> pd.DataFrame:
        df = data.copy()

        # Flatten MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join(str(c) for c in col).strip("_") for col in df.columns.values
            ]

        # Reset index for the time axis to become a column
        df = df.reset_index()

        # Identify the datetime column by dtype
        datetime_col = next(
            col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])
        )
        if datetime_col is None:
            raise ValueError(
                "[DataStandardizer] Could not identify a datetime column in generation data. "
                f"Available columns: {list(df.columns)}"
            )

        df = df.rename(columns={datetime_col: "datetime_utc"})

        # Melt wide format (columns = PSR types) into long format
        df = df.melt(
            id_vars=["datetime_utc"], var_name="psr_type", value_name="generation_mw"
        )
        df["country_code"] = country_code
        df = df.dropna(subset=["generation_mw"])
        return df

    @staticmethod
    def _standardize_flows(
        data: pd.Series, in_domain: str, out_domain: str
    ) -> pd.DataFrame:
        df = data.to_frame(name="flow_mw").reset_index()
        df.rename(columns={df.columns[0]: "datetime_utc"}, inplace=True)
        df["in_domain"] = in_domain
        df["out_domain"] = out_domain
        return df

    @staticmethod
    def standardize(
        raw_data: pd.Series | pd.DataFrame | None,
        dataset_id: str,
        country_code: str,
        in_domain: str | None = None,
        out_domain: str | None = None,
    ) -> pd.DataFrame:
        if raw_data is None or raw_data.empty:
            return pd.DataFrame()

        if dataset_id == "day_ahead_prices":
            if not isinstance(raw_data, pd.Series):
                raise TypeError(
                    f"[DataStandardizer] Expected pd.Series for '{dataset_id}', "
                    f"got {type(raw_data).__name__}"
                )
            return DataStandardizer._standardize_prices(raw_data, country_code)

        if dataset_id in ("actual_load", "load_forecast"):
            if not isinstance(raw_data, pd.DataFrame):
                raise TypeError(
                    f"[DataStandardizer] Expected pd.DataFrame for '{dataset_id}', "
                    f"got {type(raw_data).__name__}"
                )
            return DataStandardizer._standardize_load(
                raw_data, country_code, dataset_id
            )

        if dataset_id in ("actual_generation", "renewable_forecast"):
            if not isinstance(raw_data, pd.DataFrame):
                raise TypeError(
                    f"[DataStandardizer] Expected pd.DataFrame for '{dataset_id}', "
                    f"got {type(raw_data).__name__}"
                )
            return DataStandardizer._standardize_generation(raw_data, country_code)

        if dataset_id in (
            "physical_cross_border_flows",
            "scheduled_commercial_exchanges",
        ):
            if not isinstance(raw_data, pd.Series):
                raise TypeError(
                    f"[DataStandardizer] Expected pd.Series for '{dataset_id}', "
                    f"got {type(raw_data).__name__}"
                )
            if in_domain is None or out_domain is None:
                raise ValueError(
                    f"[DataStandardizer] 'in_domain' and 'out_domain' are required "
                    f"for '{dataset_id}'"
                )
            return DataStandardizer._standardize_flows(raw_data, in_domain, out_domain)

        raise ValueError(
            f"[DataStandardizer] No standardizer implemented for dataset_id: {dataset_id}"
        )
