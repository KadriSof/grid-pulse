from datetime import datetime

import pandas as pd
from entsoe.entsoe import EntsoePandasClient, EntsoeRawClient

from gridpulse.config import settings

DateTimeLike = pd.Timestamp | datetime | str


class EntsoeClient:
    """
    Encapsulates, configures, and exposes the ENTSO-E Raw and Pandas clients.
    """

    def __init__(self):
        if not settings.entsoe_api_key:
            raise ValueError(
                "GRIDPULSE_ENTSOE_API_KEY is not set in environment variables."
            )

        # Initialize both clients from the SDK.
        # The SDK handles its own retries, timeouts, and pagination by default.
        self.raw_client = EntsoeRawClient(api_key=settings.entsoe_api_key)
        self.pandas_client = EntsoePandasClient(api_key=settings.entsoe_api_key)

    def ensure_tz_aware(self, dt: DateTimeLike) -> pd.Timestamp:
        """Ensure the datetime is a timezone-aware pd.Timestamp in UTC."""
        ts = pd.Timestamp(dt)
        if not isinstance(ts, pd.Timestamp):  # excludes NaTType
            raise TypeError(f"[EntsoeClient] Invalid datetime input: {dt!r}")
        return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")
