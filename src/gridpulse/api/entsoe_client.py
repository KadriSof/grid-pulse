from datetime import datetime
from typing import Any

import pandas as pd
from entsoe.entsoe import EntsoePandasClient, EntsoeRawClient
from requests import Response

from gridpulse.config import settings

DateTimeLike = pd.Timestamp | datetime | str


class EntsoeClient:
    """
    Encapsulates, configures, and exposes the ENTSO-E Raw and Pandas clients.
    """

    def __init__(self, api_key: str | None = None):
        api_key = api_key or settings.entsoe_api_key

        if not api_key:
            raise ValueError(
                "GRIDPULSE_ENTSOE_API_KEY is not set in environment variables."
            )

        # Initialize both clients from the SDK.
        # The SDK handles its own retries, timeouts, and pagination by default.
        self.raw_client = EntsoeRawClient(api_key=api_key)
        self.pandas_client = EntsoePandasClient(api_key=api_key)

    def raw_request(
        self,
        params: dict[str, Any],
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> Response:
        """
        Execute a raw ENTSO-E API request through the SDK's
        underlying request mechanism.

        Intended for API operations not exposed by entsoe-py.
        """
        if start >= end:
            raise ValueError("start must be earlier than end")

        return self.raw_client._base_request(
            params=params.copy(),
            start=start,
            end=end,
        )
