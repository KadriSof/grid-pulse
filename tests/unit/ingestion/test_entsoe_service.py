from unittest.mock import MagicMock

import pandas as pd
import pytest
from entsoe.exceptions import NoMatchingDataError

from gridpulse.api.entsoe_client import EntsoeClient
from gridpulse.ingestion.entsoe_service import EntsoeService


@pytest.fixture
def mock_entsoe_client():
    client = MagicMock(spec=EntsoeClient)
    client.raw_client = MagicMock()
    client.pandas_client = MagicMock()
    # Mock ensure_tz_aware to return a valid UTC timestamp
    client.ensure_tz_aware = lambda dt: pd.Timestamp(dt, tz="UTC")
    return client


@pytest.fixture
def service(mock_entsoe_client):
    return EntsoeService(client=mock_entsoe_client)


def test_get_dataset_success(service):
    dataset = service._get_dataset("day_ahead_prices")
    assert dataset.id == "entsoe_day_ahead_prices"


def test_get_dataset_unknown(service):
    with pytest.raises(ValueError, match="Unknown ENTSO-E dataset"):
        service._get_dataset("unknown_dataset")


def test_normalize_time_range_success(service):
    start, end = service._normalize_time_range("2023-10-01", "2023-10-02")
    assert start < end
    assert start.tz is not None


def test_normalize_time_range_invalid(service):
    with pytest.raises(ValueError, match="start must be earlier than end"):
        service._normalize_time_range("2023-10-02", "2023-10-01")


def test_validate_geography_success_single(service):
    dataset_cfg = service._get_dataset("day_ahead_prices")
    service._validate_geography(dataset_cfg, country_code="10Y1001A1001A82H")


def test_validate_geography_success_pair(service):
    dataset_cfg = service._get_dataset("physical_cross_border_flows")
    service._validate_geography(
        dataset_cfg, country_code="10Y1001A1001A82H", out_domain="10YFR-RTE------C"
    )


def test_validate_geography_missing_pair(service):
    dataset_cfg = service._get_dataset("physical_cross_border_flows")
    with pytest.raises(
        ValueError, match="Both country_code and out_domain are required"
    ):
        service._validate_geography(dataset_cfg, country_code="10Y1001A1001A82H")


def test_fetch_bronze_data_normal(service, mock_entsoe_client):
    dataset = service._get_dataset("day_ahead_prices")
    # Explicitly disable fallback to test the normal SDK method path
    dataset.api.use_base_request = False

    mock_api_method = MagicMock(return_value="<xml>data</xml>")
    mock_entsoe_client.raw_client.query_day_ahead_prices = mock_api_method

    result = service.fetch_bronze_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )
    assert result == "<xml>data</xml>"
    mock_api_method.assert_called_once()


def test_fetch_bronze_data_base_request_fallback(service, mock_entsoe_client):
    dataset = service._get_dataset("day_ahead_prices")
    # Force fallback path
    dataset.api.use_base_request = True
    dataset.api.base_request_params = {"documentType": "A44", "processType": "A01"}

    mock_response = MagicMock()
    mock_response.text = "<xml>data</xml>"
    mock_response.headers = {"Content-Type": "application/xml"}
    mock_entsoe_client.raw_request.return_value = mock_response

    result = service.fetch_bronze_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )
    assert result == "<xml>data</xml>"
    mock_entsoe_client.raw_request.assert_called_once()


def test_fetch_bronze_data_no_matching_data(service, mock_entsoe_client):
    dataset = service._get_dataset("day_ahead_prices")
    # Explicitly disable fallback to test the normal SDK method path
    dataset.api.use_base_request = False

    mock_entsoe_client.raw_client.query_day_ahead_prices.side_effect = (
        NoMatchingDataError("No data for day_ahead_prices in 10Y1001A1001A82H")
    )

    with pytest.raises(
        NoMatchingDataError, match="No data for day_ahead_prices in 10Y1001A1001A82H"
    ):
        service.fetch_bronze_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )
