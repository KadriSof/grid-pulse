from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests
from entsoe.exceptions import (
    InvalidParameterError,
    NoMatchingDataError,
)

from gridpulse.api.entsoe_client import EntsoeClient
from gridpulse.ingestion.entsoe_service import (
    EntsoeService,
    EntsoeServiceError,
)


@pytest.fixture
def mock_entsoe_client():
    client = MagicMock(spec=EntsoeClient)
    client.raw_client = MagicMock()
    client.pandas_client = MagicMock()
    return client


@pytest.fixture
def service(mock_entsoe_client):
    return EntsoeService(client=mock_entsoe_client)


# Configuration
def test_get_dataset_success(service):
    dataset = service._get_dataset("day_ahead_prices")

    assert dataset.id == "entsoe_day_ahead_prices"


def test_get_dataset_unknown(service):
    with pytest.raises(ValueError, match="Unknown ENTSO-E dataset"):
        service._get_dataset("unknown_dataset")


# Time normalization
def test_normalize_time_range_success(service):
    start, end = service._normalize_time_range(
        "2023-10-01",
        "2023-10-02",
    )

    assert start == pd.Timestamp("2023-10-01", tz="UTC")
    assert end == pd.Timestamp("2023-10-02", tz="UTC")
    assert start.tz is not None
    assert end.tz is not None
    assert start < end


def test_normalize_time_range_converts_timezone_to_utc(service):
    start = pd.Timestamp("2023-10-01 02:00:00", tz="Europe/Paris")
    end = pd.Timestamp("2023-10-01 03:00:00", tz="Europe/Paris")

    normalized_start, normalized_end = service._normalize_time_range(
        start,
        end,
    )

    assert normalized_start == pd.Timestamp("2023-10-01 00:00:00", tz="UTC")
    assert normalized_end == pd.Timestamp("2023-10-01 01:00:00", tz="UTC")


def test_normalize_time_range_invalid(service):
    with pytest.raises(ValueError, match="start must be earlier than end"):
        service._normalize_time_range(
            "2023-10-02",
            "2023-10-01",
        )


def test_normalize_time_range_equal_timestamps(service):
    with pytest.raises(ValueError, match="start must be earlier than end"):
        service._normalize_time_range(
            "2023-10-01",
            "2023-10-01",
        )


# Geography validation
def test_validate_geography_success_single(service):
    dataset = service._get_dataset("day_ahead_prices")

    service._validate_geography(
        dataset_cfg=dataset,
        country_code="10Y1001A1001A82H",
    )


def test_validate_geography_success_pair(service):
    dataset = service._get_dataset("physical_cross_border_flows")

    service._validate_geography(
        dataset_cfg=dataset,
        country_code="10Y1001A1001A82H",
        out_domain="10YFR-RTE------C",
    )


def test_validate_geography_missing_country(service):
    dataset = service._get_dataset("day_ahead_prices")

    with pytest.raises(ValueError, match="country_code is required"):
        service._validate_geography(
            dataset_cfg=dataset,
            country_code=None,
        )


def test_validate_geography_missing_pair(service):
    dataset = service._get_dataset("physical_cross_border_flows")

    with pytest.raises(
        ValueError,
        match="Both country_code and out_domain are required",
    ):
        service._validate_geography(
            dataset_cfg=dataset,
            country_code="10Y1001A1001A82H",
        )


# SDK kwargs construction
def test_build_kwargs_single_geography(service):
    dataset = service._get_dataset("day_ahead_prices")

    start = pd.Timestamp("2023-10-01", tz="UTC")
    end = pd.Timestamp("2023-10-02", tz="UTC")

    kwargs = service._build_kwargs(
        dataset_cfg=dataset,
        start=start,
        end=end,
        country_code="10Y1001A1001A82H",
    )

    assert kwargs == {
        "start": start,
        "end": end,
        "country_code": "10Y1001A1001A82H",
    }


def test_build_kwargs_border_pair(service):
    dataset = service._get_dataset("physical_cross_border_flows")

    start = pd.Timestamp("2023-10-01", tz="UTC")
    end = pd.Timestamp("2023-10-02", tz="UTC")

    kwargs = service._build_kwargs(
        dataset_cfg=dataset,
        start=start,
        end=end,
        country_code="10Y1001A1001A82H",
        out_domain="10YFR-RTE------C",
    )

    assert kwargs == {
        "start": start,
        "end": end,
        "country_code_from": "10Y1001A1001A82H",
        "country_code_to": "10YFR-RTE------C",
    }


def test_build_kwargs_with_psr_type(service):
    dataset = service._get_dataset("day_ahead_prices")

    start = pd.Timestamp("2023-10-01", tz="UTC")
    end = pd.Timestamp("2023-10-02", tz="UTC")

    kwargs = service._build_kwargs(
        dataset_cfg=dataset,
        start=start,
        end=end,
        country_code="10Y1001A1001A82H",
        psr_type="B16",
    )

    assert kwargs["psr_type"] == "B16"


# Bronze: public SDK method
def test_fetch_raw_data_normal(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")

    monkeypatch.setattr(dataset.api, "use_base_request", False)

    mock_api_method = MagicMock(return_value="<xml>data</xml>")
    mock_entsoe_client.raw_client.query_day_ahead_prices = mock_api_method

    result = service.fetch_raw_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )

    assert result == "<xml>data</xml>"

    mock_api_method.assert_called_once_with(
        start=pd.Timestamp("2023-10-01", tz="UTC"),
        end=pd.Timestamp("2023-10-02", tz="UTC"),
        country_code="10Y1001A1001A82H",
    )


def test_fetch_raw_data_missing_sdk_method(
    service,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")
    monkeypatch.setattr(dataset.api, "use_base_request", False)
    monkeypatch.setattr(
        dataset.api,
        "entsoe_py_method",
        "non_existing_sdk_method",
    )

    service.client.raw_client = SimpleNamespace(query_day_ahead_prices=MagicMock())

    with pytest.raises(
        EntsoeServiceError,
        match="raw SDK method",
    ):
        service.fetch_raw_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )


def test_fetch_raw_data_no_matching_data(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")
    monkeypatch.setattr(dataset.api, "use_base_request", False)

    original_error = NoMatchingDataError("No matching data found")

    mock_entsoe_client.raw_client.query_day_ahead_prices.side_effect = original_error

    with pytest.raises(NoMatchingDataError) as exc_info:
        service.fetch_raw_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )

    assert exc_info.value is original_error


def test_fetch_raw_data_sdk_error_is_wrapped(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")
    monkeypatch.setattr(dataset.api, "use_base_request", False)

    mock_entsoe_client.raw_client.query_day_ahead_prices.side_effect = (
        InvalidParameterError("Invalid parameter")
    )

    with pytest.raises(
        EntsoeServiceError,
        match="ENTSO-E request failed",
    ):
        service.fetch_raw_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )


# Bronze: raw base-request fallback
def test_fetch_raw_data_base_request_fallback(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")

    monkeypatch.setattr(dataset.api, "use_base_request", True)
    monkeypatch.setattr(
        dataset.api,
        "base_request_params",
        {"documentType": "A44", "processType": "A01"},
    )

    mock_response = MagicMock()
    mock_response.text = "<xml>data</xml>"
    mock_response.headers = {"Content-Type": "application/xml"}

    mock_entsoe_client.raw_request.return_value = mock_response

    result = service.fetch_raw_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )

    assert result == "<xml>data</xml>"

    mock_entsoe_client.raw_request.assert_called_once_with(
        params={
            "documentType": "A44",
            "processType": "A01",
            "in_Domain": "10Y1001A1001A82H",
        },
        start=pd.Timestamp("2023-10-01", tz="UTC"),
        end=pd.Timestamp("2023-10-02", tz="UTC"),
    )

    mock_response.raise_for_status.assert_called_once()


def test_fetch_raw_data_base_request_zip_response(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")

    monkeypatch.setattr(dataset.api, "use_base_request", True)
    monkeypatch.setattr(
        dataset.api,
        "base_request_params",
        {"documentType": "A44", "processType": "A01"},
    )

    zip_content = b"PK\x03\x04fake-zip-content"

    mock_response = MagicMock()
    mock_response.content = zip_content
    mock_response.headers = {"Content-Type": "application/zip"}

    mock_entsoe_client.raw_request.return_value = mock_response

    result = service.fetch_raw_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )

    assert result == zip_content
    assert isinstance(result, bytes)


def test_fetch_raw_data_base_request_no_matching_data(
    service,
    mock_entsoe_client,
    monkeypatch,
):
    dataset = service._get_dataset("day_ahead_prices")

    monkeypatch.setattr(dataset.api, "use_base_request", True)
    monkeypatch.setattr(
        dataset.api,
        "base_request_params",
        {"documentType": "A44", "processType": "A01"},
    )

    error_response = MagicMock()
    error_response.text = "No matching data found"

    http_error = requests.HTTPError(
        "404 Client Error",
        response=error_response,
    )

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = http_error

    mock_entsoe_client.raw_request.return_value = mock_response

    with pytest.raises(NoMatchingDataError, match="No data"):
        service.fetch_raw_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )


# Silver: Pandas SDK method
def test_fetch_structured_data(
    service,
    mock_entsoe_client,
):
    expected_df = pd.DataFrame(
        {"price": [100.0, 110.0]},
        index=pd.date_range(
            "2023-10-01",
            periods=2,
            freq="h",
            tz="UTC",
        ),
    )

    mock_api_method = MagicMock(return_value=expected_df)
    mock_entsoe_client.pandas_client.query_day_ahead_prices = mock_api_method

    result = service.fetch_structured_data(
        dataset_id="day_ahead_prices",
        country_code="10Y1001A1001A82H",
        start="2023-10-01",
        end="2023-10-02",
    )

    pd.testing.assert_frame_equal(result, expected_df)

    mock_api_method.assert_called_once_with(
        start=pd.Timestamp("2023-10-01", tz="UTC"),
        end=pd.Timestamp("2023-10-02", tz="UTC"),
        country_code="10Y1001A1001A82H",
    )


def test_fetch_structured_data_no_matching_data(
    service,
    mock_entsoe_client,
):
    original_error = NoMatchingDataError("No matching data found")

    mock_entsoe_client.pandas_client.query_day_ahead_prices.side_effect = original_error

    with pytest.raises(NoMatchingDataError) as exc_info:
        service.fetch_structured_data(
            dataset_id="day_ahead_prices",
            country_code="10Y1001A1001A82H",
            start="2023-10-01",
            end="2023-10-02",
        )

    assert exc_info.value is original_error
