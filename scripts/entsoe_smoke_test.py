import os

import pandas as pd
from entsoe.exceptions import NoMatchingDataError

from gridpulse.ingestion.entsoe_service import EntsoeService, EntsoeServiceError


def main():
    print("🔌 Initializing GridPulse ENTSO-E Service...")
    service = EntsoeService()

    country_code = "10Y1001A1001A82H"
    start = pd.Timestamp("2023-10-01 00:00:00", tz="UTC")
    end = pd.Timestamp("2023-10-02 00:00:00", tz="UTC")
    dataset_id = "day_ahead_prices"

    print("Fetching Day-Ahead Prices")
    print(f"   Dataset ID: {dataset_id}")
    print(f"   Domain: {country_code}")
    print(f"   Period: {start} to {end}")

    try:
        # 1. Test Bronze (Raw Data: XML or ZIP)
        print("\n--- Testing Bronze (Raw Data) ---")
        raw_data = service.fetch_bronze_data(
            dataset_id=dataset_id, country_code=country_code, start=start, end=end
        )

        if isinstance(raw_data, str):
            assert "Publication_MarketDocument" in raw_data
            print(f"SUCCESS: Raw XML fetched ({len(raw_data)} bytes)")
            os.makedirs("data/bronze/entsoe", exist_ok=True)
            output_path = f"data/bronze/entsoe/sample_A44_{country_code}.xml"
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(raw_data)
            print(f"Saved raw XML to {output_path}")
        else:
            print(f"SUCCESS: Raw ZIP data fetched ({len(raw_data)} bytes)")

        # 2. Test Silver (Pandas DataFrame)
        print("\n--- Testing Silver (Pandas DataFrame) ---")
        df = service.fetch_silver_dataframe(
            dataset_id=dataset_id, country_code=country_code, start=start, end=end
        )
        print("SUCCESS: DataFrame fetched")
        print(df.head())
        print(f"   Shape: {df.shape}")

    except NoMatchingDataError as e:
        print(f"NO DATA (Valid response, but empty): {e}")
    except EntsoeServiceError as e:
        print(f"SERVICE ERROR: {e}")
        raise
    except Exception as e:
        print(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
