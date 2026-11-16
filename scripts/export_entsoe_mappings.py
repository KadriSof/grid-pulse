import csv
import os

from gridpulse.utils import EntsoeMappings


def export_to_csv(filepath: str, data: list[dict]):
    if not data:
        return

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Exported {filepath} ({len(data)} rows)")


def main():
    seed_dir = "dbt/seeds"
    os.makedirs(seed_dir, exist_ok=True)

    # 1. Bidding Zones (from Area enum)
    export_to_csv(
        f"{seed_dir}/entsoe_bidding_zones.csv", EntsoeMappings.get_bidding_zones()
    )

    # 2. PSR Types (Fuel/Generation types)
    psr_data = [
        {"psr_code": k, "fuel_type_name": v}
        for k, v in EntsoeMappings.psr_code_to_name.items()
    ]
    export_to_csv(f"{seed_dir}/entsoe_psr_types.csv", psr_data)

    # 3. Process Types (e.g., Day ahead, Realised)
    process_data = [
        {"process_code": k, "process_name": v}
        for k, v in EntsoeMappings.process_code_to_name.items()
    ]
    export_to_csv(f"{seed_dir}/entsoe_process_types.csv", process_data)

    # 4. Business Types (e.g., Production, Consumption)
    bsn_data = [
        {"bsn_code": k, "bsn_name": v}
        for k, v in EntsoeMappings.bsn_code_to_name.items()
    ]
    export_to_csv(f"{seed_dir}/entsoe_business_types.csv", bsn_data)

    # 5. Document Types
    doc_data = [
        {"doc_code": k, "doc_name": v}
        for k, v in EntsoeMappings.doc_code_to_name.items()
    ]
    export_to_csv(f"{seed_dir}/entsoe_document_types.csv", doc_data)

    # 6. Neighbours (Exploded into pairs for easy SQL joins)
    neighbour_data = [
        {"zone_short_code": zone, "neighbour_short_code": n}
        for zone, neighbours in EntsoeMappings.neighbours.items()
        for n in neighbours
    ]
    export_to_csv(f"{seed_dir}/entsoe_neighbours.csv", neighbour_data)

    print("\nAll ENTSO-E mappings successfully exported to dbt/seeds/")


if __name__ == "__main__":
    main()
