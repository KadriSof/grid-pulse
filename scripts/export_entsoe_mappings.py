import csv
import os

from entsoe.mappings import PSRTYPE_MAPPINGS


def main():
    seed_dir = "dbt/seeds"
    os.makedirs(seed_dir, exist_ok=True)

    with open(f"{seed_dir}/entsoe_psr_types.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["fuel_type_name", "psr_code"])
        for code, name in PSRTYPE_MAPPINGS.items():
            writer.writerow([name, code])

    print("Exported entsoe_psr_types.csv to dbt/seeds/")


if __name__ == "__main__":
    main()
