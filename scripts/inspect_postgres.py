#!/usr/bin/env python3
"""
Standalone utility to inspect and examine the GridPulse application database.
Now discovers tables AND views across all schemas (public, public_staging, public_marts).

Usage:
    uv run python scripts/inspect_postgres.py overview
    uv run python scripts/inspect_postgres.py schema public_staging.stg_entsoe_actual_generation
    uv run python scripts/inspect_postgres.py sample public_staging.stg_entsoe_actual_generation --limit 10
    uv run python scripts/inspect_postgres.py query "SELECT ..."
"""

import argparse
import logging
import sys

import pandas as pd
from sqlalchemy import inspect, text

from gridpulse.config import settings
from gridpulse.storage.postgres_client import PostgresClient

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

logging.basicConfig(level=logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

SEPARATOR = "=" * 78
SYSTEM_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast"}


def print_header(title: str) -> None:
    print(f"\n{SEPARATOR}\n  {title}\n{SEPARATOR}")


def get_managed_schemas(inspector) -> list[str]:
    """Return all non-system schemas (auto-discovers public, public_staging, public_marts)."""
    return [s for s in inspector.get_schema_names() if s not in SYSTEM_SCHEMAS]


def get_all_objects(inspector) -> list[tuple[str, str, str]]:
    """Return (schema, object_name, type) for all tables and views."""
    objects = []
    for schema in get_managed_schemas(inspector):
        for table in inspector.get_table_names(schema=schema):
            objects.append((schema, table, "table"))
        for view in inspector.get_view_names(schema=schema):
            objects.append((schema, view, "view"))
    return objects


def resolve_object(inspector, name: str) -> tuple[str, str] | None:
    """Resolve 'schema.object' or bare 'object' (searched across schemas)."""
    if "." in name:
        schema, obj = name.split(".", 1)
        return (schema, obj)
    # Search all schemas for a bare name
    for schema, obj, _ in get_all_objects(inspector):
        if obj == name:
            return (schema, obj)
    return None


def print_connection_info() -> None:
    print_header("Connection")
    print(f"  Host     : {settings.pg_host}")
    print(f"  Port     : {settings.pg_port}")
    print(f"  Database : {settings.pg_database}")
    print(f"  User     : {settings.pg_user}")


def cmd_overview(client: PostgresClient) -> None:
    inspector = inspect(client.engine)
    objects = get_all_objects(inspector)

    if not objects:
        print("\n  No tables or views found.\n")
        return

    print_header(f"Overview — {len(objects)} object(s)")

    records = []
    with client.engine.connect() as conn:
        for schema, obj, obj_type in sorted(objects):
            qualified = f"{schema}.{obj}"
            row_count = conn.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{obj}"')
            ).scalar()

            columns = [c["name"] for c in inspector.get_columns(obj, schema=schema)]
            date_min = date_max = "—"
            if "datetime_utc" in columns:
                row = conn.execute(
                    text(
                        f'SELECT MIN(datetime_utc), MAX(datetime_utc) FROM "{schema}"."{obj}"'
                    )
                ).fetchone()
                if row and row[0] is not None:
                    date_min = str(row[0])
                    date_max = str(row[1])

            records.append(
                {
                    "schema": schema,
                    "object": obj,
                    "type": obj_type,
                    "rows": row_count,
                    "min_datetime": date_min,
                    "max_datetime": date_max,
                }
            )

    df = pd.DataFrame(records)
    print(df.to_string(index=False))
    print()


def cmd_schema(client: PostgresClient, name: str) -> None:
    inspector = inspect(client.engine)
    resolved = resolve_object(inspector, name)
    if not resolved:
        print(f"\n  Object '{name}' not found.\n")
        return

    schema, obj = resolved
    print_header(f"Schema — {schema}.{obj}")

    pk_columns = set(
        inspector.get_pk_constraint(obj, schema=schema).get("constrained_columns", [])
    )
    records = [
        {
            "column": col["name"],
            "type": str(col["type"]),
            "nullable": col.get("nullable", True),
            "primary_key": col["name"] in pk_columns,
        }
        for col in inspector.get_columns(obj, schema=schema)
    ]
    print(pd.DataFrame(records).to_string(index=False))
    print()


def cmd_sample(client: PostgresClient, name: str, limit: int) -> None:
    inspector = inspect(client.engine)
    resolved = resolve_object(inspector, name)
    if not resolved:
        print(f"\n  Object '{name}' not found.\n")
        return

    schema, obj = resolved
    print_header(f"Sample — {schema}.{obj} (limit {limit})")

    df = pd.read_sql(
        text(f'SELECT * FROM "{schema}"."{obj}" LIMIT :limit'),
        client.engine,
        params={"limit": limit},
    )
    print("  (empty)" if df.empty else df.to_string(index=False))
    print()


def cmd_query(client: PostgresClient, sql: str) -> None:
    print_header("Custom Query")
    print(f"  SQL: {sql}\n")
    try:
        df = pd.read_sql(text(sql), client.engine)
        print("  (no rows returned)" if df.empty else df.to_string(index=False))
        print(f"\n  ({len(df)} row(s))\n")
    except Exception as e:
        print(f"  Query failed: {e}\n")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the GridPulse application database."
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("overview", help="List all tables and views")
    schema_p = subparsers.add_parser("schema", help="Show an object's schema")
    schema_p.add_argument("name")
    sample_p = subparsers.add_parser("sample", help="Show sample rows")
    sample_p.add_argument("name")
    sample_p.add_argument("--limit", type=int, default=10)
    query_p = subparsers.add_parser("query", help="Run a custom SQL query")
    query_p.add_argument("sql")

    args = parser.parse_args()
    command = args.command or "overview"

    try:
        client = PostgresClient()
    except Exception as e:
        print(f"\nFailed to connect: {e}\n")
        sys.exit(1)

    print_connection_info()

    if command == "overview":
        cmd_overview(client)
    elif command == "schema":
        cmd_schema(client, args.name)
    elif command == "sample":
        cmd_sample(client, args.name, args.limit)
    elif command == "query":
        cmd_query(client, args.sql)


if __name__ == "__main__":
    main()
