#!/usr/bin/env python3
"""
Standalone utility to inspect and examine the GridPulse application database.

Usage:
    uv run python scripts/inspect_postgres.py overview
    uv run python scripts/inspect_postgres.py schema day_ahead_prices
    uv run python scripts/inspect_postgres.py sample day_ahead_prices --limit 10
    uv run python scripts/inspect_postgres.py query "SELECT country_code, COUNT(*) FROM actual_load GROUP BY 1"
"""

import argparse
import logging
import sys

import pandas as pd
from sqlalchemy import inspect, text

from gridpulse.config import settings
from gridpulse.storage.postgres_client import PostgresClient

# Clean terminal output
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)

logging.basicConfig(level=logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logger = logging.getLogger("inspect_postgres")

SEPARATOR = "=" * 78


def print_header(title: str) -> None:
    print(f"\n{SEPARATOR}\n  {title}\n{SEPARATOR}")


def table_exists(inspector, table: str) -> bool:
    return table in inspector.get_table_names()


def print_connection_info() -> None:
    print_header("Connection")
    print(f"  Host     : {settings.pg_host}")
    print(f"  Port     : {settings.pg_port}")
    print(f"  Database : {settings.pg_database}")
    print(f"  User     : {settings.pg_user}")


# Commands
def cmd_overview(client: PostgresClient) -> None:
    """List every table with its row count and datetime range."""
    inspector = inspect(client.engine)
    tables = sorted(inspector.get_table_names())

    if not tables:
        print("\n  No tables found in the database.\n")
        return

    print_header(f"Overview — {len(tables)} table(s)")

    records = []
    with client.engine.connect() as conn:
        for table in tables:
            row_count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()

            columns = [c["name"] for c in inspector.get_columns(table)]
            date_min = date_max = "—"
            if "datetime_utc" in columns:
                row = conn.execute(
                    text(f'SELECT MIN(datetime_utc), MAX(datetime_utc) FROM "{table}"')
                ).fetchone()
                if row and row[0] is not None:
                    date_min = str(row[0])
                    date_max = str(row[1])

            records.append(
                {
                    "table": table,
                    "rows": row_count,
                    "min_datetime": date_min,
                    "max_datetime": date_max,
                }
            )

    df = pd.DataFrame(records)
    print(df.to_string(index=False))
    print()


def cmd_schema(client: PostgresClient, table: str) -> None:
    """Show the schema (columns, types, PKs) of a specific table."""
    inspector = inspect(client.engine)

    if not table_exists(inspector, table):
        print(f"\n  Table '{table}' not found.\n")
        return

    print_header(f"Schema — {table}")

    pk_constraint = inspector.get_pk_constraint(table)
    pk_columns = set(pk_constraint.get("constrained_columns", []))

    records = []
    for col in inspector.get_columns(table):
        records.append(
            {
                "column": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "primary_key": col["name"] in pk_columns,
            }
        )

    df = pd.DataFrame(records)
    print(df.to_string(index=False))
    print()


def cmd_sample(client: PostgresClient, table: str, limit: int) -> None:
    """Show a sample of rows from a specific table."""
    inspector = inspect(client.engine)

    if not table_exists(inspector, table):
        print(f"\n  Table '{table}' not found.\n")
        return

    print_header(f"Sample — {table} (limit {limit})")

    df = pd.read_sql(
        text(f'SELECT * FROM "{table}" LIMIT :limit'),
        client.engine,
        params={"limit": limit},
    )

    if df.empty:
        print("  (table is empty)")
    else:
        print(df.to_string(index=False))
    print()


def cmd_query(client: PostgresClient, sql: str) -> None:
    """Run an arbitrary read-only SQL query."""
    print_header("Custom Query")
    print(f"  SQL: {sql}\n")

    try:
        df = pd.read_sql(text(sql), client.engine)
        if df.empty:
            print("  (no rows returned)")
        else:
            print(df.to_string(index=False))
        print(f"\n  ({len(df)} row(s))\n")
    except Exception as e:
        print(f"  Query failed: {e}\n")
        sys.exit(1)


# CLI
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the GridPulse application database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "overview", help="List all tables with counts and date ranges"
    )

    schema_parser = subparsers.add_parser("schema", help="Show a table's schema")
    schema_parser.add_argument("table", help="Table name")

    sample_parser = subparsers.add_parser(
        "sample", help="Show sample rows from a table"
    )
    sample_parser.add_argument("table", help="Table name")
    sample_parser.add_argument(
        "--limit", type=int, default=10, help="Number of rows (default 10)"
    )

    query_parser = subparsers.add_parser("query", help="Run a custom SQL query")
    query_parser.add_argument("sql", help="SQL statement to execute")

    args = parser.parse_args()

    # Default to overview when no command is given
    command = args.command or "overview"

    try:
        client = PostgresClient()
    except Exception as e:
        print(f"\nFailed to connect to the database: {e}\n")
        sys.exit(1)

    print_connection_info()

    if command == "overview":
        cmd_overview(client)
    elif command == "schema":
        cmd_schema(client, args.table)
    elif command == "sample":
        cmd_sample(client, args.table, args.limit)
    elif command == "query":
        cmd_query(client, args.sql)


if __name__ == "__main__":
    main()
