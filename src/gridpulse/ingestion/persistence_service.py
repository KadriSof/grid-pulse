import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from gridpulse.models.db_models import DATASET_MODEL_MAP, Base
from gridpulse.storage.postgres_client import PostgresClient

logger = logging.getLogger(__name__)


class PersistenceService:
    """Handles upserting Pandas DataFrames into PostgreSQL."""

    def __init__(self, client: PostgresClient | None = None):
        self.client = client or PostgresClient()
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Creates all defined tables if they don't exist."""
        Base.metadata.create_all(bind=self.client.engine)
        logger.info("[PersistenceService] Ensured all database tables exist.")

    def upsert_dataframe(self, df: pd.DataFrame, dataset_id: str) -> int:
        """
        Upserts a DataFrame into the corresponding Postgres table.
        Returns the number of rows processed.
        """
        if df.empty:
            logger.warning(
                f"[PersistenceService] Empty DataFrame for {dataset_id}, skipping."
            )
            return 0

        model = DATASET_MODEL_MAP.get(dataset_id)
        if not model:
            raise ValueError(f"No database model mapped for dataset_id: {dataset_id}")

        table_name = model.__tablename__

        # Extract primary key columns from the SQLAlchemy model
        primary_keys = [col.name for col in model.__table__.primary_key.columns]

        # Custom Pandas to_sql method for Postgres UPSERT
        def upsert_method(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = insert(table.table).values(data)

            # Build the ON CONFLICT DO UPDATE clause
            update_dict = {
                col.name: stmt.excluded[col.name]
                for col in table.table.columns
                if col.name not in primary_keys and col.name != "ingested_at"
            }
            update_dict["ingested_at"] = pd.Timestamp.now(tz="UTC")

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=primary_keys, set_=update_dict
            )
            conn.execute(upsert_stmt)

        try:
            # Ensure datetime columns are timezone-aware UTC before insertion
            for col in df.columns:
                if "datetime" in col or "time" in col:
                    df[col] = pd.to_datetime(df[col], utc=True)

            rows_inserted = df.to_sql(
                name=table_name,
                con=self.client.engine,
                if_exists="append",
                index=False,
                method=upsert_method,
                chunksize=1000,
            )

            logger.info(
                f"[PersistenceService] Upserted {rows_inserted} rows into {table_name}"
            )
            return rows_inserted or 0

        except Exception as e:
            logger.error(
                f"[PersistenceService] Failed to upsert into {table_name}: {e}"
            )
            raise
