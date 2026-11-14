import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import sessionmaker

from gridpulse.config import settings

logger = logging.getLogger(__name__)


class PostgresClient:
    """Production-grade client for PostgreSQL storage."""

    def __init__(self):
        connection_url = URL.create(
            drivername="postgresql+psycopg2",
            username=settings.pg_user,
            password=settings.pg_password,
            host=str(settings.pg_host),
            port=int(settings.pg_port),
            database=settings.pg_database,
        )

        self.engine: Engine = create_engine(
            connection_url, pool_size=5, max_overflow=10, pool_pre_ping=True
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        logger.info(f"[PostgresClient] Initialized engine for {settings.pg_database}")

    def get_session(self):
        """Yields a database session for context-managed transactions."""
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()
