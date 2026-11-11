import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from gridpulse.config import settings

logger = logging.getLogger(__name__)


class PostgresClient:
    """Client for PostgreSQL storage"""

    def __init__(self) -> None:
        self.connection_string = (
            f"postgresql+psycopg2://{settings.pg_user}:{settings.pg_password}"
            f"@{settings.pg_host}:{settings.pg_host}/{settings.pg_database}"
        )

        self.engine: Engine = create_engine(
            self.connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # replace stale connections before use
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
