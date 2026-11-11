from datetime import UTC

from sqlalchemy import Column, DateTime, Float, Index, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DayAheadPrice(Base):
    __tablename__ = "day_ahead_prices"

    country_code = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    price = Column(Float, nullable=False)
    currency = Column(String, default="EUR")
    ingested_at = Column(DateTime, default=UTC)

    __table_args__ = (Index("idx_dap_country_time", "country_code", "datetime_utc"),)


class ActualLoad(Base):
    __tablename__ = "actual_load"

    country_code = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    load_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class PhysicalFlow(Base):
    __tablename__ = "physical_cross_border_flows"

    in_domain = Column(String, primary_key=True)
    out_domain = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    flow_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class GenerationByType(Base):
    __tablename__ = "actual_generation"

    country_code = Column(String, primary_key=True)
    psr_type = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    generation_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


# Mapping dataset_ids from YAML to SQLAlchemy models
DATASET_MODEL_MAP = {
    "day_ahead_prices": DayAheadPrice,
    "actual_load": ActualLoad,
    "physical_cross_border_flows": PhysicalFlow,
    "actual_generation": GenerationByType,
}
