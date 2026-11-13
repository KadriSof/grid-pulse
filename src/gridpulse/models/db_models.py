from datetime import UTC

from sqlalchemy import Column, DateTime, Float, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DayAheadPrice(Base):
    __tablename__ = "day_ahead_prices"
    country_code = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    price = Column(Float, nullable=False)
    currency = Column(String, default="EUR")
    ingested_at = Column(DateTime, default=UTC)


class ActualLoad(Base):
    __tablename__ = "actual_load"
    country_code = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    load_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class LoadForecast(Base):
    __tablename__ = "load_forecast"
    country_code = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    load_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class ActualGeneration(Base):
    __tablename__ = "actual_generation"
    country_code = Column(String, primary_key=True)
    psr_type = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    generation_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class RenewableForecast(Base):
    __tablename__ = "renewable_forecast"
    country_code = Column(String, primary_key=True)
    psr_type = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    generation_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class PhysicalFlow(Base):
    __tablename__ = "physical_cross_border_flows"
    in_domain = Column(String, primary_key=True)
    out_domain = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    flow_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


class ScheduledExchange(Base):
    __tablename__ = "scheduled_commercial_exchanges"

    in_domain = Column(String, primary_key=True)
    out_domain = Column(String, primary_key=True)
    datetime_utc = Column(DateTime(timezone=True), primary_key=True)
    flow_mw = Column(Float, nullable=False)
    ingested_at = Column(DateTime, default=UTC)


# Mapping dataset_ids from YAML to SQLAlchemy models
DATASET_MODEL_MAP = {
    "day_ahead_prices": DayAheadPrice,
    "actual_load": ActualLoad,
    "load_forecast": LoadForecast,
    "actual_generation": ActualGeneration,
    "renewable_forecast": RenewableForecast,
    "physical_cross_border_flows": PhysicalFlow,
    "scheduled_commercial_exchanges": ScheduledExchange,
}
