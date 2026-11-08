from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


# YAML Schema Models
class RateLimitConfig(BaseModel):
    requests_per_minute: int
    retry_attempts: int
    backoff_factor: float


class ApiConfig(BaseModel):
    entsoe_py_method: str
    document_type: str
    process_type: str
    requires_out_domain: bool = False
    psr_type_options: list[str] | None = None
    use_base_request: bool = False
    base_request_params: dict[str, str] | None = None


class GeographyConfig(BaseModel):
    level: str
    sample_eic_codes: list[str] | None = None
    sample_pairs: list[list[str]] | None = None  # For cross-border datasets


class TemporalConfig(BaseModel):
    default_resolution: str


class ValidationRule(BaseModel):
    field: str
    condition: str


class SilverContractConfig(BaseModel):
    primary_keys: list[str]
    fill_missing_positions: bool = False
    fill_value: float = 0.0
    validation_rules: list[ValidationRule] | None = None


class DatasetConfig(BaseModel):
    id: str
    description: str
    priority: str
    api: ApiConfig
    geography: GeographyConfig
    temporal: TemporalConfig
    silver_contract: SilverContractConfig | None = None


class EntsoeConfig(BaseModel):
    base_url: str
    rate_limit: RateLimitConfig
    datasets: dict[str, DatasetConfig]


# Application Settings
class GridPulseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GRIDPULSE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    entsoe_api_key: str | None = None
    config_path: str = "config/datasets.yaml"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"

    def load_entsoe_config(self) -> EntsoeConfig:
        path = Path(self.config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found at {path.resolve()}")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if "entsoe" not in data:
            raise ValueError("YAML file must contain a top-level 'entsoe' key")

        return EntsoeConfig(**data["entsoe"])


# Global Instances
settings = GridPulseSettings()
entsoe_config = settings.load_entsoe_config()
