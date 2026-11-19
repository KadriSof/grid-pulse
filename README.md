# GridPulse

**GridPulse is a Data Engineering Portfolio Project focused on Apache Airflow and dbt.** It demonstrates an end-to-end data pipeline for European electricity-market data from the ENTSO-E Transparency Platform, including orchestration, ingestion, object storage, PostgreSQL, and SQL-based transformations.

The project is designed to showcase practical data-engineering patterns: scheduled and asset-driven workflows, configurable ingestion, layered data storage, repeatable transformations, and local development with Docker Compose.

## Table of contents

- [Project goals](#project-goals)
- [Data sources and datasets](#data-sources-and-datasets)
- [Technologies](#technologies)
- [Architecture](#architecture)
- [Medallion Architecture](#medallion-architecture)
- [Project structure](#project-structure)
- [Getting started](#getting-started)
- [Running the project](#running-the-project)
- [Running tests and checks](#running-tests-and-checks)
- [Configuration](#configuration)
- [Development notes](#development-notes)
- [Contributing](#contributing)
- [Security](#security)
- [Credits and data attribution](#credits-and-data-attribution)
- [License](#license)

## Project goals

GridPulse is a portfolio project for exploring how to build and operate a data platform using **Airflow for orchestration** and **dbt for analytics transformations**. Its goals include:

- Ingesting electricity-market data from the ENTSO-E API.
- Orchestrating scheduled ingestion and downstream processing with Airflow.
- Preserving raw API responses in S3-compatible object storage.
- Persisting selected structured datasets in PostgreSQL.
- Transforming PostgreSQL source tables into dbt staging models.
- Demonstrating a Medallion Architecture and documenting where each layer fits.

This is an evolving project. The README describes the implementation currently represented by the repository; the Gold layer is configured as a storage and dbt target, but a complete Gold transformation layer should not be assumed to exist yet.

## Data sources and datasets

The primary source is the **ENTSO-E Transparency Platform**, which publishes electricity-market and system data for Europe. Dataset definitions, API methods, geography, priorities, and temporal resolutions are maintained in [`config/datasets.yaml`](config/datasets.yaml).

Configured datasets include:

| Priority                              | Examples                                                                            |
| ------------------------------------- | ----------------------------------------------------------------------------------- |
| P0 — Core market fundamentals         | Day-ahead prices, actual load, load forecast, actual generation, renewable forecast |
| P1 — Market coupling and cross-border | Physical cross-border flows, scheduled commercial exchanges                         |
| P2 — Balancing and outages            | Imbalance prices, generation outages                                                |

The configured geographies are sample areas and border pairs for the portfolio pipeline; they are not a promise of complete European coverage.

## Technologies

| Area                            | Technologies                                                                                                     |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Language and package management | Python 3.13+, `uv` lock file                                                                                     |
| Workflow orchestration          | Apache Airflow 3.2, Airflow DAGs, task mapping, Airflow assets                                                   |
| Transformation                  | dbt Core and `dbt-postgres`                                                                                      |
| Relational storage              | PostgreSQL 16, SQLAlchemy, `psycopg2`                                                                            |
| Object storage                  | S3-compatible storage through the MinIO Python client; local Compose runs SeaweedFS as the S3-compatible service |
| Data handling                   | pandas, PyArrow / Parquet                                                                                        |
| ENTSO-E integration             | `entsoe-py`, HTTP client and retry/configuration libraries                                                       |
| Configuration and validation    | YAML, Pydantic, Pydantic Settings                                                                                |
| Local infrastructure            | Docker, Docker Compose                                                                                           |

## Architecture

At a high level, the project combines Airflow orchestration, S3-compatible object storage, PostgreSQL, and dbt:

1. **Airflow schedules and orchestrates ingestion.** `entsoe_bronze_producer` generates extraction parameters from the dataset configuration and dynamically maps work across datasets and configured geographies.
2. **The producer fetches ENTSO-E data.** Raw XML or ZIP responses are written to the Bronze object-storage bucket. For datasets with a structured database model, structured data is also standardized and upserted into the application PostgreSQL database.
3. **Airflow asset events trigger the Silver consumer.** `entsoe_silver_consumer` fetches structured ENTSO-E data for the event metadata and writes partitioned Parquet files to the Silver bucket.
4. **dbt transforms PostgreSQL source tables.** The dbt project declares the application tables as Bronze sources and defines staging models that build views in a staging schema. The dbt project also configures a marts schema for table models.

**Current implementation detail:** the Silver consumer fetches structured data from ENTSO-E again based on the triggering event; it does not read and transform the raw Bronze objects. Likewise, the producer’s structured PostgreSQL persistence is a separate path from its raw-object upload. Keep this distinction in mind when tracing lineage or extending the pipeline.

## Medallion Architecture

GridPulse uses the Medallion Architecture as a way to organize data by its level of refinement.

### Bronze — raw and source-aligned data

- Raw ENTSO-E XML or ZIP responses are stored in the `gridpulse-bronze` object-storage bucket.
- Objects use source, dataset, date, and geography in their partitioned paths.
- Selected structured datasets are also upserted into PostgreSQL. The dbt project declares these application database tables as the `bronze` source.
- In the current implementation, not every configured dataset has a structured PostgreSQL model; some data remains Bronze-only.

### Silver — standardized data

- The Silver Airflow DAG writes structured data as Parquet to the `gridpulse-silver` bucket.
- Files use date-partitioned paths and include dataset and geography identifiers.
- The consumer adds ingestion metadata and normalizes its datetime column to UTC.
- The consumer currently obtains this data with a structured ENTSO-E API request; it does not derive Silver directly from the raw Bronze object.

### Gold — analytics-ready data

- The storage client has a `gridpulse-gold` bucket configured.
- The dbt project configures a `marts` schema for table-materialized models.
- Treat Gold as the intended home for curated, analytics-ready outputs. The presence of a bucket and dbt configuration alone does not mean a complete Gold pipeline or set of marts is implemented.

## Project structure

```text
.
├── config/
│   ├── airflow_connections.json  # Airflow connection configuration placeholder
│   └── datasets.yaml             # ENTSO-E dataset, geography, and contract settings
├── dags/
│   ├── entsoe_bronze_producer.py # Scheduled raw ingestion and structured persistence
│   └── entsoe_silver_consumer.py # Asset-triggered structured Parquet output
├── dbt/
│   ├── dbt_project.yml           # dbt project and model materialization settings
│   ├── profiles.yml              # Local PostgreSQL development profile
│   ├── models/
│   │   └── staging/              # ENTSO-E source declarations and staging SQL models
│   └── seeds/                    # ENTSO-E reference and code mapping CSVs
├── docker/
│   ├── Dockerfile                # Custom Airflow image
│   └── docker-compose.yml        # Local Airflow, PostgreSQL, and S3-compatible stack
├── scripts/
│   ├── entsoe_smoke_test.py      # Manual ENTSO-E API smoke test
│   ├── export_entsoe_mappings.py # Mapping export utility
│   └── inspect_postgres.py       # PostgreSQL inspection utility
├── src/
│   └── gridpulse/
│       ├── api/                  # ENTSO-E client wrappers
│       ├── ingestion/            # Dataset retrieval, standardization, persistence
│       ├── models/               # Database models and dataset mappings
│       ├── parsers/              # Parsing utilities
│       ├── storage/              # PostgreSQL and S3-compatible storage clients
│       ├── utils/                # Shared utilities
│       └── config.py             # Settings and dataset configuration loading
├── tests/
│   ├── integration/              # Integration test area
│   └── unit/                     # Unit tests organized by component
├── .env.example                  # Local environment variable template
├── pyproject.toml                # Python project metadata and dependencies
└── uv.lock                       # Locked Python dependency versions
```

## Getting started

### Prerequisites

- Git
- Docker Engine and the Docker Compose plugin
- An ENTSO-E Transparency Platform API security token
- Python 3.13+ and `uv` if you want to run Python and dbt commands outside Docker

### 1. Clone the repository

```bash
git clone https://github.com/KadriSof/grid-pulse.git
cd grid-pulse
```

### 2. Configure environment variables

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

Set `GRIDPULSE_ENTSOE_API_KEY` to your ENTSO-E API token. Docker Compose also requires `AIRFLOW_JWT_SECRET`; generate a value and add it to `.env`:

```bash
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

For local development, `.env.example` contains default MinIO-compatible credentials and Airflow UI credentials. Replace defaults before exposing the services beyond your local machine.

### 3. Start the local stack

```bash
docker compose -f docker/docker-compose.yml up --build
```

The Compose stack includes Airflow services, a PostgreSQL metadata database, a separate PostgreSQL application database, and SeaweedFS providing an S3-compatible API for object storage.

When the services are ready:

- Airflow API/UI: <http://localhost:8080>
- S3-compatible API: <http://localhost:9000>
- SeaweedFS UI: <http://localhost:9001>

Use the Airflow username and password configured in `.env`. The Compose defaults are for local development only.

Stop the stack with:

```bash
docker compose -f docker/docker-compose.yml down
```

To also remove persisted database and object-storage volumes, use `docker compose -f docker/docker-compose.yml down -v`. This deletes local stack data.

## Running the project

### Airflow

The Compose stack initializes the Airflow metadata database and creates the configured Airflow user. The DAGs are mounted into the Airflow containers.

- `entsoe_bronze_producer` is scheduled daily at 14:00 UTC, with catchup disabled.
- `entsoe_silver_consumer` is triggered by the producer’s ENTSO-E Bronze assets.

Enable and trigger DAGs from the Airflow interface after the stack is running. Successful API ingestion requires a valid ENTSO-E API token and available data for the requested period and geography.

### dbt

The dbt project is in `dbt/`. Its development profile targets the application PostgreSQL database on `localhost:5433` and reads `GRIDPULSE_PG_USER` and `GRIDPULSE_PG_PASSWORD`, with local defaults defined in `dbt/profiles.yml`.

With Python dependencies installed, validate the dbt connection and run the project from the repository root:

```bash
uv run dbt debug --project-dir dbt --profiles-dir dbt
uv run dbt run --project-dir dbt --profiles-dir dbt
```

The database must be running and contain the source tables referenced by the dbt source declarations. Seed reference data when needed:

```bash
uv run dbt seed --project-dir dbt --profiles-dir dbt
```

The configured staging models are views; dbt models under `marts` are configured as tables.

### Manual ENTSO-E smoke test

The smoke-test script exercises raw and structured retrieval for a sample day-ahead prices query. Run it from the repository root after installing the project and configuring `.env`:

```bash
uv run python scripts/entsoe_smoke_test.py
```

## Running tests and checks

Install the project and its development dependencies:

```bash
uv sync --group dev
```

Run the test suite:

```bash
uv run pytest
```

Run the configured Ruff checks:

```bash
uv run ruff check .
```

Tests that make real API or database requests may require credentials and running services. Check individual tests before running them against external systems.

## Configuration

### Environment variables

| Variable                    | Purpose                                                     |
| --------------------------- | ----------------------------------------------------------- |
| `GRIDPULSE_ENTSOE_API_KEY`  | ENTSO-E API security token                                  |
| `AIRFLOW_JWT_SECRET`        | Required by Docker Compose for Airflow 3 API authentication |
| `AIRFLOW_WWW_USER_USERNAME` | Local Airflow UI username                                   |
| `AIRFLOW_WWW_USER_PASSWORD` | Local Airflow UI password                                   |
| `MINIO_ROOT_USER`           | S3-compatible storage access key in the Compose environment |
| `MINIO_ROOT_PASSWORD`       | S3-compatible storage secret key in the Compose environment |
| `GRIDPULSE_PG_USER`         | Application PostgreSQL username                             |
| `GRIDPULSE_PG_PASSWORD`     | Application PostgreSQL password                             |
| `GRIDPULSE_PG_DATABASE`     | Application PostgreSQL database name                        |

Dataset behavior is configured in `config/datasets.yaml`, including ENTSO-E API methods, priority, geography, temporal resolution, and Silver-layer contracts. Application settings use the `GRIDPULSE_` prefix.

## Development notes

- The repository uses a `src/` layout for the Python package.
- `uv.lock` records the resolved Python dependencies.
- Airflow orchestration is implemented in `dags/`; reusable pipeline logic lives in `src/gridpulse/`.
- dbt source declarations, staging models, and reference seeds are kept under `dbt/`.
- The local Compose stack uses SeaweedFS as the S3-compatible service, while application code uses the MinIO client library.
- Do not commit `.env`, API tokens, passwords, generated logs, or local data.

## Contributing

Contributions, issue reports, and suggestions are welcome. For changes to the pipeline, please include a short description of the behavior change and tests or validation steps where appropriate. Keep secrets and real credentials out of commits.

## Security

Never commit API tokens, passwords, or other credentials. Use a private local `.env` file for development and replace the example credentials before deploying or exposing services. If you discover a security issue, avoid posting credentials or exploit details publicly; contact the repository owner privately.

## Credits and data attribution

GridPulse uses data provided by the **ENTSO-E Transparency Platform**.

- Website: <https://transparency.entsoe.eu/>
- ENTSO-E Web API guide: <https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html>

ENTSO-E is the source of the electricity-market data used in this project. Please consult ENTSO-E’s terms and data-use requirements when accessing, redistributing, or publishing data.

## License

No license file was present in the repository when this README was prepared. Unless a license is added, the project’s reuse and redistribution permissions are not explicitly specified.
