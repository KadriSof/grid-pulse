# GridPulse Custom Airflow Image
# (Based on official Airflow 3.2.0 with Python 3.13)
FROM apache/airflow:slim-3.2.0-python3.13

ARG ROOT_UID=0
ARG AIRFLOW_UID=50000
ARG AIRFLOW_VERSION=3.2.0
ARG PYTHON_VERSION=3.13

# Switch to root to install system dependencies
USER ${ROOT_UID}

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  libpq-dev \
  curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Python dependencies
USER ${AIRFLOW_UID}

# Set working directory
WORKDIR /opt/airflow

# Copy project metadata and application source
COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src/ ./src/

# Install Airflow with constraints into the official Airflow Python environment
RUN pip install --no-cache-dir \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
  "apache-airflow==${AIRFLOW_VERSION}" \
  .

# Copy the DAGs and config
COPY --chown=airflow:root dags/ ./dags/
COPY --chown=airflow:root config/ ./config/

# Set PYTHONPATH to ensure gridpulse is importable
ENV PYTHONPATH="/opt/airflow/src"

# Default command (overridden by docker-compose)
CMD ["airflow", "version"]
