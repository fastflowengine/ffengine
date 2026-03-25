FROM apache/airflow:3.1.6-python3.12

USER root

# Install system dependencies for DB drivers
RUN apt-get update && \
    apt-get install -y gcc g++ unixodbc-dev libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install FFEngine dependencies
COPY --chown=airflow:0 README.md /opt/airflow/
COPY --chown=airflow:0 pyproject.toml /opt/airflow/
COPY --chown=airflow:0 src/ /opt/airflow/src/

RUN pip install --no-cache-dir -e .[dev]
