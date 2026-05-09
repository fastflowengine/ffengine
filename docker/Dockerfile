FROM apache/airflow:3.2.1-python3.12

ARG AIRFLOW_VERSION=3.2.1
ARG PYTHON_VERSION=3.12
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

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

RUN curl -fsSL "${CONSTRAINTS_URL}" -o /tmp/constraints.txt && \
    sed -i 's/^apache-airflow-providers-fab==.*/apache-airflow-providers-fab==3.6.2/' /tmp/constraints.txt && \
    pip install --no-cache-dir -e .[dev] --constraint /tmp/constraints.txt

# C18: FabAuthManager + DB auth (SimpleAuthManager replaced).
# apache-airflow-providers-fab and flask-appbuilder are pulled in via pyproject.toml.
COPY --chown=airflow:0 docker/webserver_config.py /opt/airflow/webserver_config.py
COPY --chown=airflow:0 docker/seed_airflow_users.py /opt/airflow/seed_airflow_users.py
