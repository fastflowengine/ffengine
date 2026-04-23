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

RUN pip install --no-cache-dir -e .[dev] \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-oracle

# Patch: Airflow 3.1.6 bug — DagRun.start_date must be Optional (None when queued)
# All occurrences of non-optional start_date in _generated.py must be patched
# RUN python3 -c "\
# f='/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/api/datamodels/_generated.py'; \
# t=open(f).read(); \
# old='    start_date: Annotated[AwareDatetime, Field(title=\"Start Date\")]'; \
# new='    start_date: Annotated[AwareDatetime | None, Field(title=\"Start Date\")] = None'; \
# t=t.replace(old, new); \
# open(f,'w').write(t); \
# print('Patched ALL DagRun.start_date -> Optional')"

# C18: FabAuthManager + DB auth (SimpleAuthManager replaced).
# apache-airflow-providers-fab and flask-appbuilder are pulled in via pyproject.toml.
COPY --chown=airflow:0 docker/webserver_config.py /opt/airflow/webserver_config.py
COPY --chown=airflow:0 docker/seed_airflow_users.py /opt/airflow/seed_airflow_users.py
