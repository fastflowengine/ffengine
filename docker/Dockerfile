FROM apache/airflow:3.2.2-python3.12 AS wheel-builder

USER root

# Build dependencies are kept in the builder stage only.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ unixodbc-dev libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /tmp/ffengine-build

COPY --chown=airflow:0 README.md LICENSE pyproject.toml ./
COPY --chown=airflow:0 src/ ./src/

RUN pip wheel --no-cache-dir --wheel-dir /tmp/ffengine-wheels . \
    --constraint "${HOME}/constraints.txt"

FROM apache/airflow:3.2.2-python3.12

USER root

# Runtime libraries only. Keep compilers and headers out of the final image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends unixodbc libpq5 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# C18 invariant: FFEngine Community uses FAB DB auth by default.
# Do not rely only on Compose for this; containers must not fall back to
# Airflow's default/simple auth behavior when run directly.
ENV AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins

COPY --from=wheel-builder --chown=airflow:0 /tmp/ffengine-wheels /tmp/ffengine-wheels

RUN pip install --no-cache-dir --no-index --find-links=/tmp/ffengine-wheels \
    ffengine \
    --constraint "${HOME}/constraints.txt" && \
    rm -rf /tmp/ffengine-wheels

# FAB auth and FFEngine plugin bootstrap files.
COPY --chown=airflow:0 docker/webserver_config.py /opt/airflow/webserver_config.py
COPY --chown=airflow:0 docker/seed_airflow_users.py /opt/airflow/seed_airflow_users.py
COPY --chown=airflow:0 plugins/flow_studio_plugin.py /opt/airflow/plugins/flow_studio_plugin.py
