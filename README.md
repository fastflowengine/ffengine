# FFEngine Community

Fast Flow Engine Community is the public core of the FFEngine product family.
It provides the community runtime, shared contracts, and the foundation that
the private Enterprise extension builds on.

## Repository Role

This repository is intended to become the public `ffengine/ffengine` repository.

- Public scope: Community code and shared contracts
- Private counterpart: `ffengine-enterprise`
- Public docs: `ffengine-docs`
- Private website: `ffengine-www`

## Version Compatibility

| Component | Minimum | Tested |
|---|---|---|
| Python | 3.12 | 3.12.x |
| Apache Airflow | 3.0.0 | 3.1.6+ |
| psycopg2 (Postgres) | 2.9 | 2.9.x |
| pyodbc (MSSQL) | 4.0 | 4.0.x |
| oracledb (Oracle) | 2.0 | 2.x |

> **Note:** Airflow is not officially supported on Windows. Use WSL2 or Docker for local development.

## Development Progress (Wave Plan)

| Wave | Epic | Status | Description |
|---|---|---|---|
| 1 | C01 | ✅ Done | Project scaffold, Docker, CI skeleton |
| 2 | C02 | ✅ Done | DBSession + AirflowConnectionAdapter |
| 3 | C03 | ✅ Done | Dialect layer (Postgres / MSSQL / Oracle) |
| 3 | C05 | ✅ Done | YAML Config |
| 4 | C04 | ✅ Done | Core Engine |
| 4 | C06 | ✅ Done | Partition |
| 4 | C09 | ✅ Done | Mapping Tools |
| 5 | C07 | ✅ Done | Airflow Operator & DAG |
| 5 | C08 | ✅ Done | ETL Studio (Airflow 3 FastAPI plugin) |
| 5 | C10 | ✅ Done | Error Handling (typed exceptions, handler normalization, structured logs) |
| 6 | C11 | ✅ Done | Integration Tests & Release |

## UI Architecture Standard

- Airflow 3 plugin extensions (ETL Studio) follow `fastapi_apps + external_views` as the
  standard model.

## ETL Studio Hierarchical Model

ETL Studio now uses a hard-switched hierarchical model for YAML and DAG generation.

- Projects root (required shape): `FFENGINE_STUDIO_PROJECTS_ROOT/<project>/<domain>/<level>/<flow>/`
- YAML naming:
  `<project>_<domain>_<level>_<flow>_group_<n>.yaml` (`n` is positive integer)
- DAG root mirrors the same hierarchy:
  `FFENGINE_STUDIO_DAG_ROOT/<project>/<domain>/<level>/<flow>/`
- DAG naming standard:
  `<domain>_to_<flow_target>_<level>_group_<n>_dag.py`
- One DAG Python file per group under the flow path; each DAG parses its own YAML and creates `FFEngineOperator` tasks from `etl_tasks`.

Environment defaults (if not overridden):

- `FFENGINE_STUDIO_PROJECTS_ROOT=/opt/airflow/projects`
- `FFENGINE_STUDIO_DAG_ROOT=/opt/airflow/dags`

## Installation

```bash
pip install -e .
```

## Community Quickstart (Wave 6)

- Quickstart and release prep guide: [`docs/community_quickstart.md`](docs/community_quickstart.md)
- Integration tests are opt-in via `FFENGINE_ENABLE_CROSS_DB_TESTS=1`.
- Wave 6 mandatory gate command:

```bash
py -3.12 -m pytest tests/integration/test_cross_db_etl.py::test_pg_to_pg tests/integration/test_cross_db_etl.py::test_pg_to_mssql tests/integration/test_cross_db_etl.py::test_pg_to_oracle tests/integration/test_mapping_chain.py -q
```

## Airflow Bugfix Notes

- Detailed Airflow 3.1.6 execution/scheduler bugfix and verification guide:
  [`docs/airflow-execution-api-bugfix.md`](docs/airflow-execution-api-bugfix.md)

## Governance

- Repository ownership model: GitHub organization, not a shared user account
- Required branch: `main`
- Expected protections: PR review, status checks, no direct push to `main`
- Local and generated artifacts must stay out of Git; update `.gitignore` and
  untrack accidental binary/runtime files before push

See [docs/github-organization-runbook.md](/c:/fast-flow/FFEngineCommunity/docs/github-organization-runbook.md)
for the organization bootstrap and migration steps.

## Development & Test Environment (Docker)

To avoid local port conflicts, the FFEngine development environment allocates isolated ports for the testing cluster. The `.env` file must be present at the root directory to store database credentials.

**Active Port Mappings:**
- **Airflow Web UI (`core-airflow-webserver`):** `http://localhost:8085`
- **Airflow Postgres (`core-postgres`):** `localhost:5436`
- **Test Postgres (`test-postgres`):** `localhost:5435`
- **Test MSSQL (`test-mssql`):** `localhost:1433`
- **Test Oracle (`test-oracle`):** `localhost:1521`

**Start the Core Cluster (Airflow):**
```bash
docker-compose -p ffengine-core -f docker/docker-compose.yml --env-file .env up -d --remove-orphans
```
*(This launches the Webserver on port 8085, the internal background Scheduler, the Airflow 3 DAG Processor, and the Metadata Postgres database.)*

**Start the Test Databases (Isolated):**
```bash
docker-compose -p ffengine-test -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```
