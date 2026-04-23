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

| Component           | Minimum | Tested |
| ------------------- | ------- | ------ |
| Python              | 3.12    | 3.12.x |
| Apache Airflow      | 3.0.0   | 3.1.6+ |
| psycopg2 (Postgres) | 2.9     | 2.9.x  |
| pyodbc (MSSQL)      | 4.0     | 4.0.x  |
| oracledb (Oracle)   | 2.0     | 2.x    |

> **Note:** Airflow is not officially supported on Windows. Use WSL2 or Docker for local development.

## Development Progress (Wave Plan)

| Wave | Epic | Status  | Description                                                               |
| ---- | ---- | ------- | ------------------------------------------------------------------------- |
| 1    | C01  | ✅ Done | Project scaffold, Docker, CI skeleton                                     |
| 2    | C02  | ✅ Done | DBSession + AirflowConnectionAdapter                                      |
| 3    | C03  | ✅ Done | Dialect layer (Postgres / MSSQL / Oracle)                                 |
| 3    | C05  | ✅ Done | YAML Config                                                               |
| 4    | C04  | ✅ Done | Core Engine                                                               |
| 4    | C06  | ✅ Done | Partition                                                                 |
| 4    | C09  | ✅ Done | Mapping Tools                                                             |
| 5    | C07  | ✅ Done | Airflow Operator & DAG                                                    |
| 5    | C08  | ✅ Done | ETL Studio (Airflow 3 FastAPI plugin)                                     |
| 5    | C10  | ✅ Done | Error Handling (typed exceptions, handler normalization, structured logs) |
| 6    | C11  | ✅ Done | Integration Tests & Release                                               |

## UI Architecture Standard

- Airflow 3 plugin extensions (ETL Studio) follow `fastapi_apps + external_views` as the
  standard model.

## Airflow Execution Model

- Canonical execution path is `FFEngineOperator` only.
- `plan/prepare/run` phases are orchestrated inside `src/ffengine/airflow/operator.py`.
- Local runtime executor (C20): `LocalExecutor`.
- Default C20 tuning in compose:
  - `AIRFLOW__CORE__PARALLELISM=8`
  - `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4`
  - `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=2`
- XCom policy is minimal for performance:
  - `rows_transferred`
  - `duration_seconds`
  - `rows_per_second`
  - `retry_telemetry`
  - `error_summary` (only on failure)
- Large intermediate phase payloads are not pushed to XCom.

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
- Update mode deep-link:
  `/etl-studio/?dag_id=<dag_id>` opens ETL Studio with DAG config preload.
- Legacy DAG IDs (`ffengine_config_*`, `ffengine_*`) are read-only for update mode; ETL Studio shows a migration guard instead of silent fallback.

## ETL Studio Form Notes

- Schema/table autocomplete is case-insensitive.
- Connection dropdowns are loaded from Airflow connections; `Bindings -> Airflow Variable` is a separate optional source.
- Target table input is not restricted to datalist options; manual entry is supported.
- For MSSQL connections, schema is typically `dbo` (not `public`).

Environment defaults (if not overridden):

- `FFENGINE_STUDIO_PROJECTS_ROOT=/opt/airflow/projects`
- `FFENGINE_STUDIO_DAG_ROOT=/opt/airflow/dags`

## Breaking Changes

- `ffengine.airflow.XComKeys` and `ffengine.airflow.build_task_group` are no longer public API.
- Use `ffengine.airflow.operator.FFEngineOperator` for all Airflow execution flows.

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
  [`handbook/reference/AIRFLOW_EXECUTION_API_BUGFIX.md`](handbook/reference/AIRFLOW_EXECUTION_API_BUGFIX.md)

## Debugpy UAT

- Docker icinde asamali breakpoint UAT adimlari local ekip notu olarak tutulur.

## Type Mapping Contract

- Cross-dialect type conversion policy and matrix:
  [`handbook/reference/TYPE_MAPPING_POLICY.md`](handbook/reference/TYPE_MAPPING_POLICY.md)

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

**Airflow Login Credentials (C18 — FabAuthManager + DB auth):**

Dev defaults (override via env vars in prod):

| Username     | Role   | Default password | Env var                              |
| ------------ | ------ | ---------------- | ------------------------------------ |
| `admin`      | Admin  | `admin`          | `FFENGINE_AIRFLOW_ADMIN_PASSWORD`    |
| `breakglass` | Admin  | `breakglass`     | `FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD` |
| `operator`   | Op     | `operator`       | `FFENGINE_AIRFLOW_OP_PASSWORD`       |
| `viewer`     | Viewer | `viewer`         | `FFENGINE_AIRFLOW_VIEWER_PASSWORD`   |

- **Prod**: `FFENGINE_AIRFLOW_*_PASSWORD` ortam degiskenleri zorunludur; dev
  default'lari kullanmayin.
- **Break-glass**: `breakglass` hesabi acil durum Admin erisimi icindir;
  parolasi vault'ta tutulmali ve kullanildiginda rotate edilmelidir.

**Start the Core Cluster (Airflow):**

```bash
docker-compose -p ffengine-core -f docker/docker-compose.yml --env-file .env up -d --remove-orphans
```

_(This launches the Webserver on port 8085, the internal background Scheduler, the Airflow 3 DAG Processor, and the Metadata Postgres database.)_

**Apply config changes after compose edits (recreate):**

```bash
docker-compose -p ffengine-core -f docker/docker-compose.yml --env-file .env up -d --force-recreate --remove-orphans
```

**Verify LocalExecutor is active (C20):**

```bash
docker exec core-airflow-webserver airflow config get-value core executor
docker exec core-airflow-scheduler airflow config get-value core executor
```

Expected output on both commands: `LocalExecutor`

**LocalExecutor smoke DAG (parallel evidence):**

```bash
docker exec core-airflow-webserver airflow dags trigger ffengine_local_executor_smoke
docker exec core-airflow-webserver airflow dags list-runs ffengine_local_executor_smoke --no-backfill
docker exec core-airflow-webserver airflow tasks states-for-dag-run ffengine_local_executor_smoke <dag_run_id>
```

Acceptance hint: `probe_a`, `probe_b`, `probe_c` task time windows should overlap in the same DAG run.

**Start the Test Databases (Isolated):**

```bash
docker-compose -p ffengine-test -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```

## Wave Tracking

- Canonical WBS source: [`handbook/wbs/WBS_COMMUNITY.md`](handbook/wbs/WBS_COMMUNITY.md)
- Wave 7 contexts:
  - [`handbook/context/C12_ETL_STUDIO_EVOLUTION.md`](handbook/context/C12_ETL_STUDIO_EVOLUTION.md)
  - [`handbook/context/C13_DEBUGPY_UAT.md`](handbook/context/C13_DEBUGPY_UAT.md)
  - [`handbook/context/C14_UAT_RELEASE_GATES.md`](handbook/context/C14_UAT_RELEASE_GATES.md)
