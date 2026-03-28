# Airflow 3.1.6 Execution API / Scheduler Fix Notes

Last updated: 2026-03-28

This document explains the connection-test DAG failure and the runtime fixes
applied in the FFEngine Docker-based Airflow setup.

## Problem Summary

`ffengine_connection_test` DAG tasks were failing to execute reliably under
Airflow 3.1.6 in local Docker runs. The failure pattern was tied to scheduler
execution flow and internal API auth/config mismatches between components.

Symptoms observed:
- Tasks staying queued or failing before execution
- Scheduler/webserver communication inconsistencies
- Missing provider support for non-Postgres test connections

## Root Causes

1. Executor/runtime mismatch for local dev
- For local deterministic test execution, the environment must force
  `SequentialExecutor`.

2. Internal API/auth settings not aligned across containers
- Airflow 3 execution flow depends on consistent API secret/JWT-related values
  between webserver, scheduler, dag-processor, and init containers.

3. Incomplete provider set in runtime image
- MSSQL and Oracle providers were not guaranteed in image build, causing
  connection-type-specific task failures.

4. Metadata persistence instability in iterative local runs
- Recreating containers without a stable named volume can reset metadata and
  lead to confusing environment state.

## Implemented Fixes

### 1) Runtime image updates
File: `docker/Dockerfile`

- Added provider packages to image build:
  - `apache-airflow-providers-microsoft-mssql`
  - `apache-airflow-providers-oracle`
- Added simple auth manager passwords file and env:
  - `/opt/airflow/simple_auth_manager_passwords.json`
  - `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`

### 2) Compose-level execution/api hardening
File: `docker/docker-compose.yml`

- Added stable compose project name:
  - `name: ffengine-core`
- Forced local executor mode:
  - `AIRFLOW__CORE__EXECUTOR=SequentialExecutor` on init/webserver/scheduler/dag-processor
- Aligned API/auth settings across services:
  - `AIRFLOW__API__SECRET_KEY`
  - `AIRFLOW__API_AUTH__JWT_SECRET`
  - `AIRFLOW__API__BASE_URL`
  - `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`
  - `AIRFLOW__CORE__INTERNAL_API_URL` (scheduler + dag-processor)
  - `AIRFLOW__CORE__FERNET_KEY`
- Enabled Airflow connection test UX:
  - `AIRFLOW__CORE__TEST_CONNECTION=Enabled`
- Enforced JWT secret line in `airflow.cfg` at service startup to prevent drift.
- Added persistent metadata volume:
  - `airflow-pgdata` (named as `ffengine-airflow-pgdata`)

### 3) Test DB compose consistency
File: `docker/docker-compose.test.yml`

- Added stable compose project name:
  - `name: ffengine-test`

### 4) Dedicated connection verification DAG
File: `dags/connection_test_dag.py`

- Added `ffengine_connection_test` DAG with 3 parallel tasks:
  - `test_postgres`
  - `test_mssql`
  - `test_oracle`
- Each task resolves Airflow connection + dialect and runs:
  - `SELECT COUNT(*) FROM ff_test_data`
- Connection IDs are overrideable via env:
  - `FFENGINE_TEST_POSTGRES_CONN_ID`
  - `FFENGINE_TEST_MSSQL_CONN_ID`
  - `FFENGINE_TEST_ORACLE_CONN_ID`

## Reproduction and Verification

Use these commands from repo root:

```bash
docker compose -f docker/docker-compose.yml --env-file .env up -d --remove-orphans
docker compose -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```

Trigger and inspect DAG run:

```bash
docker exec core-airflow-webserver airflow dags trigger ffengine_connection_test
docker exec core-airflow-webserver airflow dags list-runs -d ffengine_connection_test --no-backfill
```

Task-level checks:

```bash
docker exec core-airflow-webserver airflow tasks test ffengine_connection_test test_postgres 2026-03-28
docker exec core-airflow-webserver airflow tasks test ffengine_connection_test test_mssql 2026-03-28
docker exec core-airflow-webserver airflow tasks test ffengine_connection_test test_oracle 2026-03-28
```

Expected result:
- All three tasks complete successfully and return row count metadata.

## Notes for Airflow Developers

- Scope of this fix is local/dev Docker reliability for Airflow 3.1.6.
- The `SequentialExecutor` choice is intentional for deterministic local tests.
- If moving to parallel executors later, keep API/auth and internal URL settings
  consistent first, then change executor mode.
