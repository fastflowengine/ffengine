# Community Quickstart

This guide is the Wave 6 baseline for running FFEngine Community locally and
validating the integration/release gates.

## 1) Prerequisites

- Python `3.12.x`
- Docker Desktop
- Editable install with dev extras:

```bash
py -3.12 -m pip install -e ".[dev]"
```

## 2) Environment Setup

Create a `.env` file in repository root and define at least:

- `PG_TEST_HOST`, `PG_TEST_PORT`, `PG_TEST_USER`, `PG_TEST_PASSWORD`, `PG_TEST_DB`
- `MSSQL_TEST_HOST`, `MSSQL_TEST_PORT`, `MSSQL_TEST_USER`, `MSSQL_TEST_PASSWORD`, `MSSQL_TEST_DB`
- `MSSQL_TEST_DRIVER` (example: `{ODBC Driver 17 for SQL Server}`)
- `ORACLE_TEST_HOST`, `ORACLE_TEST_PORT`, `ORACLE_TEST_USER`, `ORACLE_TEST_PASSWORD`, `ORACLE_TEST_SERVICE`

Start integration databases:

```bash
docker-compose -p ffengine-test -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```

## 3) Test Activation Flags

Cross-DB integration tests are opt-in. Enable before running:

```bash
$env:PYTHONPATH="src"
$env:FFENGINE_ENABLE_CROSS_DB_TESTS="1"
```

## 4) Wave 6 Mandatory Test Commands

Run required cross-DB flows:

```bash
py -3.12 -m pytest tests/integration/test_cross_db_etl.py::test_pg_to_pg tests/integration/test_cross_db_etl.py::test_pg_to_mssql tests/integration/test_cross_db_etl.py::test_pg_to_oracle -q
```

Run mapping chain gate:

```bash
py -3.12 -m pytest tests/integration/test_mapping_chain.py -q
```

Run both together:

```bash
py -3.12 -m pytest tests/integration/test_cross_db_etl.py::test_pg_to_pg tests/integration/test_cross_db_etl.py::test_pg_to_mssql tests/integration/test_cross_db_etl.py::test_pg_to_oracle tests/integration/test_mapping_chain.py -q
```

## 5) Release Prep Checklist (Community)

- Mandatory flows PASS: `PG->PG`, `PG->MSSQL`, `PG->Oracle`
- Mapping chain PASS: `mapping_generator -> config -> DAG -> run -> verify`
- C11 checkpoint/handoff updated
- `README.md` and `handbook/wbs/WBS_COMMUNITY.md` status aligned
