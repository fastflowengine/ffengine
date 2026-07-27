# FFEngine Community

Fast Flow Engine Community is the public core of FFEngine.
It includes the community runtime, shared contracts, and Airflow-native orchestration components.

## Repository Role

- Public scope: Community code and shared contracts
- Private counterpart: `ffengine-enterprise`
- Public docs repo: `ffengine-docs`
- Private website: `ffengine-www`

## Current Baseline

- Community Wave baseline: Wave 16
- Latest completed epic: `C23` (Airflow 3.2.2 upgrade)
- Canonical planning source: `handbook/wbs/WBS_COMMUNITY.md`

## Version Compatibility

| Component | Minimum | Tested |
| --- | --- | --- |
| Python | 3.12 | 3.12.x |
| Apache Airflow | 3.2.2 | 3.2.2 |
| psycopg (Postgres) | 3.1 | 3.3.x |
| pyodbc (MSSQL) | 5.0 | 5.0.x |
| oracledb (Oracle) | 2.0 | 3.x |

> Airflow is not officially supported on native Windows runtime. Use Docker/WSL2 for local development.

## Key Runtime Model

- Canonical execution path: `FFEngineOperator`
- Runtime executor (C20+): `LocalExecutor`
- Airflow stack services:
  - `core-airflow-webserver` (`http://localhost:8085`)
  - `core-airflow-scheduler`
  - `core-airflow-dag-processor`
  - `core-postgres` (`localhost:5436`)

## Airflow Login Users

FFEngine Community uses Airflow `FabAuthManager` with FAB database auth
(`AUTH_DB`). Users are stored in the Airflow metadata database `ab_user`
tables. The Docker image sets `AIRFLOW__CORE__AUTH_MANAGER` to
`airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` by
default; containerized runtime must not fall back to Airflow simple/default
auth.

Airflow users are seeded during `airflow-init`. Passwords are not embedded in
the image; set them in `.env` before starting the stack.

| Username | Role | Password env var |
| --- | --- | --- |
| `admin` | Admin | `FFENGINE_AIRFLOW_ADMIN_PASSWORD` |
| `breakglass` | Admin | `FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD` |
| `operator` | Op | `FFENGINE_AIRFLOW_OP_PASSWORD` |
| `viewer` | Viewer | `FFENGINE_AIRFLOW_VIEWER_PASSWORD` |

If any password variable is missing or empty, user seeding fails intentionally.

## Docker Commands

Build and start the local Airflow stack:

```bash
docker compose -f docker/docker-compose.yml --env-file .env up -d --remove-orphans
```

Recreate local stack after compose/env changes:

```bash
docker compose -f docker/docker-compose.yml --env-file .env up -d --force-recreate --remove-orphans
```

Start isolated integration databases:

```bash
docker compose -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```

Test DB ports:
- Postgres: `5435`
- MSSQL: `1433`
- Oracle: `1521`

## Test Commands

Unit tests:

```bash
py -3.12 -m pytest tests/unit/ -q
```

Integration gates:

```bash
$env:FFENGINE_ENABLE_PG_TESTS='1'; py -3.12 -m pytest tests/integration/test_pg_to_pg.py -q
$env:FFENGINE_ENABLE_CROSS_DB_TESTS='1'; py -3.12 -m pytest tests/integration/test_cross_db_flow.py -q
$env:FFENGINE_ENABLE_CROSS_DB_TESTS='1'; py -3.12 -m pytest tests/integration/test_mapping_chain.py -q
$env:FFENGINE_ENABLE_AIRFLOW_AUTH_TESTS='1'; py -3.12 -m pytest tests/integration/test_airflow_auth.py -q
```

## Known Conflict Points

### 1) Line ending warnings on `git add`

Repository policy is LF-first. If you see repeated LF/CRLF warnings:

- Check `.gitattributes`
- Use repo-local git settings:

```bash
git config core.autocrlf false
git config core.eol lf
git config core.safecrlf true
```

### 2) Debug guard hook blocks commit/push

Local hooks block debug artifacts in staged diffs (`debugpy`, `ENABLE_DEBUG`, etc.).
Use `docker/docker-compose.local.debug.yml` for local debug overrides, not production compose files.

## Upgrade and Operational References

- Airflow upgrade backup/restore playbook:
  - `handbook/reference/AIRFLOW_UPGRADE_BACKUP_RESTORE_PLAYBOOK.md`
- Airflow execution bugfix notes/history:
  - `handbook/reference/AIRFLOW_EXECUTION_API_BUGFIX.md`
- Debug UAT playbook:
  - `handbook/reference/DEBUGPY_UAT_PLAYBOOK.md`
- Type mapping policy:
  - `handbook/reference/TYPE_MAPPING_POLICY.md`

## Wave Tracking and Context Links

- WBS source: `handbook/wbs/WBS_COMMUNITY.md`
- Context references:
  - `handbook/context/C12_FLOW_STUDIO_EVOLUTION.md`
  - `handbook/context/C13_DEBUGPY_UAT.md`
  - `handbook/context/C14_UAT_RELEASE_GATES.md`
  - `handbook/context/C22_AIRFLOW_3_2_UPGRADE.md`
  - `handbook/context/C23_AIRFLOW_3_2_2_UPGRADE.md`

## Governance

- Branch model: `main` protected
- Expected controls: PR review + required status checks + no direct push
- Keep local/generated artifacts out of Git (`.gitignore` + untrack if needed)

Organization runbook:
- `handbook/reference/GITHUB_ORGANIZATION_RUNBOOK.md`

## License and Commercial Offerings

FFEngine Community is licensed under the [Apache License 2.0](LICENSE). It may
be used freely for development, testing, data warehouse workloads, and
production deployments without a data-volume restriction.

Future Enterprise capabilities may be distributed separately under commercial
terms and are not part of the FFEngine Community license or distribution.
Professional support and commercial offerings are available from the FFEngine
team.

- Website: https://www.ffengine.com
- Contact: contact@ffengine.com
