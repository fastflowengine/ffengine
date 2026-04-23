# HANDOFF: C18 - Airflow FabAuthManager + DB Auth

**Date:** 2026-04-23
**Wave:** 11
**Status:** COMPLETE
**Source Agent:** claude
**Target Agent:** human
**Checkpoint Ref:** `projects/webhook/checkpoints/C18_checkpoint.yaml`

## Changed Files

| File | Action | Notes |
|---|---|---|
| `pyproject.toml` | Modified | Added `apache-airflow-providers-fab>=3.6.1` and `flask-appbuilder>=4.3.0`. |
| `docker/Dockerfile` | Modified | Removed SimpleAuthManager JSON password file + env var; copies `docker/webserver_config.py` and `docker/seed_airflow_users.py` into image. |
| `docker/webserver_config.py` | Added | `AUTH_TYPE = AUTH_DB`, registration disabled, session cookie hardening. Monkey-patches `airflow.api_fastapi.app.create_auth_manager` to return the existing global instead of overwriting it — workaround for FAB 3.6.1 bug where `AirflowAppBuilder.__init__` clobbers the auth_manager singleton and leaves `flask_app=None`, causing `GET /auth/logout` to 500 with "Flask app is not initialized". |
| `docker/seed_airflow_users.py` | Added | Python-based FAB SecurityManager seed (Airflow 3 removed `airflow users create` CLI); idempotent create of admin/breakglass/operator/viewer with env-var-overridable passwords. |
| `docker/docker-compose.yml` | Modified | `AIRFLOW__CORE__AUTH_MANAGER=...FabAuthManager` on all 4 Airflow services. `airflow-init` runs `airflow db migrate` (which auto-creates FAB tables under FabAuthManager) then executes the Python seed script. `AIRFLOW__API__BASE_URL` set to `http://localhost:8085` on webserver/scheduler/dag-processor so UI-generated redirects (login, logout) resolve in the browser; inter-service dialing still uses `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` / `AIRFLOW__CORE__INTERNAL_API_URL` with the container hostname. |
| `tests/integration/test_airflow_auth.py` | Added | httpx-based login + docker-exec RBAC + artefact absence tests. Gated by `FFENGINE_ENABLE_AIRFLOW_AUTH_TESTS=1`. |
| `handbook/reference/AIRFLOW3_PLUGIN_STANDARD.md` | Modified | §7 Güvenlik updated to note FAB RBAC is active and Flow Studio API-key is a complementary guard. |
| `handbook/reference/AIRFLOW_EXECUTION_API_BUGFIX.md` | Modified | SimpleAuthManager bullet replaced with C18 migration note. |
| `handbook/context/C18_AIRFLOW_FAB_DB_AUTH.md` | Modified | İlerleme Notları populated with implementation log. |
| `README.md` | Modified | Airflow login credentials table + prod env var requirement + break-glass note. |
| `handbook/wbs/WBS_COMMUNITY.md` | Modified | C18 row status → COMPLETED. |
| `handbook/wbs/EPIC_ARTIFACTS.md` | Modified | C18 artifact section added. |
| `handbook/manifest.txt` | Modified | C18 entries registered. |
| `handbook/AGENTS.md` | Modified | §56 pointer bumped to Wave 11/11 C18 COMPLETED. |

## Completed Acceptance Criteria

- FabAuthManager active across init/webserver/scheduler/dag-processor; `AIRFLOW__CORE__AUTH_MANAGER` pinned.
- SimpleAuthManager artefacts (`simple_auth_manager_passwords.json`, env var) removed from Dockerfile.
- `airflow-init` seeds 4 users (`admin` Admin, `breakglass` Admin, `operator` Op, `viewer` Viewer) idempotently.
- `docker/webserver_config.py` enforces `AUTH_TYPE = AUTH_DB` and disables self-registration.
- Break-glass admin (second Admin account) seeded.
- JWT handshake env (`AIRFLOW__API_AUTH__JWT_SECRET`) preserved — inter-service flow regression-safe.
- Integration test suite covers login, RBAC enforcement, artefact absence, and ab_user population.
- Handbook, README, plugin standard, and execution API bugfix notes updated for C18.

## Open Risks

- Manual UAT against running docker stack still a reviewer gate (golden path: 3 role logins + 1 RBAC negative check).
- Dev default passwords must be overridden in prod via `FFENGINE_AIRFLOW_*_PASSWORD`; release playbook should block default usage.
- Break-glass credential rotation policy lives in ops playbook; not enforced by code.

## Notes For Next Wave

- OIDC/SSO (candidate C19) will swap `AUTH_TYPE = AUTH_DB` for `AUTH_TYPE = AUTH_OAUTH` in `docker/webserver_config.py`; DB user table stays as fallback.
- Password complexity / MFA is explicitly out of scope for C18 — revisit when enterprise hardening epic opens.
- If `ab_user*` backup procedure is added, reference it from handbook/context/C18 Gate Criteria.
