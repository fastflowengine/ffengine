# HANDOFF: C19 - DAG Explorer (Folder View)

**Date:** 2026-04-23
**Wave:** 12
**Status:** COMPLETE
**Source Agent:** codex
**Target Agent:** human
**Checkpoint Ref:** `projects/webhook/checkpoints/C19_checkpoint.yaml`

## Changed Files
| File | Action | Notes |
|---|---|---|
| `src/ffengine/ui/plugin.py` | Modified | `Browse > DAG Explorer` menu registration and icon override for menu polish. |
| `src/ffengine/ui/api_app.py` | Modified | Added `/flow-studio/dag-explorer` page and `/flow-studio/api/dag-explorer` data endpoint. |
| `src/ffengine/ui/studio_service.py` | Modified | DAG Explorer data shaping, sorting, latest run derivation, and creation date metadata/file fallback logic. |
| `src/ffengine/ui/templates/dag_explorer/index.html` | Added/Modified | Explorer UI, dark-mode, tree controls, DAG navigation behavior, and grid column updates. |
| `tests/unit/test_flow_studio_api.py` | Modified | DAG Explorer service/API/UI smoke coverage updated. |
| `docker/docker-compose.yml` | Modified | Airflow webserver command patched to allow top-level navigation from plugin iframe. |
| `docker/docker-compose.local.debug.yml` | Modified | Debug compose webserver command aligned with iframe sandbox patch. |
| `docker/docker-compose.local.debug.yml.example` | Modified | Debug compose example aligned with iframe sandbox patch. |
| `handbook/context/C19_DAG_EXPLORER.md` | Added/Modified | C19 scope/gates and closure updates captured. |
| `handbook/wbs/WBS_COMMUNITY.md` | Modified | Wave 12 / C19 marked COMPLETED. |
| `handbook/wbs/EPIC_ARTIFACTS.md` | Modified | C19 artifact contract added. |
| `handbook/AGENTS.md` | Modified | Community execution pointer advanced to Wave 12/12 COMPLETED. |
| `handbook/manifest.txt` | Modified | C19 context entry registered. |
| `projects/webhook/checkpoints/C19_checkpoint.yaml` | Added | C19 checkpoint closed with COMPLETE status. |
| `projects/webhook/checkpoints/C19_handoff.md` | Added | Final handoff artifact for epic closure. |

## Completed Acceptance Criteria
- DAG Explorer is exposed under `Browse` without changing the native Airflow DAG list.
- `GET /flow-studio/dag-explorer` and `GET /flow-studio/api/dag-explorer` are available and stable.
- Folder hierarchy and `external` bucket rendering are deterministic.
- DAG click navigation opens direct DAG detail route (`/dags/{dag_id}`) from plugin context.
- Grid columns show `Latest Run` and `Creation Date`.
- `Creation Date` uses Airflow metadata (`DagVersion` / `SerializedDagModel`) with DAG file timestamp fallback when metadata is unavailable.

## Test Evidence
- `pytest tests/unit/test_flow_studio_api.py -q` -> `95 passed`.
- `pytest tests/unit/test_flow_studio_api.py -q -k dag_explorer` -> `6 passed`.
- Runtime smoke: `/flow-studio/dag-explorer` and `/flow-studio/api/dag-explorer` return `200`.

## Open Risks
- Airflow dist bundle token patch in compose startup is version-sensitive and should be revalidated after Airflow upgrades.
- Filesystem timestamp fallback for creation date may differ by host OS/filesystem behavior.

## Notes For Next Wave
- C19 is fully closed with checkpoint + handoff artifacts.
- Next work should start from backlog prioritization (post-Wave-12 scope selection).
