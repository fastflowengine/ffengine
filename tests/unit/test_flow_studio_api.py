"""
C08_T13 - Flow Studio FastAPI endpoint unit/API tests.
"""

from __future__ import annotations

import json
import shutil
import textwrap
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import ValidationError

import ffengine.ui.api_app as api_app_module
from ffengine.ui.api_app import DagUpsertPayload, flow_studio_app
from ffengine.errors import ConnectionError
from ffengine.ui import studio_service as ss


@pytest.fixture
def client():
    return TestClient(flow_studio_app)


@pytest.fixture
def studio_paths(monkeypatch):
    base = Path("logs") / "flow_studio_test_tmp" / f"case_{uuid.uuid4().hex}"
    proj = base / "projects"
    gen = base / "dags"
    proj.mkdir(parents=True, exist_ok=True)
    gen.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FFENGINE_STUDIO_PROJECTS_ROOT", str(proj))
    monkeypatch.setenv("FFENGINE_STUDIO_DAG_ROOT", str(gen))
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "0")
    try:
        yield proj, gen
    finally:
        shutil.rmtree(base, ignore_errors=True)


def _minimal_table_payload():
    return {
        "project": "webhook",
        "domain": "whk",
        "level": "level1",
        "flow": "src_to_stg",
        "source_conn_id": "src_c",
        "target_conn_id": "tgt_c",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "dwh",
        "target_table": "orders_stg",
        "source_type": "table",
        "load_method": "append",
    }


def _auto_id_ambiguous_param_payload():
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_left",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-01",
            }],
            "depends_on": [],
        },
        {
            "task_type": "binding",
            "task_group_id": "bind_right",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-02",
            }],
            "depends_on": [],
        },
        {
            "task_type": "source_target",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_left", "bind_right"],
        },
    ]
    return payload


def _sql_mapping_yaml(columns: list[str]) -> str:
    lines = [
        "version: v1",
        "source_dialect: postgres",
        "target_dialect: postgres",
        "columns:",
    ]
    for col in columns:
        lines.extend(
            [
                f"  - source_name: {col}",
                f"    target_name: {col}",
                "    source_type: TEXT",
                "    target_type: TEXT",
                "    nullable: true",
            ]
        )
    return "\n".join(lines) + "\n"


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["service"] == "flow-studio"
    assert ss.STUDIO_DAG_MARKER in data.get("dag_marker", "")


def test_index_html_ok(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "Flow Studio" in r.text
    assert "/flow-studio/static/flow_studio/css/style.css" in r.text
    assert "/flow-studio/static/flow_studio/js/app.js" in r.text
    assert "theme_notice" in r.text
    assert "theme_source_debug" in r.text
    assert 'class="flow-studio-root"' in r.text
    assert "preload_dag_id" not in r.text
    assert "Load DAG Context" not in r.text
    assert "folder_path_display" in r.text
    assert "Select / Create Folder" in r.text
    assert 'placeholder="Select a project and DAG path"' in r.text
    assert 'id="folder_path_display" value=""' in r.text
    assert 'id="project" value=""' in r.text
    assert 'id="domain" value=""' in r.text
    assert 'id="level" value=""' in r.text
    assert 'id="flow" value=""' in r.text
    assert "Source Connection" in r.text
    assert "Select Source Connection" in r.text
    assert "Target Connection" in r.text
    assert "Select Target Connection" in r.text
    assert "Source DB Connection" not in r.text
    assert "Target DB Connection" not in r.text
    assert "folder_picker_modal" in r.text
    assert "Group No" not in r.text
    assert "Source Target" in r.text
    assert "Script" in r.text
    assert "Filter & Bindings" in r.text
    assert "Task Group ID (Optional)" not in r.text
    assert "task-group-id-readonly" in r.text
    assert "Expand All" in r.text
    assert "Collapse All" in r.text
    assert "Save Configuration" in r.text
    assert "+ Add New Task" in r.text
    assert "Custom Tags" in r.text
    assert "custom_tags_input" in r.text
    assert "Scheduler" in r.text
    assert "Configure Scheduler" in r.text
    assert "Cron Expression (5 fields)" in r.text
    assert "scheduler_timezone_options" in r.text
    assert "scheduler_modal" in r.text
    assert "scheduler_compact_summary" in r.text
    assert "DAG Dependencies" not in r.text
    assert "dag_deps_modal" not in r.text
    assert "Advanced" in r.text
    assert "advanced_modal" in r.text
    assert "advanced_compact_summary" in r.text
    assert 'data-task-type="binding"' in r.text
    assert "Delete DAG" in r.text
    assert "delete_dag_modal" in r.text
    assert "delete_task_modal" in r.text
    assert "Update DAG + YAML" not in r.text
    assert "Load Timeline" not in r.text
    assert "Timeline DAG ID (optional)" not in r.text
    assert "Timeline State (optional)" not in r.text
    assert "Timeline Limit" not in r.text


def test_binding_task_selection_auto_creates_first_parameter_row():
    app_js = (
        Path(api_app_module.__file__).parent
        / "static"
        / "flow_studio"
        / "js"
        / "app.js"
    ).read_text(encoding="utf-8")
    assert "function ensureBindingRowForBindingTask(card)" in app_js
    assert "!getBindingRows(card).length" in app_js
    assert "ensureBindingRowForBindingTask(card);" in app_js


def test_binding_task_hides_empty_source_card_and_uses_advanced_only_layout():
    ui_root = Path(api_app_module.__file__).parent
    app_js = (
        ui_root / "static" / "flow_studio" / "js" / "app.js"
    ).read_text(encoding="utf-8")
    style_css = (
        ui_root / "static" / "flow_studio" / "css" / "style.css"
    ).read_text(encoding="utf-8")
    index_html = (
        ui_root / "templates" / "flow_studio" / "index.html"
    ).read_text(encoding="utf-8")

    assert 'const sourceCard = card.querySelector(".source-card");' in app_js
    assert (
        'card.classList.toggle("binding-task", taskType === TASK_TYPES.BINDING);'
        in app_js
    )
    assert (
        'sourceCard?.classList.toggle("hidden", taskType === TASK_TYPES.BINDING);'
        in app_js
    )
    assert ".task-card.binding-task .task-layout" in style_css
    assert 'grid-template-areas: "advanced";' in style_css
    assert "app.js?v=100" in index_html


def test_advanced_dag_parameter_uses_parameter_type_label():
    index_html = (
        Path(api_app_module.__file__).parent
        / "templates"
        / "flow_studio"
        / "index.html"
    ).read_text(encoding="utf-8")

    assert "Parameter Type" in index_html
    assert "Data Type" not in index_html
    assert "style.css?v=65" in index_html


def test_connection_selector_uses_generic_source_and_target_labels():
    ui_root = Path(api_app_module.__file__).parent
    app_js = (
        ui_root / "static" / "flow_studio" / "js" / "app.js"
    ).read_text(encoding="utf-8")
    index_html = (
        ui_root / "templates" / "flow_studio" / "index.html"
    ).read_text(encoding="utf-8")

    assert "Select Source Connection" in app_js
    assert "Select Target Connection" in app_js
    assert "DB Connection" not in app_js
    assert "database connection" not in index_html
    assert "app.js?v=100" in index_html


def test_binding_ui_has_conditional_default_and_searchable_variable_selector():
    app_js = (
        Path(api_app_module.__file__).parent
        / "static"
        / "flow_studio"
        / "js"
        / "app.js"
    ).read_text(encoding="utf-8")
    assert "binding-default-wrap hidden" in app_js
    assert 'role="combobox"' in app_js
    assert 'role="listbox"' in app_js
    assert "limit=50" in app_js
    assert "validateAirflowVariableKey" in app_js
    assert "validateAllAirflowVariableBindings" in app_js


def test_dag_parameter_ui_uses_namespaced_binding_contract():
    app_js = (
        Path(api_app_module.__file__).parent
        / "static"
        / "flow_studio"
        / "js"
        / "app.js"
    ).read_text(encoding="utf-8")
    assert "dag-param-default" not in app_js
    assert "dag-param-required" not in app_js
    assert "DAG Parameter Bindings" in app_js
    assert "binding-variable-name-select" in app_js
    assert "validateAirflowNamespaceKeys" in app_js
    assert "legacyDagParamMigrationPending" not in app_js
    assert "Legacy custom DAG parameter default/required fields detected" not in app_js
    assert "compileDagParameterFlow" in app_js
    assert "require at least one Binding task assignment" not in app_js
    assert "must directly depend on the Binding task" not in app_js
    assert "Ambiguous DAG parameter source" in app_js
    assert "must be assigned exactly once" not in app_js


def test_binding_ui_prevents_duplicate_names_within_each_task():
    app_js = (
        Path(api_app_module.__file__).parent
        / "static"
        / "flow_studio"
        / "js"
        / "app.js"
    ).read_text(encoding="utf-8")

    assert "function selectedDagBindingNames" in app_js
    assert "option.disabled = selectedByOtherRows.has(name);" in app_js
    assert 'variableSelect.addEventListener("change"' in app_js
    assert "function validateUniqueTaskBindingNames" in app_js
    assert "validateUniqueTaskBindingNames(payload.flow_tasks || []);" in app_js
    assert "is defined more than once in task" in app_js


def test_folder_path_ui_requires_explicit_selection():
    ui_root = Path(api_app_module.__file__).parent
    app_js = (
        ui_root / "static" / "flow_studio" / "js" / "app.js"
    ).read_text(encoding="utf-8")
    index_html = (
        ui_root / "templates" / "flow_studio" / "index.html"
    ).read_text(encoding="utf-8")

    assert 'const FOLDER_PATH_PROMPT = "Select a project and DAG path";' in app_js
    assert "requireFolderSelection" in app_js
    assert app_js.count("if (!validateFolderSelectionBeforeSubmit()) return;") == 2
    assert 'el("project").value.trim() || "webhook"' not in app_js
    assert '(el("project").value || "").trim() || "webhook"' not in app_js
    assert 'el("domain").value.trim() || "default_domain"' not in app_js
    assert '(el("domain").value || "").trim() || "default_domain"' not in app_js
    assert 'el("level").value.trim() || "level1"' not in app_js
    assert '(el("level").value || "").trim() || "level1"' not in app_js
    assert 'el("flow").value.trim() || "src_to_stg"' not in app_js
    assert '(el("flow").value || "").trim() || "src_to_stg"' not in app_js
    assert "app.js?v=100" in index_html


def test_dag_explorer_html_ok(client):
    r = client.get("/dag-explorer")
    assert r.status_code == 200
    assert "DAG Explorer" in r.text
    assert "/api/dag-explorer" in r.text
    assert "Folders" in r.text
    assert "No DAG in this folder." in r.text


def test_schemas_mocked(client):
    with patch.object(
        api_app_module, "discover_schemas", return_value=["public", "dwh"]
    ):
        r = client.get("/api/schemas?conn_id=test_pg")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 2
    assert "public" in body["items"]


def test_api_dag_explorer_mocked(client):
    mocked = {
        "root": "/opt/airflow/dags",
        "count": 2,
        "items": [
            {
                "dag_id": "a_dag",
                "is_paused": False,
                "owners": ["alice"],
                "fileloc": "/opt/airflow/dags/team/a.py",
                "latest_run": "2026-04-23T20:00:00+00:00",
                "create_date": "2026-04-20T12:00:00+00:00",
                "relative_path": "team/a.py",
                "folder_parts": ["team"],
                "bucket": "dags_root",
                "dag_url": "/dags/a_dag",
            },
            {
                "dag_id": "x_dag",
                "is_paused": True,
                "owners": [],
                "fileloc": "/external/x.py",
                "latest_run": None,
                "create_date": None,
                "relative_path": None,
                "folder_parts": [],
                "bucket": "external",
                "dag_url": "/dags/x_dag",
            },
        ],
    }
    with patch.object(
        api_app_module, "discover_dag_explorer_items", return_value=mocked
    ):
        r = client.get("/api/dag-explorer")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["root"] == "/opt/airflow/dags"
    assert body["count"] == 2
    assert body["items"][0]["bucket"] == "dags_root"
    assert body["items"][1]["bucket"] == "external"


def test_api_dag_explorer_maps_connection_error_to_502(client):
    with patch.object(
        api_app_module,
        "discover_dag_explorer_items",
        side_effect=ConnectionError("db offline"),
    ):
        r = client.get("/api/dag-explorer")
    assert r.status_code == 502
    assert r.json()["detail"] == "db offline"


def test_schemas_mocked_forwards_q_and_limit(client):
    with patch.object(
        api_app_module, "discover_schemas", return_value=["public"]
    ) as mocked:
        r = client.get("/api/schemas?conn_id=test_pg&q=pub&limit=25")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["items"] == ["public"]
    mocked.assert_called_once_with("test_pg", search="pub", limit=25)


def test_schemas_maps_connection_error_to_502(client):
    with patch.object(
        api_app_module, "discover_schemas", side_effect=ConnectionError("db offline")
    ):
        r = client.get("/api/schemas?conn_id=test_pg")
    assert r.status_code == 502
    assert r.json()["detail"] == "db offline"


def test_tables_typeahead_too_short(client):
    r = client.get(
        "/api/tables?conn_id=x&schema=public&q=a",
    )
    assert r.status_code == 400


def test_tables_mocked(client):
    with patch.object(
        api_app_module,
        "discover_tables",
        return_value={
            "schema": "public",
            "total": 1,
            "limit": 50,
            "offset": 0,
            "items": ["orders"],
        },
    ):
        r = client.get("/api/tables?conn_id=x&schema=public&q=or")
    assert r.status_code == 200
    assert r.json()["items"] == ["orders"]


def test_columns_mocked(client):
    cols = [
        {
            "name": "id",
            "data_type": "INTEGER",
            "nullable": False,
            "precision": None,
            "scale": None,
        }
    ]
    with patch.object(api_app_module, "discover_columns", return_value=cols):
        r = client.get("/api/columns?conn_id=x&schema=public&table=orders")
    assert r.status_code == 200
    assert r.json()["count"] == 1


def test_mapping_generate_mocked(client):
    mocked = {
        "mapping_content": "version: v1\ncolumns: []\n",
        "generated_mapping_file": "mapping/1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml",
        "warnings": [],
        "column_count": 0,
    }
    with patch.object(
        api_app_module, "generate_mapping_preview", return_value=mocked
    ) as fn:
        r = client.post(
            "/api/mapping/generate",
            json={
                "project": "webhook",
                "domain": "whk",
                "level": "level1",
                "flow": "src_to_stg",
                "source_conn_id": "src_c",
                "target_conn_id": "tgt_c",
                "source_type": "table",
                "source_schema": "public",
                "source_table": "orders",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert (
        body["generated_mapping_file"]
        == "mapping/1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    fn.assert_called_once()


def test_mapping_generate_rejects_unselected_folder_path(client):
    payload = {
        "project": "   ",
        "domain": "whk",
        "level": "level1",
        "flow": "src_to_stg",
        "source_conn_id": "src_c",
        "target_conn_id": "tgt_c",
        "source_type": "table",
        "source_schema": "public",
        "source_table": "orders",
    }
    with patch.object(api_app_module, "generate_mapping_preview") as fn:
        response = client.post("/api/mapping/generate", json=payload)

    assert response.status_code == 422
    assert "Select a project and DAG path" in response.text
    fn.assert_not_called()


def test_connections_mocked(client):
    conns = [
        {"conn_id": "ffengine_source", "conn_type": "postgres"},
        {"conn_id": "ffengine_target", "conn_type": "mssql"},
    ]
    with patch.object(api_app_module, "discover_connections", return_value=conns):
        r = client.get("/api/connections")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 2
    assert body["items"][0]["conn_id"] == "ffengine_source"


def test_airflow_variables_mocked(client):
    with patch.object(
        api_app_module, "discover_airflow_variables", return_value=["k1", "k2"]
    ) as mocked:
        r = client.get("/api/airflow-variables?q=k&limit=50")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 2
    assert body["items"] == ["k1", "k2"]
    mocked.assert_called_once_with(search="k", limit=50, exact=False)


def test_airflow_variables_exact_lookup_is_case_sensitive(client):
    with patch.object(
        api_app_module,
        "discover_airflow_variables",
        return_value=["ETL.BusinessDate"],
    ) as mocked:
        r = client.get(
            "/api/airflow-variables?q=ETL.BusinessDate&limit=1&exact=true"
        )
    assert r.status_code == 200
    assert r.json()["items"] == ["ETL.BusinessDate"]
    mocked.assert_called_once_with(
        search="ETL.BusinessDate", limit=1, exact=True
    )


def test_timezones_mocked(client):
    with patch.object(
        api_app_module, "discover_timezones", return_value=["UTC", "Europe/Istanbul"]
    ) as mocked:
        r = client.get("/api/timezones?q=eu&limit=25")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 2
    assert body["items"] == ["UTC", "Europe/Istanbul"]
    mocked.assert_called_once_with(search="eu", limit=25)


def test_folder_options_mocked(client):
    data = {
        "projects": ["webhook", "ocean"],
        "domains": ["whk"],
        "levels": ["level1", "level2"],
        "flows": ["src_to_stg"],
    }
    with patch.object(api_app_module, "discover_hierarchy_options", return_value=data):
        r = client.get("/api/folder-options?project=webhook&domain=whk&level=level1")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["projects"] == ["webhook", "ocean"]
    assert body["flows"] == ["src_to_stg"]


def test_folder_options_source_param_passed(client):
    data = {
        "projects": ["webhook"],
        "domains": ["whk"],
        "levels": ["level1"],
        "flows": ["src_to_stg"],
    }
    with patch.object(
        api_app_module, "discover_hierarchy_options", return_value=data
    ) as mocked:
        r = client.get("/api/folder-options?project=webhook&source=dag")
    assert r.status_code == 200
    mocked.assert_called_once_with(
        project="webhook",
        domain=None,
        level=None,
        source="dag",
    )


def test_folder_options_reads_real_dag_hierarchy(client, studio_paths):
    _proj, dag_root = studio_paths
    (dag_root / "webhook" / "whk" / "level1" / "src_to_stg").mkdir(
        parents=True, exist_ok=True
    )
    (dag_root / "test" / "public_level1" / "src_to_odc").mkdir(
        parents=True, exist_ok=True
    )

    root_resp = client.get("/api/folder-options?source=dag")
    assert root_resp.status_code == 200
    root_body = root_resp.json()
    assert root_body["ok"] is True
    assert "webhook" in root_body["projects"]
    assert "test" in root_body["projects"]

    domain_resp = client.get("/api/folder-options?source=dag&project=webhook")
    assert domain_resp.status_code == 200
    assert domain_resp.json()["domains"] == ["whk"]

    level_resp = client.get("/api/folder-options?source=dag&project=webhook&domain=whk")
    assert level_resp.status_code == 200
    assert level_resp.json()["levels"] == ["level1"]

    flow_resp = client.get(
        "/api/folder-options?source=dag&project=webhook&domain=whk&level=level1"
    )
    assert flow_resp.status_code == 200
    assert flow_resp.json()["flows"] == ["src_to_stg"]


def test_create_dag_writes_files(client, studio_paths):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["ok"] is True
    assert "task_group_id" in body
    assert (
        body["task_group_id"] == "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"
    )
    flow = Path(body["flow_dir"])
    assert flow.as_posix().endswith("/projects/webhook/whk/level1/src_to_stg")
    assert (flow / ss.STUDIO_METADATA_NAME).is_file()
    meta = json.loads((flow / ss.STUDIO_METADATA_NAME).read_text(encoding="utf-8"))
    assert "user_tags" in meta
    assert "auto_tags" in meta
    dag_py = Path(body["dag_path"])
    yaml_name = "webhook_whk_level1_src_to_stg_1.yaml"
    assert (flow / yaml_name).is_file()
    assert dag_py.as_posix().endswith(
        "/dags/webhook/whk/level1/src_to_stg/webhook_whk_level1_src_to_stg_1_dag.py"
    )
    assert dag_py.is_file()
    dag_source = dag_py.read_text(encoding="utf-8")
    assert ss.STUDIO_DAG_MARKER in dag_source
    assert (
        "from ffengine.airflow.generated_factory import build_generated_dag"
        in dag_source
    )
    assert "RAW_CONFIG = {" in dag_source
    assert "yaml.safe_load(" not in dag_source
    assert "CONFIG_PATH.read_text(" not in dag_source
    assert yaml_name in dag_source


@pytest.mark.parametrize("field", ["project", "domain", "level", "flow"])
@pytest.mark.parametrize("value", ["", "   "])
def test_create_dag_rejects_unselected_folder_path(client, field, value):
    payload = _minimal_table_payload()
    payload[field] = value

    response = client.post("/api/create-dag", json=payload)

    assert response.status_code == 422
    assert "Select a project and DAG path" in response.text


@pytest.mark.parametrize("field", ["project", "domain", "level", "flow"])
def test_service_rejects_unselected_folder_path_without_writes(
    studio_paths, field
):
    projects_root, dag_root = studio_paths
    payload = _minimal_table_payload()
    payload[field] = "   "

    with pytest.raises(ValueError, match="Select a project and DAG path"):
        ss.create_or_update_dag(payload)

    assert list(projects_root.iterdir()) == []
    assert list(dag_root.iterdir()) == []


def test_create_dag_response_includes_revision_metadata(client, studio_paths):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["dag_id"] == Path(body["dag_path"]).stem
    assert isinstance(body["revision_count"], int)
    assert body["revision_count"] >= 1
    assert str(body.get("active_revision_id") or "").startswith("rev_")


def test_create_dag_writes_yaml_with_supported_fields(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "view",
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id", "amount"]),
            "where": "id > 10",
            "batch_size": 20000,
            "partitioning_enabled": True,
            "partitioning_mode": "explicit",
            "partitioning_column": None,
            "partitioning_parts": 4,
            "partitioning_distinct_limit": 24,
            "partitioning_ranges": ["id < 100", "id >= 100"],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]

    assert task["source_type"] == "view"
    assert task["column_mapping_mode"] == "mapping_file"
    assert (
        task["task_group_id"] == "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"
    )
    assert (
        task["mapping_file"]
        == "mapping/1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    assert (flow / task["mapping_file"]).is_file()
    assert task["batch_size"] == 20000
    assert task["partitioning"]["enabled"] is True
    assert task["partitioning"]["mode"] == "explicit"
    assert task["partitioning"]["column"] is None
    assert task["partitioning"]["parts"] == 4
    assert task["partitioning"]["distinct_limit"] == 24
    assert task["partitioning"]["ranges"] == ["id < 100", "id >= 100"]


def test_create_dag_persists_upsert_match_columns_and_roundtrips(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "load_method": "upsert",
            "upsert_match_columns": [" id ", "id", "name"],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["load_method"] == "upsert"
    assert task["upsert_match_columns"] == ["id", "name"]

    dag_id = Path(r.json()["dag_path"]).stem
    cfg_resp = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert cfg_resp.status_code == 200, cfg_resp.text
    task_preload = (cfg_resp.json().get("payload") or {}).get("flow_tasks", [])[0]
    assert task_preload["upsert_match_columns"] == ["id", "name"]


def test_create_dag_persists_notifications_and_roundtrips(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure", "success"],
        "notify_emails": ["ops@bank.example", "dev@bank.example"],
        "notify_conn_id": "smtp_default",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert cfg["notifications"] == {
        "notify_on": ["failure", "success"],
        "notify_emails": ["ops@bank.example", "dev@bank.example"],
        "notify_conn_id": "smtp_default",
    }

    dag_id = Path(r.json()["dag_path"]).stem
    cfg_resp = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert cfg_resp.status_code == 200, cfg_resp.text
    reloaded = (cfg_resp.json().get("payload") or {}).get("notifications")
    assert reloaded == {
        "notify_on": ["failure", "success"],
        "notify_emails": ["ops@bank.example", "dev@bank.example"],
        "notify_conn_id": "smtp_default",
    }


def test_create_dag_without_notifications_omits_block(client, studio_paths):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    # backward-compatible: no notifications key when unused
    assert "notifications" not in cfg


# ---------------------------------------------------------------------------
# F1.4/F1.5 — file source (csv/json) + file target round-trip
# ---------------------------------------------------------------------------


def _read_first_task(response):
    flow = Path(response.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    return cfg["flow_tasks"][0]


def test_create_dag_persists_csv_file_source(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "csv",
            "file_path": "/incoming/orders_{{ run_date }}.csv",
            "delimiter": ";",
            "encoding": "utf-8",
            "mapping_content": _sql_mapping_yaml(["id", "name"]),
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    task = _read_first_task(r)
    assert task["source_type"] == "csv"
    assert task["file_path"] == "/incoming/orders_{{ run_date }}.csv"
    assert task["delimiter"] == ";"
    # file sources are always explicit-mapping (no type inference)
    assert task["column_mapping_mode"] == "mapping_file"
    assert not task.get("source_table")

    # preload (edit) must round-trip the file fields so the UI can rehydrate.
    dag_id = Path(r.json()["dag_path"]).stem
    reloaded = client.get(f"/api/dag-config?dag_id={dag_id}").json()["payload"]
    rt = reloaded["flow_tasks"][0]
    assert rt["source_type"] == "csv"
    assert rt["file_path"] == "/incoming/orders_{{ run_date }}.csv"
    assert rt["delimiter"] == ";"


def test_create_dag_persists_json_flat_file_source(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "json",
            "file_path": "/incoming/orders.jsonl",
            "json_mode": "flat",
            "mapping_content": _sql_mapping_yaml(["id", "name"]),
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    task = _read_first_task(r)
    assert task["source_type"] == "json"
    assert task["json_mode"] == "flat"


def test_create_dag_persists_file_target(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "target_type": "file",
            "target_file_path": "/out/orders.csv",
            "target_delimiter": "|",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    task = _read_first_task(r)
    assert task["target_type"] == "file"
    assert task["target_file_path"] == "/out/orders.csv"
    assert task["target_delimiter"] == "|"


def test_create_dag_rejects_csv_without_file_path(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {"source_type": "csv", "mapping_content": _sql_mapping_yaml(["id"])}
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422, r.text
    assert "file_path" in r.text


def test_create_dag_rejects_json_raw_mode(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "json",
            "file_path": "/x.jsonl",
            "json_mode": "raw",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422, r.text
    assert "json_mode" in r.text or "raw" in r.text


def test_create_dag_db_to_db_omits_file_keys(client, studio_paths):
    # backward-compat: a plain DB→DB task carries no file keys.
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    task = _read_first_task(r)
    assert "file_path" not in task
    assert task.get("target_type", "db") != "file"


def test_dag_explorer_html_restyled_with_search(client):
    r = client.get("/dag-explorer")
    assert r.status_code == 200, r.text
    html = r.text
    # adopts the Flow Studio design system + carries the new search box, while
    # keeping the JS-owned ids intact.
    assert "flow_studio/css/style.css" in html
    assert 'id="dag_search"' in html
    assert 'id="tree_container"' in html
    assert 'id="list_container"' in html


def test_dag_search_matches_config_content(client, studio_paths, monkeypatch):
    # DAG A — csv source carrying a distinctive keyword in its config.yaml.
    a = _minimal_table_payload()
    a.update(
        {
            "source_type": "csv",
            "file_path": "/incoming/zztophit_orders.csv",
            "mapping_content": _sql_mapping_yaml(["id", "name"]),
        }
    )
    ra = client.post("/api/create-dag", json=a)
    assert ra.status_code == 201, ra.text
    a_path = ra.json()["dag_path"]
    a_id = Path(a_path).stem

    # DAG B — plain table source in a different flow (distinct files/id).
    b = _minimal_table_payload()
    b.update({"flow": "stg_to_dwh", "source_table": "plainaudit"})
    rb = client.post("/api/create-dag", json=b)
    assert rb.status_code == 201, rb.text
    b_path = rb.json()["dag_path"]
    b_id = Path(b_path).stem

    a_item = {"dag_id": a_id, "bucket": "dags_root", "fileloc": a_path}
    b_item = {"dag_id": b_id, "bucket": "dags_root", "fileloc": b_path}

    # _dag_searchable_text pulls the .py + config.yaml → contains the csv path.
    assert "zztophit_orders" in ss._dag_searchable_text(a_item).lower()

    monkeypatch.setattr(
        ss,
        "discover_dag_explorer_items",
        lambda: {"root": "/opt/airflow/dags", "items": [a_item, b_item], "count": 2},
    )

    # case-insensitive content match → only DAG A.
    r = client.get("/api/dag-search?q=ZZTOPHIT")
    assert r.status_code == 200, r.text
    assert [it["dag_id"] for it in r.json()["items"]] == [a_id]

    # empty query → all.
    r = client.get("/api/dag-search?q=")
    assert {it["dag_id"] for it in r.json()["items"]} == {a_id, b_id}

    # dag_id substring match works too.
    r = client.get(f"/api/dag-search?q={b_id[:6]}")
    assert b_id in [it["dag_id"] for it in r.json()["items"]]


def test_file_source_mapping_preview_detects_columns(tmp_path, monkeypatch):
    src = tmp_path / "orders.csv"
    src.write_text("id;name\n1;alice\n2;bob\n", encoding="utf-8")
    monkeypatch.setattr(
        ss.AirflowConnectionAdapter,
        "get_connection_params",
        staticmethod(lambda conn_id: {"conn_type": "fs"}),
    )
    out = ss.generate_mapping_preview(
        {
            "source_type": "csv",
            "source_conn_id": "fs_default",
            "target_conn_id": "tgt_c",
            "file_path": str(src),
            "delimiter": ";",
        }
    )
    names = [c["source_name"] for c in out["columns"]]
    assert names == ["id", "name"]
    assert out["sample_rows"][0] == ["1", "alice"]
    assert all(c["target_type"] == "varchar(255)" for c in out["columns"])


def test_create_dag_rejects_invalid_notification_email(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure"],
        "notify_emails": ["not-an-email"],
        "notify_conn_id": "smtp_default",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "Invalid email address" in r.text


# ---- F1.3b mail templates ------------------------------------------------

def test_mail_templates_list_returns_names_and_placeholders(client):
    fake = {
        "Default": {"subject": "s", "html_body": "b"},
        "Banka": {"subject": "s2", "html_body": "b2"},
    }
    with patch.object(api_app_module, "load_templates", return_value=fake):
        r = client.get("/api/mail-templates")
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert "Default" in data["names"] and "Banka" in data["names"]
    assert data["default_name"] == "Default"
    assert any(p["name"] == "status" for p in data["placeholders"])


def test_mail_template_save_ok(client):
    with patch.object(
        api_app_module, "save_template",
        return_value={"subject": "S", "html_body": "B"},
    ) as saver:
        r = client.post(
            "/api/mail-templates",
            json={"name": "Banka", "subject": "S", "html_body": "B"},
        )
    assert r.status_code == 200, r.text
    saver.assert_called_once()
    assert r.json()["name"] == "Banka"


def test_mail_template_save_validation_422(client):
    with patch.object(
        api_app_module, "save_template",
        side_effect=ValueError("subject cannot be empty"),
    ):
        r = client.post(
            "/api/mail-templates",
            json={"name": "X", "subject": "", "html_body": "B"},
        )
    assert r.status_code == 422
    assert "subject cannot be empty" in r.text


def test_mail_template_delete_ok(client):
    with patch.object(api_app_module, "delete_template") as deleter:
        r = client.post("/api/mail-templates/delete", json={"name": "Banka"})
    assert r.status_code == 200, r.text
    deleter.assert_called_once()


def test_mail_template_preview_renders_status_first(client):
    body = {
        "subject": "{{status}} DAG {{dag_id}}",
        "html_body": "<p>{{status}} {{rows}}</p>",
        "kind": "failure",
    }
    r = client.post("/api/mail-templates/preview", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["subject"].startswith("FAILED")
    assert "no data rows" in data["html"].lower()


def test_mail_templates_page_served(client):
    r = client.get("/mail-templates")
    assert r.status_code == 200
    assert "Mail Templates" in r.text
    assert "/api/mail-templates" in r.text


def test_plugin_registers_admin_mail_templates_view():
    from ffengine.ui.plugin import FlowStudioPlugin

    entry = next(
        (v for v in FlowStudioPlugin.external_views if v.get("url_route") == "mail_templates"),
        None,
    )
    assert entry is not None
    assert entry["category"] == "admin"
    assert entry["href"] == "/flow-studio/mail-templates"


def test_create_dag_persists_notify_template(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure"],
        "notify_emails": ["ops@bank.example"],
        "notify_conn_id": "smtp_default",
        "notify_template": "Banka-Kritik",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert cfg["notifications"]["notify_template"] == "Banka-Kritik"


def test_create_dag_omits_default_notify_template(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure"],
        "notify_emails": ["ops@bank.example"],
        "notify_conn_id": "smtp_default",
        "notify_template": "Default",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert "notify_template" not in cfg["notifications"]


def test_create_dag_persists_deadline_minutes_roundtrips(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure", "deadline"],
        "notify_emails": ["ops@bank.example"],
        "notify_conn_id": "smtp_default",
        "notify_deadline_minutes": 45,
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert cfg["notifications"]["notify_on"] == ["failure", "deadline"]
    assert cfg["notifications"]["notify_deadline_minutes"] == 45

    dag_id = Path(r.json()["dag_path"]).stem
    reloaded = (
        client.get(f"/api/dag-config?dag_id={dag_id}").json().get("payload") or {}
    ).get("notifications")
    assert reloaded["notify_deadline_minutes"] == 45


def test_create_dag_rejects_deadline_without_minutes(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["deadline"],
        "notify_emails": ["ops@bank.example"],
        "notify_conn_id": "smtp_default",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "notify_deadline_minutes must be a positive" in r.text


def test_create_dag_omits_deadline_minutes_when_not_selected(client, studio_paths):
    payload = _minimal_table_payload()
    payload["notifications"] = {
        "notify_on": ["failure"],
        "notify_emails": ["ops@bank.example"],
        "notify_conn_id": "smtp_default",
        "notify_deadline_minutes": 30,
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert "notify_deadline_minutes" not in cfg["notifications"]


def test_create_dag_rejects_upsert_without_match_columns(client, studio_paths):
    payload = _minimal_table_payload()
    payload["load_method"] = "upsert"
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "upsert_match_columns" in r.text


def test_create_dag_mapping_file_requires_content_when_file_missing(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload.update(
        {
            "column_mapping_mode": "mapping_file",
            "mapping_content": "",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert (
        "mapping_content is required when column_mapping_mode='mapping_file'" in r.text
    )


def test_create_dag_rejects_invalid_mapping_content_and_does_not_write_dag(
    client, studio_paths
):
    _, dag_root = studio_paths
    payload = _minimal_table_payload()
    payload.update(
        {
            "column_mapping_mode": "mapping_file",
            "mapping_content": "version: v1\ncolumns:\n  - source_name: id\n"
            "    target_name: id\n    source_type: TEXT\n    target_type: TEXT\n"
            "    nullable: [\n",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "Invalid mapping YAML" in r.text
    assert list(Path(dag_root).rglob("*_dag.py")) == []


def test_create_dag_distinct_mode_persists_distinct_limit(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "partitioning_enabled": True,
            "partitioning_mode": "distinct",
            "partitioning_column": "country_code",
            "partitioning_parts": 3,
            "partitioning_distinct_limit": 9,
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["partitioning"]["mode"] == "distinct"
    assert task["partitioning"]["column"] == "country_code"
    assert task["partitioning"]["distinct_limit"] == 9


def test_update_dag_rejects_invalid_existing_mapping_file_and_keeps_bundle(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload.update(
        {
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text

    dag_path = Path(r1.json()["dag_path"])
    cfg_path = Path(r1.json()["config_path"])
    flow_dir = Path(r1.json()["flow_dir"])
    mapping_path = (
        flow_dir
        / "mapping"
        / "1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    assert mapping_path.is_file()

    dag_before = dag_path.read_text(encoding="utf-8")
    cfg_before = cfg_path.read_text(encoding="utf-8")
    mapping_path.write_text(
        "version: v1\ncolumns:\n  - source_name: id\n    target_name: id\n    source_type: TEXT\n    target_type: TEXT\n    nullable: [\n",
        encoding="utf-8",
    )

    upd = _minimal_table_payload()
    upd.update(
        {
            "column_mapping_mode": "mapping_file",
            "mapping_content": "",
            "load_method": "append",
        }
    )
    dag_id = dag_path.stem
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=upd)
    assert r2.status_code == 422
    assert "Invalid mapping YAML" in r2.text

    assert dag_path.read_text(encoding="utf-8") == dag_before
    assert cfg_path.read_text(encoding="utf-8") == cfg_before
    cfg_after = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    assert cfg_after["flow_tasks"][0]["load_method"] == "append"


def test_create_dag_rejects_full_scan_partitioning_mode(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "partitioning_enabled": True,
            "partitioning_mode": "full_scan",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "Invalid partitioning.mode" in r.text


def test_create_dag_sql_source_persists_inline_sql(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id, amount FROM public.orders WHERE amount > 0",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id", "amount"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[
            {"name": "id", "source_type": "INTEGER"},
            {"name": "amount", "source_type": "NUMERIC"},
        ],
    ):
        r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]

    assert task["source_type"] == "sql"
    assert task["inline_sql"] == "SELECT id, amount FROM public.orders WHERE amount > 0"
    assert task["task_group_id"] == "1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg"
    assert (
        task["mapping_file"]
        == "mapping/1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    assert (
        flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    ).is_file()


def test_create_dag_sql_source_requires_inline_sql(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "   ",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "inline_sql" in r.text


def test_create_dag_sql_source_rejects_source_mapping_mode(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id FROM public.orders",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "source",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "column_mapping_mode='mapping_file'" in r.text


def test_create_dag_sql_source_rejects_column_count_mismatch(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id, amount FROM public.orders",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[
            {"name": "id", "source_type": "INTEGER"},
            {"name": "amount", "source_type": "NUMERIC"},
        ],
    ):
        r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "SQL select columns are incompatible with mapping" in r.text


def test_create_dag_sql_source_rejects_column_order_mismatch(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id, amount FROM public.orders",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["amount", "id"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[
            {"name": "id", "source_type": "INTEGER"},
            {"name": "amount", "source_type": "NUMERIC"},
        ],
    ):
        r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "SQL select columns are incompatible with mapping" in r.text


def test_create_dag_with_bindings_persists_yaml(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "updated_at >= {{ last_sync }}",
            "bindings": [
                {
                    "variable_name": "last_sync",
                    "binding_source": "airflow_variable",
                    "airflow_variable_key": "etl.last_sync",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["where"] == "updated_at >= {{ last_sync }}"
    assert task["bindings"][0]["variable_name"] == "last_sync"
    assert task["bindings"][0]["binding_source"] == "airflow_variable"
    assert task["bindings"][0]["airflow_variable_key"] == "etl.last_sync"


def test_create_dag_persists_dag_params_and_binding_task(client, studio_paths):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {
            "name": "log_level",
            "type": "string",
            "default": "default",
            "enum": ["default", "DEBUG"],
        },
        {"name": "run_date", "type": "string", "description": "Run date"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_run_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "target",
                    "sql": "SELECT MAX(business_date) FROM control.calendar",
                }
            ],
            "depends_on": [],
        },
        {
            "task_type": "source_target",
            "task_group_id": "load_orders",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_run_date"],
        },
    ]

    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 201, response.text
    flow = Path(response.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert cfg["dag_params"][1]["name"] == "run_date"
    assert "default" not in cfg["dag_params"][1]
    assert "required" not in cfg["dag_params"][1]
    assert cfg["flow_tasks"][0]["task_type"] == "binding"
    assert cfg["flow_tasks"][0]["bindings"][0]["binding_source"] == "target"


def test_create_dag_rejects_binding_target_not_declared_as_dag_param(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"}
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_run_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
            "depends_on": [],
        }
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 422
    assert "declared DAG parameter" in response.text


def test_create_dag_rejects_builtin_log_level_binding_target(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"}
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_log_level",
            "bindings": [{
                "variable_name": "log_level",
                "binding_source": "default",
                "default_value": "DEBUG",
            }],
            "depends_on": [],
        }
    ]

    response = client.post("/api/create-dag", json=payload)

    assert response.status_code == 422
    assert "Built-in DAG parameter 'log_level' cannot be assigned" in response.text


def test_service_rejects_builtin_log_level_binding_without_writes(studio_paths):
    projects_root, dag_root = studio_paths
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"}
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_log_level",
            "bindings": [{
                "variable_name": "log_level",
                "binding_source": "default",
                "default_value": "DEBUG",
            }],
            "depends_on": [],
        }
    ]

    with pytest.raises(
        ValueError,
        match="Built-in DAG parameter 'log_level' cannot be assigned",
    ):
        ss.create_or_update_dag(payload)

    assert list(projects_root.iterdir()) == []
    assert list(dag_root.iterdir()) == []


def test_create_dag_rejects_custom_param_default_and_required(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {
            "name": "run_date",
            "type": "string",
            "default": "2026-01-01",
            "required": True,
        },
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 422
    assert "custom DAG parameters do not support default or required" in response.text


def test_create_dag_allows_custom_param_with_trigger_value_only(client, studio_paths):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 201, response.text


def test_create_dag_allows_parameter_reassignment_across_binding_tasks(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_initial_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
            "depends_on": [],
        },
        {
            "task_type": "binding",
            "task_group_id": "bind_updated_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-02",
                }
            ],
            "depends_on": ["bind_initial_date"],
        },
        {
            "task_type": "source_target",
            "task_group_id": "load_orders",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_updated_date"],
        },
    ]

    response = client.post("/api/create-dag", json=payload)

    assert response.status_code == 201, response.text
    flow = Path(response.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(
            encoding="utf-8"
        )
    )
    assignments = [
        task["bindings"][0]["default_value"]
        for task in cfg["flow_tasks"]
        if task["task_type"] == "binding"
    ]
    assert assignments == ["2026-01-01", "2026-01-02"]


def test_create_dag_rejects_ambiguous_parameter_sources_at_branch_merge(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_date_a",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
            "depends_on": [],
        },
        {
            "task_type": "binding",
            "task_group_id": "bind_date_b",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-02",
                }
            ],
            "depends_on": [],
        },
        {
            "task_type": "source_target",
            "task_group_id": "load_orders",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_date_a", "bind_date_b"],
        },
    ]

    response = client.post("/api/create-dag", json=payload)

    assert response.status_code == 422
    assert "Ambiguous DAG parameter source" in response.text


def test_create_dag_rejects_ambiguous_final_graph_with_auto_task_id(
    client, studio_paths
):
    projects_root, dag_root = studio_paths

    response = client.post(
        "/api/create-dag",
        json=_auto_id_ambiguous_param_payload(),
    )

    assert response.status_code == 422
    assert "Ambiguous DAG parameter source" in response.text
    assert list(projects_root.rglob("*.yaml")) == []
    assert list(dag_root.rglob("*.py")) == []
    assert list(projects_root.rglob(".flow_studio_history")) == []


def test_service_rejects_ambiguous_final_auto_id_graph_before_writes(
    studio_paths,
):
    projects_root, dag_root = studio_paths

    with pytest.raises(ValueError, match="Ambiguous DAG parameter source"):
        ss.create_or_update_dag(_auto_id_ambiguous_param_payload())

    assert list(projects_root.rglob("*.yaml")) == []
    assert list(dag_root.rglob("*.py")) == []
    assert list(projects_root.rglob(".flow_studio_history")) == []


def test_create_dag_allows_transitive_binding_parameter_flow(client, studio_paths):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_run_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
            "depends_on": [],
        },
        {
            "task_type": "source_target",
            "task_group_id": "bridge",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_bridge",
            "depends_on": ["bind_run_date"],
        },
        {
            "task_type": "source_target",
            "task_group_id": "load_orders",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bridge"],
        },
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 201, response.text


def test_create_dag_rejects_airflow_variable_source_for_binding_task(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_run_date",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "airflow_variable",
                    "airflow_variable_key": "etl.run_date",
                }
            ],
            "depends_on": [],
        }
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 422
    assert "Binding tasks support source, target, or default" in response.text


def test_create_dag_rejects_binding_default_that_does_not_match_param_type(client):
    payload = _minimal_table_payload()
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "batch_limit", "type": "integer"},
    ]
    payload["flow_tasks"] = [
        {
            "task_type": "binding",
            "task_group_id": "bind_limit",
            "bindings": [
                {
                    "variable_name": "batch_limit",
                    "binding_source": "default",
                    "default_value": "not-an-integer",
                }
            ],
            "depends_on": [],
        }
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 422
    assert "does not match DAG parameter type" in response.text


def test_create_dag_rejects_obsolete_colon_parameter(client):
    payload = _minimal_table_payload()
    payload["where"] = "updated_at >= :last_sync"
    payload["bindings"] = [
        {
            "variable_name": "last_sync",
            "binding_source": "default",
            "default_value": "2026-01-01",
        }
    ]
    response = client.post("/api/create-dag", json=payload)
    assert response.status_code == 422
    assert "replace :last_sync with {{ last_sync }}" in response.text


def test_create_dag_rejects_missing_binding_for_where_param(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "updated_at >= {{ last_sync }}",
            "bindings": [],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "declared DAG parameter" in r.text


def test_create_dag_rejects_unused_binding(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "id > 10",
            "bindings": [
                {
                    "variable_name": "unused_param",
                    "binding_source": "default",
                    "default_value": "1",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "unused" in r.text


def test_create_dag_script_run_with_bindings_persists_yaml(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "task_type": "script_run",
            "script_run_environment": "target",
            "script_sql": "DELETE FROM dwh.orders_stg WHERE updated_at >= {{ last_sync }}",
            "bindings": [
                {
                    "variable_name": "last_sync",
                    "binding_source": "airflow_variable",
                    "airflow_variable_key": "etl.last_sync",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["task_type"] == "script_run"
    assert (
        task["script_sql"]
        == "DELETE FROM dwh.orders_stg WHERE updated_at >= {{ last_sync }}"
    )
    assert task["bindings"][0]["variable_name"] == "last_sync"

    dag_source = Path(r.json()["dag_path"]).read_text(encoding="utf-8")
    assert (
        "from ffengine.airflow.generated_factory import build_generated_dag"
        in dag_source
    )
    assert "RAW_CONFIG = {" in dag_source
    assert "script_sql" in dag_source


def test_create_dag_script_run_rejects_missing_binding_for_script_param(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload.update(
        {
            "task_type": "script_run",
            "script_run_environment": "target",
            "script_sql": "DELETE FROM dwh.orders_stg WHERE updated_at >= {{ last_sync }}",
            "bindings": [],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "declared DAG parameter" in r.text


def test_create_dag_script_run_rejects_unused_binding(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "task_type": "script_run",
            "script_run_environment": "target",
            "script_sql": "DELETE FROM dwh.orders_stg",
            "bindings": [
                {
                    "variable_name": "unused_param",
                    "binding_source": "default",
                    "default_value": "1",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "unused" in r.text


def test_create_dag_rejects_removed_fields(client, studio_paths):
    payload = _minimal_table_payload()
    payload["reader_workers"] = 4
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


def test_create_dag_rejects_group_no_field(client, studio_paths):
    payload = _minimal_table_payload()
    payload["group_no"] = 3
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


def test_create_dag_rejects_removed_tags_field(client, studio_paths):
    p = _minimal_table_payload()
    p["tags"] = ["prod", "nightly"]
    r = client.post("/api/create-dag", json=p)
    assert r.status_code == 422


def test_create_dag_custom_tags_normalized_and_merged(client, studio_paths):
    payload = _minimal_table_payload()
    payload["custom_tags"] = [" Team-A ", "LEVEL1", "nightly", "team-a", "odd !! tag "]

    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    flow = Path(body["flow_dir"])

    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert cfg["custom_tags"] == ["team-a", "level1", "nightly", "odd_tag"]
    assert cfg["flow_tasks"][0]["tags"] == [
        "webhook",
        "whk",
        "level1",
        "src_to_stg",
        "team-a",
        "nightly",
        "odd_tag",
    ]

    meta = json.loads((flow / ss.STUDIO_METADATA_NAME).read_text(encoding="utf-8"))
    assert meta["auto_tags"] == ["webhook", "whk", "level1", "src_to_stg"]
    assert meta["user_tags"] == ["team-a", "level1", "nightly", "odd_tag"]
    assert meta["tags"] == [
        "webhook",
        "whk",
        "level1",
        "src_to_stg",
        "team-a",
        "nightly",
        "odd_tag",
    ]


def test_create_dag_rejects_custom_tags_count_over_limit(client, studio_paths):
    payload = _minimal_table_payload()
    payload["custom_tags"] = [f"t{i}" for i in range(1, 23)]
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "custom_tags" in r.text


def test_create_dag_rejects_custom_tag_length_over_limit(client, studio_paths):
    payload = _minimal_table_payload()
    payload["custom_tags"] = ["x" * 33]
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "length must be at most 32" in r.text


def test_create_dag_requires_new_hierarchy_fields(client, studio_paths):
    payload = _minimal_table_payload()
    payload.pop("flow")
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


def test_create_dag_rejects_invalid_group_no(client, studio_paths):
    payload = _minimal_table_payload()
    payload["group_no"] = 0
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


def test_dag_filename_fallback_when_flow_not_to_pattern(client, studio_paths):
    payload = _minimal_table_payload()
    payload["flow"] = "delta_sync"
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201
    dag_py = Path(r.json()["dag_path"])
    assert dag_py.name == "webhook_whk_level1_delta_sync_1_dag.py"


def test_create_dag_same_flow_creates_numbered_dags_and_yamls(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    assert r1.status_code == 201, r1.text
    r2 = client.post("/api/create-dag", json=p2)
    assert r2.status_code == 201, r2.text

    body1 = r1.json()
    body2 = r2.json()
    assert body1["dag_path"] != body2["dag_path"]

    flow = Path(body1["flow_dir"])
    assert (flow / "webhook_whk_level1_src_to_stg_1.yaml").is_file()
    assert (flow / "webhook_whk_level1_src_to_stg_2.yaml").is_file()
    assert Path(body1["dag_path"]).name == "webhook_whk_level1_src_to_stg_1_dag.py"
    assert Path(body2["dag_path"]).name == "webhook_whk_level1_src_to_stg_2_dag.py"


def test_update_dag_keeps_legacy_dag_id_and_path(client, studio_paths):
    projects_root, dag_root = studio_paths
    flow_dir = projects_root / "webhook" / "whk" / "level1" / "src_to_stg"
    flow_dir.mkdir(parents=True, exist_ok=True)
    config_path = flow_dir / "webhook_whk_level1_src_to_stg_group_9.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "source_db_var": "src_c",
                "target_db_var": "tgt_c",
                "flow_tasks": [
                    {
                        "task_group_id": "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg",
                        "depends_on": [],
                        "source_schema": "public",
                        "source_table": "orders",
                        "source_type": "table",
                        "column_mapping_mode": "source",
                        "target_schema": "dwh",
                        "target_table": "orders_stg",
                        "load_method": "append",
                        "batch_size": 10000,
                        "partitioning": {
                            "enabled": False,
                            "mode": "auto_numeric",
                            "column": None,
                            "parts": 2,
                            "distinct_limit": 16,
                            "ranges": [],
                        },
                        "tags": ["webhook", "whk", "level1", "src_to_stg"],
                    }
                ],
                "custom_tags": [],
                "scheduler": {
                    "cron_expression": None,
                    "timezone": "UTC",
                    "active": True,
                    "start_date": "2023-01-01T00:00:00",
                },
                "dag_dependencies": {"upstream_dag_ids": []},
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )

    legacy_dag_id = "whk_to_stg_level1_group_9_dag"
    legacy_dag_path = (
        dag_root / "webhook" / "whk" / "level1" / "src_to_stg" / f"{legacy_dag_id}.py"
    )
    legacy_dag_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_dag_path.write_text(
        "\n".join(
            [
                ss.STUDIO_DAG_MARKER,
                "from pathlib import Path",
                f'CONFIG_PATH = Path("{config_path.as_posix()}")',
                f'DAG_ID = "{legacy_dag_id}"',
            ]
        ),
        encoding="utf-8",
    )

    payload = _minimal_table_payload()
    payload["load_method"] = "replace"
    r = client.post(f"/api/update-dag?dag_id={legacy_dag_id}", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dag_id"] == legacy_dag_id
    assert body["dag_path"] == legacy_dag_path.as_posix()
    assert Path(body["dag_path"]).name == "whk_to_stg_level1_group_9_dag.py"


def test_create_dag_group_no_increments_with_mixed_legacy_and_new_dag_names(
    client, studio_paths
):
    projects_root, dag_root = studio_paths
    flow_dir = projects_root / "webhook" / "whk" / "level1" / "src_to_stg"
    flow_dir.mkdir(parents=True, exist_ok=True)
    legacy_dag_dir = dag_root / "webhook" / "whk" / "level1" / "src_to_stg"
    legacy_dag_dir.mkdir(parents=True, exist_ok=True)

    legacy_dag = legacy_dag_dir / "whk_to_stg_level1_group_3_dag.py"
    legacy_dag.write_text("# legacy dag\n", encoding="utf-8")
    new_style_dag = legacy_dag_dir / "webhook_whk_level1_src_to_stg_4_dag.py"
    new_style_dag.write_text("# new style dag\n", encoding="utf-8")

    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    dag_name = Path(r.json()["dag_path"]).name
    assert dag_name == "webhook_whk_level1_src_to_stg_5_dag.py"
    assert (flow_dir / "webhook_whk_level1_src_to_stg_5.yaml").is_file()


def test_update_dag_requires_dag_id_query(client, studio_paths):
    payload = _minimal_table_payload()
    r0 = client.post("/api/create-dag", json=payload)
    assert r0.status_code == 201
    r = client.post("/api/update-dag", json=payload)
    assert r.status_code == 422
    assert "dag_id" in r.text


def test_update_dag_requires_studio_marker(client, studio_paths):
    payload = _minimal_table_payload()
    r0 = client.post("/api/create-dag", json=payload)
    assert r0.status_code == 201
    dag_path = Path(r0.json()["dag_path"])
    dag_id = dag_path.stem
    dag_path.write_text("# manual dag\n", encoding="utf-8")
    r = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r.status_code == 422
    assert "Flow Studio" in r.json()["detail"]


def test_update_dag_ok(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201
    dag_path = r1.json()["dag_path"]
    dag_id = Path(dag_path).stem
    cfg_path = r1.json()["config_path"]
    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200
    assert r2.json()["dag_path"] == dag_path
    assert r2.json()["config_path"] == cfg_path


def test_update_dag_allows_adding_new_task(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_path = r1.json()["dag_path"]
    dag_id = Path(dag_path).stem
    cfg_path = Path(r1.json()["config_path"])

    update_payload = _minimal_table_payload()
    update_payload["flow_tasks"] = [
        {
            "source_type": "table",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dwh",
            "target_table": "orders_stg",
            "load_method": "append",
            "column_mapping_mode": "source",
        },
        {
            "source_type": "table",
            "source_schema": "public",
            "source_table": "customers",
            "target_schema": "dwh",
            "target_table": "customers_stg",
            "load_method": "append",
            "column_mapping_mode": "source",
            "depends_on": ["1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"],
        },
    ]
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=update_payload)
    assert r2.status_code == 200, r2.text
    assert r2.json()["dag_path"] == dag_path

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    tasks = cfg.get("flow_tasks") or []
    assert len(tasks) == 2
    assert tasks[0]["target_table"] == "orders_stg"
    assert tasks[1]["target_table"] == "customers_stg"
    assert tasks[0]["depends_on"] == []
    assert tasks[1]["depends_on"] == [
        "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"
    ]


def test_update_dag_targets_selected_dag_when_same_flow_has_multiple_groups(
    client, studio_paths
):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    assert dag1 != dag2

    p1["load_method"] = "replace"
    r_upd = client.post(f"/api/update-dag?dag_id={dag1}", json=p1)
    assert r_upd.status_code == 200, r_upd.text

    cfg1 = Path(r1.json()["config_path"])
    cfg2 = Path(r2.json()["config_path"])
    c1 = yaml.safe_load(cfg1.read_text(encoding="utf-8"))
    c2 = yaml.safe_load(cfg2.read_text(encoding="utf-8"))
    assert c1["flow_tasks"][0]["load_method"] == "replace"
    assert c2["flow_tasks"][0]["load_method"] == "append"


def test_dag_options_filters_scope_without_wait_previous_field(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"
    p3 = _minimal_table_payload()
    p3["domain"] = "other"
    p3["source_table"] = "products"
    p3["target_table"] = "products_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    r3 = client.post("/api/create-dag", json=p3)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text
    assert r3.status_code == 201, r3.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    dag3 = Path(r3.json()["dag_path"]).stem

    r_opts = client.get(
        f"/api/dag-options?project=webhook&domain=whk&level=level1&flow=src_to_stg&dag_id={dag2}"
    )
    assert r_opts.status_code == 200, r_opts.text
    body = r_opts.json()
    assert body["ok"] is True
    assert body["dag_id"] == dag2
    assert "wait_previous_dag_id" not in body
    option_ids = [str(item.get("dag_id") or "") for item in body.get("items", [])]
    assert dag1 in option_ids
    assert dag2 not in option_ids
    assert dag3 in option_ids


def test_dag_dependencies_accept_cross_domain_in_same_project(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["domain"] = "other"
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    cfg2_path = Path(r2.json()["config_path"])

    upd2 = _minimal_table_payload()
    upd2["domain"] = "other"
    upd2["source_table"] = "customers"
    upd2["target_table"] = "customers_stg"
    upd2["dag_dependencies"] = {"upstream_dag_ids": [dag1]}
    r_upd2 = client.post(f"/api/update-dag?dag_id={dag2}", json=upd2)
    assert r_upd2.status_code == 200, r_upd2.text
    assert (r_upd2.json().get("dag_dependencies") or {}).get("upstream_dag_ids") == [
        dag1
    ]

    cfg2 = yaml.safe_load(cfg2_path.read_text(encoding="utf-8"))
    assert (cfg2.get("dag_dependencies") or {}).get("upstream_dag_ids") == [dag1]


def test_create_update_roundtrip_dag_dependencies(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    dag1_path = Path(r1.json()["dag_path"])
    cfg2_path = Path(r2.json()["config_path"])
    dag1_source_before = dag1_path.read_text(encoding="utf-8")

    update_payload = _minimal_table_payload()
    update_payload["source_table"] = "customers"
    update_payload["target_table"] = "customers_stg"
    update_payload["dag_dependencies"] = {"upstream_dag_ids": [dag1]}

    r_upd = client.post(f"/api/update-dag?dag_id={dag2}", json=update_payload)
    assert r_upd.status_code == 200, r_upd.text
    assert (r_upd.json().get("dag_dependencies") or {}).get("upstream_dag_ids") == [
        dag1
    ]

    cfg2 = yaml.safe_load(cfg2_path.read_text(encoding="utf-8"))
    assert (cfg2.get("dag_dependencies") or {}).get("upstream_dag_ids") == [dag1]

    dag2_source = Path(r2.json()["dag_path"]).read_text(encoding="utf-8")
    assert (
        "from ffengine.airflow.generated_factory import build_generated_dag"
        in dag2_source
    )
    assert "RAW_CONFIG = {" in dag2_source
    assert "raw_config_snapshot=RAW_CONFIG" in dag2_source
    assert "yaml.safe_load(" not in dag2_source
    assert "CONFIG_PATH.read_text(" not in dag2_source
    assert f'UPSTREAM_DAG_IDS = ["{dag1}"]' in dag2_source

    dag1_source = dag1_path.read_text(encoding="utf-8")
    assert "DOWNSTREAM_DAG_IDS" not in dag1_source
    assert "trigger_downstream__" not in dag1_source
    assert "trigger_dag_id=downstream_dag_id" not in dag1_source
    assert dag1_source == dag1_source_before

    r_cfg = client.get(f"/api/dag-config?dag_id={dag2}")
    assert r_cfg.status_code == 200, r_cfg.text
    payload = r_cfg.json().get("payload") or {}
    assert (payload.get("dag_dependencies") or {}).get("upstream_dag_ids") == [dag1]


def test_dependency_render_contains_multi_upstream_join_logic(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"
    p3 = _minimal_table_payload()
    p3["source_table"] = "orders"
    p3["target_table"] = "orders_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    r3 = client.post("/api/create-dag", json=p3)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text
    assert r3.status_code == 201, r3.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    dag3 = Path(r3.json()["dag_path"]).stem

    upd3 = _minimal_table_payload()
    upd3["source_table"] = "orders"
    upd3["target_table"] = "orders_stg"
    upd3["dag_dependencies"] = {"upstream_dag_ids": [dag1, dag2]}
    r_upd3 = client.post(f"/api/update-dag?dag_id={dag3}", json=upd3)
    assert r_upd3.status_code == 200, r_upd3.text

    dag3_source = Path(r3.json()["dag_path"]).read_text(encoding="utf-8")
    assert f'UPSTREAM_DAG_IDS = ["{dag1}", "{dag2}"]' in dag3_source
    assert "raw_config_snapshot=RAW_CONFIG" in dag3_source


def test_create_dag_rejects_unknown_dag_dependency(client, studio_paths):
    payload = _minimal_table_payload()
    payload["dag_dependencies"] = {"upstream_dag_ids": ["missing_dag_id"]}
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "dag_dependencies contains invalid dag_id" in r.text


def test_dag_dependency_cycle_rejected(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem

    upd2 = _minimal_table_payload()
    upd2["source_table"] = "customers"
    upd2["target_table"] = "customers_stg"
    upd2["dag_dependencies"] = {"upstream_dag_ids": [dag1]}
    r_upd2 = client.post(f"/api/update-dag?dag_id={dag2}", json=upd2)
    assert r_upd2.status_code == 200, r_upd2.text

    upd1 = _minimal_table_payload()
    upd1["dag_dependencies"] = {"upstream_dag_ids": [dag2]}
    r_upd1 = client.post(f"/api/update-dag?dag_id={dag1}", json=upd1)
    assert r_upd1.status_code == 422
    assert "cycle" in r_upd1.text.lower()


def test_delete_dag_requires_cleanup_references_and_then_cleans(client, studio_paths):
    p1 = _minimal_table_payload()
    p2 = _minimal_table_payload()
    p2["domain"] = "other"
    p2["source_table"] = "customers"
    p2["target_table"] = "customers_stg"

    r1 = client.post("/api/create-dag", json=p1)
    r2 = client.post("/api/create-dag", json=p2)
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text

    dag1 = Path(r1.json()["dag_path"]).stem
    dag2 = Path(r2.json()["dag_path"]).stem
    dag2_path = Path(r2.json()["dag_path"])
    cfg2_path = Path(r2.json()["config_path"])

    upd2 = _minimal_table_payload()
    upd2["domain"] = "other"
    upd2["source_table"] = "customers"
    upd2["target_table"] = "customers_stg"
    upd2["dag_dependencies"] = {"upstream_dag_ids": [dag1]}
    r_upd2 = client.post(f"/api/update-dag?dag_id={dag2}", json=upd2)
    assert r_upd2.status_code == 200, r_upd2.text

    r_del_fail = client.delete(f"/api/delete-dag?dag_id={dag1}")
    assert r_del_fail.status_code == 422
    assert "cleanup_references=true" in r_del_fail.text

    r_del_ok = client.delete(f"/api/delete-dag?dag_id={dag1}&cleanup_references=true")
    assert r_del_ok.status_code == 200, r_del_ok.text
    body = r_del_ok.json()
    assert body["ok"] is True
    assert body["cleanup_references"] is True
    assert dag2 in (body.get("cleaned_reference_dags") or [])

    cfg2 = yaml.safe_load(cfg2_path.read_text(encoding="utf-8"))
    assert (cfg2.get("dag_dependencies") or {}).get("upstream_dag_ids") == []
    dag2_source = dag2_path.read_text(encoding="utf-8")
    assert "UPSTREAM_DAG_IDS = []" in dag2_source


def test_dag_revisions_promote_roundtrip(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    rev_items = r_rev.json()["items"]
    assert len(rev_items) >= 2
    revision_count_before = len(rev_items)
    create_revision = next(
        (x["revision_id"] for x in rev_items if x.get("source") == "create_initial"), ""
    )
    update_revision = next(
        (x["revision_id"] for x in rev_items if x.get("source") == "update"), ""
    )
    assert create_revision
    assert update_revision
    assert create_revision != update_revision

    r_promote_old = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
        json={},
    )
    assert r_promote_old.status_code == 200, r_promote_old.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "append"
    )

    r_rev_after = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev_after.status_code == 200, r_rev_after.text
    rev_items_after = r_rev_after.json()["items"]
    assert len(rev_items_after) == revision_count_before
    assert not any(
        str(x.get("source") or "") == "promote_before_switch" for x in rev_items_after
    )

    r_promote_new = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={update_revision}",
        json={},
    )
    assert r_promote_new.status_code == 200, r_promote_new.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )


def test_promote_regenerates_missing_revision_mapping_file(
    client, studio_paths, monkeypatch
):
    payload = _minimal_table_payload()
    payload["column_mapping_mode"] = "mapping_file"
    payload["mapping_content"] = _sql_mapping_yaml(["id"])

    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    flow_dir = Path(r1.json()["flow_dir"])

    payload["where"] = "id > 10"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    create_revision = next(
        (
            x["revision_id"]
            for x in r_rev.json()["items"]
            if x.get("source") == "create_initial"
        ),
        "",
    )
    assert create_revision

    history_root = flow_dir / ".flow_studio_history" / dag_id
    rev_dir = history_root / create_revision
    rev_cfg = yaml.safe_load((rev_dir / "config.yaml").read_text(encoding="utf-8"))
    mapping_rel = str(rev_cfg["flow_tasks"][0]["mapping_file"])
    missing_mapping_rel = mapping_rel.replace(".yaml", "_missing.yaml")
    rev_cfg["flow_tasks"][0]["mapping_file"] = missing_mapping_rel
    (rev_dir / "config.yaml").write_text(
        yaml.safe_dump(rev_cfg, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    regenerated_mapping = _sql_mapping_yaml(["id", "amount"])
    monkeypatch.setattr(
        ss,
        "_generate_mapping_content_for_task",
        lambda **_kwargs: regenerated_mapping,
    )

    r_promote = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
        json={},
    )
    assert r_promote.status_code == 200, r_promote.text
    active_mapping_path = flow_dir / missing_mapping_rel
    assert active_mapping_path.is_file()
    assert active_mapping_path.read_text(encoding="utf-8") == regenerated_mapping


def test_promote_rejects_invalid_revision_id_format(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    r = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id=bad_revision", json={}
    )
    assert r.status_code == 422
    assert "revision_id" in r.text


def test_promote_active_revision_returns_noop_success(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])
    cfg_before = cfg_path.read_text(encoding="utf-8")

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    active_revision = str(r_rev.json().get("active_revision_id") or "")
    assert active_revision

    r_promote = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={active_revision}",
        json={},
    )
    assert r_promote.status_code == 200, r_promote.text
    body = r_promote.json()
    assert body.get("active_revision_id") == active_revision
    assert body.get("promoted_revision_id") == active_revision
    assert body.get("no_op") is True
    assert cfg_path.read_text(encoding="utf-8") == cfg_before


def test_promote_returns_404_for_missing_revision(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    r = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id=rev_999999", json={}
    )
    assert r.status_code == 404


def test_delete_dag_requires_dag_id_query(client, studio_paths):
    r = client.delete("/api/delete-dag")
    assert r.status_code == 422


def test_delete_dag_removes_flow_studio_bundle_files(client, studio_paths):
    payload = _minimal_table_payload()
    payload["column_mapping_mode"] = "mapping_file"
    payload["mapping_content"] = _sql_mapping_yaml(["id"])

    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    body1 = r1.json()
    dag_path = Path(body1["dag_path"])
    cfg_path = Path(body1["config_path"])
    dag_id = dag_path.stem

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    mapping_rel = str(cfg["flow_tasks"][0].get("mapping_file") or "")
    assert mapping_rel
    mapping_path = cfg_path.parent / mapping_rel
    history_path = cfg_path.parent / ss.STUDIO_HISTORY_DIR_NAME / dag_id
    metadata_path = cfg_path.parent / ss.STUDIO_METADATA_NAME

    assert dag_path.is_file()
    assert cfg_path.is_file()
    assert mapping_path.is_file()
    assert history_path.is_dir()
    assert metadata_path.is_file()

    r2 = client.delete(f"/api/delete-dag?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    body2 = r2.json()
    assert body2["ok"] is True
    assert body2["dag_id"] == dag_id
    warnings = [str(x) for x in body2.get("warnings", [])]
    deleted_paths = [str(x) for x in body2.get("deleted_paths", [])]

    dag_deleted = any(Path(x).name == dag_path.name for x in deleted_paths)
    cfg_deleted = any(Path(x).name == cfg_path.name for x in deleted_paths)
    mapping_deleted = any(Path(x).name == mapping_path.name for x in deleted_paths)
    history_deleted = any(Path(x).name == history_path.name for x in deleted_paths)
    metadata_deleted = any(Path(x).name == metadata_path.name for x in deleted_paths)

    if dag_deleted:
        assert not dag_path.exists()
    else:
        assert any("DAG file could not be deleted" in w for w in warnings)
    if cfg_deleted:
        assert not cfg_path.exists()
    else:
        assert any("YAML file could not be deleted" in w for w in warnings)
    if mapping_deleted:
        assert not mapping_path.exists()
    else:
        assert any("Mapping file could not be deleted" in w for w in warnings)
    if history_path.exists() and not history_deleted:
        assert any("History directory could not be deleted" in w for w in warnings)
    if metadata_deleted:
        assert not metadata_path.exists()
    elif metadata_path.exists():
        assert any("Metadata file could not be deleted" in w for w in warnings)


def test_delete_dag_rejects_non_studio_marker_dag(client, studio_paths):
    _, gen = studio_paths
    dag_path = gen / "manual_non_studio_dag.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text("from airflow import DAG\n", encoding="utf-8")

    r = client.delete("/api/delete-dag?dag_id=manual_non_studio_dag")
    assert r.status_code == 422
    assert "Flow Studio" in r.text


def test_delete_dag_reports_airflow_cleanup_success(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem

    with patch.object(
        ss,
        "_cleanup_airflow_dag_metadata",
        return_value={"ok": True, "details": {"dag_models": 1}, "warnings": []},
    ):
        r2 = client.delete(f"/api/delete-dag?dag_id={dag_id}")

    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["ok"] is True
    assert body["airflow_cleanup"]["ok"] is True
    assert body["airflow_cleanup"]["details"]["dag_models"] == 1


def test_delete_dag_continues_when_airflow_cleanup_fails(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    dag_path = Path(r1.json()["dag_path"])
    cfg_path = Path(r1.json()["config_path"])

    with patch.object(
        ss,
        "_cleanup_airflow_dag_metadata",
        side_effect=RuntimeError("db cleanup failed"),
    ):
        r2 = client.delete(f"/api/delete-dag?dag_id={dag_id}")

    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["ok"] is True
    assert body["airflow_cleanup"]["ok"] is False
    assert any("cleanup exception" in str(x).lower() for x in body.get("warnings", []))
    if dag_path.exists():
        assert any(
            "DAG file could not be deleted" in str(x) for x in body.get("warnings", [])
        )
    if cfg_path.exists():
        assert any(
            "YAML file could not be deleted" in str(x) for x in body.get("warnings", [])
        )


def test_revision_retention_keeps_last_20_snapshots(client, studio_paths, monkeypatch):
    monkeypatch.setenv("FFENGINE_STUDIO_HISTORY_KEEP_LIMIT", "20")
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_path = Path(r1.json()["dag_path"])
    dag_id = dag_path.stem

    for i in range(1, 26):
        payload["target_table"] = f"orders_stg_{i}"
        payload["load_method"] = "replace" if i % 2 else "append"
        r_upd = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
        assert r_upd.status_code == 200, r_upd.text

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    assert r_rev.json()["count"] == 20
    assert len(list(dag_path.parent.glob("*_dag.py"))) == 1


def test_promote_rolls_back_when_parse_verification_fails(
    client, studio_paths, monkeypatch
):
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "1")
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    create_revision = next(
        (
            x["revision_id"]
            for x in r_rev.json()["items"]
            if x.get("source") == "create_initial"
        ),
        "",
    )
    assert create_revision

    with (
        patch.object(ss, "_wait_for_parse_refresh", return_value=False),
        patch.object(
            ss,
            "_active_bundle_hash_or_empty",
            return_value="hash_mismatch",
        ),
    ):
        r_promote = client.post(
            f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
            json={},
        )
    assert r_promote.status_code == 422
    assert "rolled back" in r_promote.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )


def test_promote_succeeds_when_parse_timeout_but_target_bundle_is_active(
    client, studio_paths, monkeypatch
):
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "1")
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])
    flow_dir = Path(r1.json()["flow_dir"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    create_revision = next(
        (
            x["revision_id"]
            for x in r_rev.json()["items"]
            if x.get("source") == "create_initial"
        ),
        "",
    )
    assert create_revision

    history_root = flow_dir / ".flow_studio_history" / dag_id
    create_manifest = json.loads(
        (history_root / create_revision / "manifest.json").read_text(encoding="utf-8")
    )
    create_bundle_hash = str((create_manifest.get("hashes") or {}).get("bundle") or "")
    assert create_bundle_hash

    with (
        patch.object(ss, "_wait_for_parse_refresh", return_value=False),
        patch.object(
            ss,
            "_active_bundle_hash_or_empty",
            return_value=create_bundle_hash,
        ),
    ):
        r_promote = client.post(
            f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
            json={},
        )

    assert r_promote.status_code == 200, r_promote.text
    body = r_promote.json()
    warnings = [str(x) for x in body.get("warnings", [])]
    assert any("parse refresh timeout" in w.lower() for w in warnings)
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "append"
    )


def test_promote_uses_recalculated_hash_when_manifest_hash_is_stale(
    client, studio_paths, monkeypatch
):
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "1")
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])
    flow_dir = Path(r1.json()["flow_dir"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "replace"
    )

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    create_revision = next(
        (
            x["revision_id"]
            for x in r_rev.json()["items"]
            if x.get("source") == "create_initial"
        ),
        "",
    )
    assert create_revision

    history_root = flow_dir / ".flow_studio_history" / dag_id
    rev_dir = history_root / create_revision
    manifest_path = rev_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("hashes", {})
    manifest["hashes"]["bundle"] = "stale_manifest_hash"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    rev_bundle = ss._load_bundle_from_revision(rev_dir)
    recalculated_hash = ss._bundle_hash_from_loaded_bundle(rev_bundle)
    assert recalculated_hash and recalculated_hash != "stale_manifest_hash"

    with (
        patch.object(ss, "_wait_for_parse_refresh", return_value=False),
        patch.object(
            ss,
            "_active_bundle_hash_or_empty",
            return_value=recalculated_hash,
        ),
    ):
        r_promote = client.post(
            f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
            json={},
        )

    assert r_promote.status_code == 200, r_promote.text
    body = r_promote.json()
    warnings = [str(x) for x in body.get("warnings", [])]
    assert any("manifest hash mismatch" in w.lower() for w in warnings)
    assert body.get("active_revision_id") == create_revision
    assert (
        yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0][
            "load_method"
        ]
        == "append"
    )


def test_wait_for_parse_refresh_timeout_from_env(monkeypatch):
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "1")
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_INTERVAL_SECONDS", "1")

    before_state = {
        "dag_version_id": "1",
        "version_number": 1,
        "dag_hash": "same",
        "serialized_last_updated": "t1",
    }

    def _run_with_timeout(timeout_value: str) -> bool:
        monkeypatch.setenv(
            "FFENGINE_STUDIO_PROMOTE_VERIFY_TIMEOUT_SECONDS", timeout_value
        )
        elapsed = {"v": 0.0}

        def _mono() -> float:
            return float(elapsed["v"])

        def _sleep(seconds: float) -> None:
            elapsed["v"] += float(seconds)

        def _parse_state(_dag_id: str) -> dict:
            # Simulate slow Airflow parse refresh: state changes only after 50s.
            if elapsed["v"] < 50.0:
                return dict(before_state)
            changed = dict(before_state)
            changed["dag_hash"] = "changed"
            return changed

        monkeypatch.setattr(ss.time, "monotonic", _mono)
        monkeypatch.setattr(ss.time, "sleep", _sleep)
        monkeypatch.setattr(ss, "_airflow_parse_state", _parse_state)
        return bool(ss._wait_for_parse_refresh("demo_dag", before_state))

    assert _run_with_timeout("35") is False
    assert _run_with_timeout("60") is True


def test_update_dag_rejects_dag_id_payload_flow_mismatch(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem

    payload["flow"] = "src_to_dwh"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 422
    assert "hierarchy do not match" in r2.text


def test_update_dag_rejects_full_scan_partitioning_mode(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201
    dag_id = Path(r1.json()["dag_path"]).stem
    payload["partitioning_enabled"] = True
    payload["partitioning_mode"] = "full_scan"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 422
    assert "Invalid partitioning.mode" in r2.text


def test_resolve_task_dependencies_depends_on_and_parallel_default():
    tasks = [
        {"task_group_id": "t1"},
        {"task_group_id": "t2", "depends_on": ["t1"]},
        {"task_group_id": "t3"},
    ]
    edges = ss.resolve_task_dependencies(tasks)
    assert ("t1", "t2") in edges
    assert ("t2", "t3") not in edges


def test_resolve_task_dependencies_rejects_self_dependency():
    tasks = [
        {"task_group_id": "t1", "depends_on": ["t1"]},
        {"task_group_id": "t2"},
    ]
    with pytest.raises(ValueError, match="cannot reference itself"):
        ss.resolve_task_dependencies(tasks)


def test_resolve_task_dependencies_invalid_upstream():
    tasks = [
        {"task_group_id": "t1"},
        {"task_group_id": "t2", "depends_on": ["missing"]},
    ]
    with pytest.raises(ValueError, match="depends_on contains invalid"):
        ss.resolve_task_dependencies(tasks)


def test_resolve_task_dependencies_cycle_error():
    tasks = [
        {"task_group_id": "t1", "depends_on": ["t2"]},
        {"task_group_id": "t2", "depends_on": ["t1"]},
    ]
    with pytest.raises(ValueError, match="cycle"):
        ss.resolve_task_dependencies(tasks)


def test_build_dag_explorer_items_root_and_external():
    rows = [
        (
            "dag_in",
            False,
            "/opt/airflow/dags/team/flow/dag_in.py",
            "alice, bob,alice",
            "2026-04-23T20:00:00+00:00",
            "2026-04-20T12:00:00+00:00",
        ),
        ("dag_out", True, "/tmp/dag_out.py", "", None, None),
    ]
    items = ss._build_dag_explorer_items(rows, Path("/opt/airflow/dags"))
    assert len(items) == 2

    first = items[0]
    assert first["dag_id"] == "dag_in"
    assert first["bucket"] == "dags_root"
    assert first["relative_path"] == "team/flow/dag_in.py"
    assert first["folder_parts"] == ["team", "flow"]
    assert first["owners"] == ["alice", "bob"]
    assert first["dag_url"] == "/dags/dag_in"
    assert first["latest_run"] == "2026-04-23T20:00:00+00:00"
    assert first["create_date"] == "2026-04-20T12:00:00+00:00"

    second = items[1]
    assert second["dag_id"] == "dag_out"
    assert second["bucket"] == "external"
    assert second["relative_path"] is None
    assert second["folder_parts"] == []
    assert second["owners"] == []
    assert second["latest_run"] is None
    assert second["create_date"] is None


def test_build_dag_explorer_items_sorted_by_bucket_folder_and_dag_id():
    rows = [
        ("z_dag", False, "/opt/airflow/dags/a/z.py", "", None, None),
        ("a_dag", False, "/opt/airflow/dags/a/a.py", "", None, None),
        ("m_dag", False, "/opt/airflow/dags/b/m.py", "", None, None),
        ("x_dag", False, "/external/x.py", "", None, None),
    ]
    items = ss._build_dag_explorer_items(rows, Path("/opt/airflow/dags"))
    assert [x["dag_id"] for x in items] == ["a_dag", "z_dag", "m_dag", "x_dag"]


def test_discover_dag_explorer_items_uses_env_root(monkeypatch):
    monkeypatch.setenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags")
    with patch.object(
        ss,
        "_read_dag_explorer_rows",
        return_value=[
            ("my_dag", False, "/opt/airflow/dags/p1/d1.py", "owner1", None, None)
        ],
    ):
        data = ss.discover_dag_explorer_items()
    assert data["root"] == "/opt/airflow/dags"
    assert data["count"] == 1
    assert data["items"][0]["dag_id"] == "my_dag"
    assert data["items"][0]["bucket"] == "dags_root"


def test_create_dag_rejects_invalid_payload_shape(client, studio_paths):
    invalid_payload = {
        "project_folder": "webhook",
        "source_conn_id": "src_c",
        "target_conn_id": "tgt_c",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "dwh",
        "target_table": "orders_stg",
        "source_type": "table",
        "load_method": "append",
    }
    r = client.post("/api/create-dag", json=invalid_payload)
    assert r.status_code == 422


def test_timeline_mocked(client):
    fake = [
        {
            "dag_id": "d1",
            "run_id": "r1",
            "state": "success",
            "start_date": None,
            "end_date": None,
        }
    ]
    with patch.object(api_app_module, "fetch_timeline_runs", return_value=fake):
        r = client.get("/api/timeline?dag_id=d1&state=success&limit=10")
    assert r.status_code == 200
    assert r.json()["count"] == 1


def test_dag_config_mocked_success(client):
    mocked = {
        "dag_id": "webhook_whk_level1_src_to_stg_1_dag",
        "payload": {"project": "webhook"},
        "dag_path": "/opt/airflow/dags/webhook/whk/level1/src_to_stg/webhook_whk_level1_src_to_stg_1_dag.py",
        "config_path": "/opt/airflow/projects/webhook/whk/level1/src_to_stg/webhook_whk_level1_src_to_stg_1.yaml",
    }
    with patch.object(
        api_app_module, "resolve_dag_config_for_update", return_value=mocked
    ):
        r = client.get("/api/dag-config?dag_id=webhook_whk_level1_src_to_stg_1_dag")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["dag_id"] == mocked["dag_id"]
    assert body["dag_path"] == mocked["dag_path"]
    assert body["config_path"] == mocked["config_path"]
    assert body["payload"]["project"] == "webhook"


def test_dag_config_not_found_returns_404(client):
    with patch.object(
        api_app_module,
        "resolve_dag_config_for_update",
        side_effect=FileNotFoundError("DAG not found: missing_dag"),
    ):
        r = client.get("/api/dag-config?dag_id=missing_dag")
    assert r.status_code == 404
    assert "missing_dag" in r.json()["detail"]


def test_resolve_dag_config_for_update_roundtrip(client, studio_paths):
    payload = _minimal_table_payload()
    payload["custom_tags"] = ["ops", "nightly"]
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    dag_id = Path(r.json()["dag_path"]).stem

    resolved = ss.resolve_dag_config_for_update(dag_id)
    assert resolved["payload"]["project"] == "webhook"
    assert resolved["payload"]["domain"] == "whk"
    assert resolved["payload"]["level"] == "level1"
    assert resolved["payload"]["flow"] == "src_to_stg"
    assert resolved["payload"]["group_no"] == 1
    assert resolved["payload"]["source_conn_id"] == "src_c"
    assert resolved["payload"]["target_conn_id"] == "tgt_c"
    assert resolved["payload"]["source_table"] == "orders"
    assert resolved["payload"]["target_table"] == "orders_stg"
    assert resolved["payload"]["custom_tags"] == ["ops", "nightly"]
    assert resolved["payload"]["flow_tasks"][0]["depends_on"] == []


def test_resolve_dag_config_returns_empty_custom_tags_when_yaml_missing_field(
    client, studio_paths
):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    dag_id = Path(r.json()["dag_path"]).stem
    flow = Path(r.json()["flow_dir"])
    yaml_path = flow / "webhook_whk_level1_src_to_stg_1.yaml"
    cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    cfg.pop("custom_tags", None)
    yaml_path.write_text(
        yaml.safe_dump(cfg, sort_keys=False, allow_unicode=False), encoding="utf-8"
    )

    resolved = ss.resolve_dag_config_for_update(dag_id)
    assert resolved["payload"]["custom_tags"] == []


def test_create_dag_rejects_invalid_scheduler_cron(client, studio_paths):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "cron_expression": "0 0 * * * *",
        "timezone": "UTC",
        "active": True,
        "start_date": "2023-01-01T00:00:00",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "cron" in r.text.lower()


def test_create_dag_rejects_invalid_scheduler_timezone(client, studio_paths):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "cron_expression": "0 3 * * *",
        "timezone": "Mars/Phobos",
        "active": True,
        "start_date": "2023-01-01T00:00:00",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "timezone" in r.text.lower()


def test_create_dag_rejects_invalid_scheduler_start_date(client, studio_paths):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "cron_expression": "0 3 * * *",
        "timezone": "UTC",
        "active": True,
        "start_date": "invalid-date",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "start_date" in r.text.lower()


def test_create_dag_persists_scheduler_and_preload_returns_scheduler(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "cron_expression": "15 4 * * 1",
        "timezone": "Europe/Istanbul",
        "active": False,
        "start_date": "2024-01-15T09:30:00",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    dag_id = Path(body["dag_path"]).stem
    config_path = Path(body["config_path"])
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    scheduler = cfg.get("scheduler") or {}
    assert scheduler["cron_expression"] == "15 4 * * 1"
    assert scheduler["timezone"] == "Europe/Istanbul"
    assert scheduler["active"] is False
    assert "catchup" not in scheduler
    assert scheduler["start_date"] == "2024-01-15T09:30:00"

    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    scheduler_payload = (r2.json().get("payload") or {}).get("scheduler") or {}
    assert scheduler_payload["cron_expression"] == "15 4 * * 1"
    assert scheduler_payload["timezone"] == "Europe/Istanbul"
    assert scheduler_payload["active"] is False
    assert "catchup" not in scheduler_payload
    assert scheduler_payload["start_date"] == "2024-01-15T09:30:00"


def test_update_dag_updates_scheduler_without_creating_new_dag(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    dag_path_before = r1.json()["dag_path"]

    payload["scheduler"] = {
        "cron_expression": "5 * * * *",
        "timezone": "UTC",
        "active": True,
        "start_date": "2023-01-01T00:00:00",
    }
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    body2 = r2.json()
    assert body2["dag_id"] == dag_id
    assert body2["dag_path"] == dag_path_before
    cfg = yaml.safe_load(Path(body2["config_path"]).read_text(encoding="utf-8"))
    assert (cfg.get("scheduler") or {}).get("cron_expression") == "5 * * * *"


def test_resolve_dag_config_for_update_roundtrip_sql_inline_sql(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT 1 AS id",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[{"name": "id", "source_type": "INTEGER"}],
    ):
        r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    dag_id = Path(r.json()["dag_path"]).stem

    resolved = ss.resolve_dag_config_for_update(dag_id)
    task = resolved["payload"]["flow_tasks"][0]
    assert task["source_type"] == "sql"
    assert task["inline_sql"] == "SELECT 1 AS id"


def test_update_dag_sql_mapping_semantic_same_does_not_touch_file(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id FROM public.orders",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[{"name": "id", "source_type": "INTEGER"}],
    ):
        r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    flow = Path(r1.json()["flow_dir"])
    mapping_path = (
        flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    before = mapping_path.stat().st_mtime_ns

    payload["mapping_content"] = textwrap.dedent("""\
        version: v1
        source_dialect: postgres
        target_dialect: postgres
        columns:
          - source_name: id
            target_name: id
            source_type: TEXT
            target_type: TEXT
            nullable: true
        """)
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[{"name": "id", "source_type": "INTEGER"}],
    ):
        r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    after = mapping_path.stat().st_mtime_ns
    assert after == before


def test_update_dag_sql_mapping_task_group_change_moves_active_path_to_new_file(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "sql",
            "inline_sql": "SELECT id FROM public.orders",
            "source_schema": None,
            "source_table": None,
            "column_mapping_mode": "mapping_file",
            "mapping_content": _sql_mapping_yaml(["id"]),
        }
    )
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[{"name": "id", "source_type": "INTEGER"}],
    ):
        r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    flow = Path(r1.json()["flow_dir"])
    old_path = (
        flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    )
    assert old_path.is_file()

    payload["task_group_id"] = "custom_sql_orders_task"
    with patch.object(
        ss,
        "extract_sql_select_columns_for_conn",
        return_value=[{"name": "id", "source_type": "INTEGER"}],
    ):
        r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    new_path = flow / "mapping" / "1_custom_sql_orders_task.yaml"
    assert new_path.is_file()
    cfg = yaml.safe_load(
        (flow / "webhook_whk_level1_src_to_stg_1.yaml").read_text(encoding="utf-8")
    )
    assert (
        cfg["flow_tasks"][0]["mapping_file"] == "mapping/1_custom_sql_orders_task.yaml"
    )


def test_resolve_dag_config_for_update_roundtrip_bindings(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "id > {{ min_id }}",
            "bindings": [
                {
                    "variable_name": "min_id",
                    "binding_source": "default",
                    "default_value": "100",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    dag_id = Path(r.json()["dag_path"]).stem

    resolved = ss.resolve_dag_config_for_update(dag_id)
    task = resolved["payload"]["flow_tasks"][0]
    assert task["bindings"][0]["variable_name"] == "min_id"
    assert task["bindings"][0]["binding_source"] == "default"
    assert task["bindings"][0]["default_value"] == "100"


def test_resolve_dag_config_for_update_not_found_raises_file_not_found():
    dag_id = "ffengine_config_group_12_public_ff_test_data_to_dbo_ff_test_data_psql_v12"
    with pytest.raises(FileNotFoundError, match="DAG not found"):
        ss.resolve_dag_config_for_update(dag_id)


def test_resolve_dag_config_for_update_with_nonstandard_dag_id_when_studio_dag_exists(
    studio_paths,
):
    proj_root, dag_root = studio_paths
    dag_id = "ffengine_config_group_12_public_ff_test_data_to_dbo_ff_test_data_psql_v12"

    flow_dir = proj_root / "test" / "public" / "level1" / "src_to_odc"
    flow_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = flow_dir / "test_public_level1_src_to_odc_group_12.yaml"
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "source_db_var": "src_c",
                "target_db_var": "tgt_c",
                "flow_tasks": [
                    {
                        "task_group_id": "public_ff_test_data_to_dbo_ff_test_data_psql_v12",
                        "source_schema": "public",
                        "source_table": "ff_test_data",
                        "source_type": "table",
                        "target_schema": "dbo",
                        "target_table": "ff_test_data_psql_v12",
                        "load_method": "append",
                        "column_mapping_mode": "source",
                        "batch_size": 10000,
                        "partitioning": {
                            "enabled": False,
                            "mode": "auto_numeric",
                            "column": None,
                            "parts": 2,
                            "ranges": [],
                        },
                    }
                ],
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )

    dag_path = dag_root / "test" / "public" / "level1" / "src_to_odc" / f"{dag_id}.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    dag_path.write_text(
        "\n".join(
            [
                ss.STUDIO_DAG_MARKER,
                "from pathlib import Path",
                f'CONFIG_PATH = Path("{yaml_path.as_posix()}")',
                f'DAG_ID = "{dag_id}"',
            ]
        ),
        encoding="utf-8",
    )

    resolved = ss.resolve_dag_config_for_update(dag_id)
    assert resolved["payload"]["project"] == "test"
    assert resolved["payload"]["domain"] == "public"
    assert resolved["payload"]["level"] == "level1"
    assert resolved["payload"]["flow"] == "src_to_odc"


def test_dag_payload_invalid_source_type():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="not_a_type",
        )


def test_dag_payload_upsert_requires_match_columns():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            load_method="upsert",
        )


def test_dag_payload_rejects_upsert_for_script_task():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            task_type="script_run",
            script_run_environment="target",
            script_sql="EXEC x",
            load_method="upsert",
            upsert_match_columns=["id"],
        )


def test_dag_payload_rejects_full_scan_partitioning_mode():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            partitioning_mode="full_scan",
        )


def test_dag_payload_accepts_auto_datetime_partitioning_mode():
    payload = DagUpsertPayload(
        project="p",
        domain="d",
        level="level1",
        flow="src_to_stg",
        source_conn_id="a",
        target_conn_id="b",
        source_schema="s",
        source_table="tbl",
        target_schema="t",
        target_table="x",
        source_type="table",
        partitioning_mode="auto_datetime",
    )
    assert payload.partitioning_mode == "auto_datetime"


def test_dag_payload_default_task_type_is_source_target():
    payload = DagUpsertPayload(
        project="p",
        domain="d",
        level="level1",
        flow="src_to_stg",
        source_conn_id="a",
        target_conn_id="b",
        source_schema="s",
        source_table="tbl",
        target_schema="t",
        target_table="x",
        source_type="table",
    )
    assert payload.task_type == "source_target"


def test_dag_payload_rejects_invalid_task_type():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            task_type="invalid_type",
        )


def test_dag_payload_script_run_requires_environment_and_sql():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            task_type="script_run",
            script_sql="DELETE FROM foo",
        )

    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            task_type="script_run",
            script_run_environment="source",
            script_sql="",
        )


def test_dag_payload_script_run_accepts_stored_procedure_text():
    payload = DagUpsertPayload(
        project="p",
        domain="d",
        level="level1",
        flow="src_to_stg",
        source_conn_id="a",
        target_conn_id="b",
        source_schema="s",
        source_table="tbl",
        target_schema="t",
        target_table="x",
        source_type="table",
        task_type="script_run",
        script_run_environment="target",
        script_sql="EXEC dbo.usp_refresh_warehouse @run_id = 1",
    )
    assert payload.task_type == "script_run"
    assert payload.script_run_environment == "target"
    assert "EXEC dbo.usp_refresh_warehouse" in payload.script_sql


def test_dag_payload_dag_task_requires_dag_id():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            project="p",
            domain="d",
            level="level1",
            flow="src_to_stg",
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            source_table="tbl",
            target_schema="t",
            target_table="x",
            source_type="table",
            task_type="dag",
            dag_task_dag_id="",
        )


def test_create_dag_persists_default_source_target_task_type(client, studio_paths):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    config_path = Path(r.json()["config_path"])
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert cfg["flow_tasks"][0]["task_type"] == "source_target"

    dag_id = Path(r.json()["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    task = (r2.json().get("payload") or {}).get("flow_tasks")[0]
    assert task["task_type"] == "source_target"


def test_create_dag_script_run_accepts_sql_and_sp_and_roundtrips(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "task_type": "script_run",
            "script_run_environment": "target",
            "script_sql": "EXEC dbo.usp_housekeeping @mode = 'truncate'",
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    config_path = Path(body["config_path"])
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    task = cfg["flow_tasks"][0]
    assert task["task_type"] == "script_run"
    assert task["script_run_environment"] == "target"
    assert task["script_sql"] == "EXEC dbo.usp_housekeeping @mode = 'truncate'"

    dag_id = Path(body["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    preload_task = ((r2.json().get("payload") or {}).get("flow_tasks") or [])[0]
    assert preload_task["task_type"] == "script_run"
    assert preload_task["script_run_environment"] == "target"
    assert preload_task["script_sql"] == "EXEC dbo.usp_housekeeping @mode = 'truncate'"


def test_create_dag_script_run_does_not_require_target_schema_or_table(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload.update(
        {
            "task_type": "script_run",
            "script_run_environment": "source",
            "script_sql": "DELETE FROM public.orders WHERE id > 0",
            "target_schema": None,
            "target_table": None,
            "flow_tasks": [
                {
                    "task_type": "script_run",
                    "script_run_environment": "source",
                    "script_sql": "DELETE FROM public.orders WHERE id > 0",
                }
            ],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text


def test_bulk_backfill_legacy_task_type_is_idempotent_and_preserves_existing(
    studio_paths,
):
    proj_root, _ = studio_paths
    marker = "_ffengine_task_type_backfilled_once"
    if hasattr(ss._bulk_backfill_legacy_task_types_once, marker):
        delattr(ss._bulk_backfill_legacy_task_types_once, marker)

    flow_dir = proj_root / "webhook" / "legacy" / "level1" / "src_to_stg"
    flow_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = flow_dir / "legacy_legacy_level1_src_to_stg_group_1.yaml"
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "source_db_var": "src_c",
                "target_db_var": "tgt_c",
                "flow_tasks": [
                    {
                        "task_group_id": "legacy_task_missing_type",
                        "source_schema": "public",
                        "source_table": "orders",
                        "source_type": "table",
                        "target_schema": "dwh",
                        "target_table": "orders_stg",
                        "load_method": "append",
                        "column_mapping_mode": "source",
                        "batch_size": 10000,
                        "partitioning": {
                            "enabled": False,
                            "mode": "auto_numeric",
                            "column": None,
                            "parts": 2,
                            "ranges": [],
                        },
                    },
                    {
                        "task_group_id": "legacy_script_task",
                        "task_type": "script_run",
                        "script_run_environment": "target",
                        "script_sql": "DELETE FROM dwh.orders_stg",
                        "source_schema": "public",
                        "source_table": "orders",
                        "source_type": "table",
                        "target_schema": "dwh",
                        "target_table": "orders_stg",
                        "load_method": "append",
                        "column_mapping_mode": "source",
                        "batch_size": 10000,
                        "partitioning": {
                            "enabled": False,
                            "mode": "auto_numeric",
                            "column": None,
                            "parts": 2,
                            "ranges": [],
                        },
                    },
                ],
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )

    ss._bulk_backfill_legacy_task_types_once()
    cfg1 = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert cfg1["flow_tasks"][0]["task_type"] == "source_target"
    assert cfg1["flow_tasks"][1]["task_type"] == "script_run"
    first_pass = yaml.safe_dump(cfg1, sort_keys=False, allow_unicode=False)

    if hasattr(ss._bulk_backfill_legacy_task_types_once, marker):
        delattr(ss._bulk_backfill_legacy_task_types_once, marker)
    ss._bulk_backfill_legacy_task_types_once()
    cfg2 = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    second_pass = yaml.safe_dump(cfg2, sort_keys=False, allow_unicode=False)
    assert second_pass == first_pass


def test_api_key_required_when_env_set(client, studio_paths, monkeypatch):
    monkeypatch.setenv("FLOW_STUDIO_API_KEY", "secret123")
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 401
    r2 = client.post(
        "/api/create-dag",
        json=payload,
        headers={"X-Flow-Studio-API-Key": "secret123"},
    )
    assert r2.status_code == 201
    dag_id = Path(r2.json()["dag_path"]).stem

    r3 = client.delete(f"/api/delete-dag?dag_id={dag_id}")
    assert r3.status_code == 401
    r4 = client.delete(
        f"/api/delete-dag?dag_id={dag_id}",
        headers={"X-Flow-Studio-API-Key": "secret123"},
    )
    assert r4.status_code == 200


# --- F3.2: dbt task type (Enterprise-run; Community seam + 422 gate) ---------


@pytest.fixture
def dbt_provider(monkeypatch):
    from ffengine.airflow import task_type_registry as reg

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    reg.clear_task_type_providers()
    reg.register_task_type_provider("dbt", lambda **kwargs: None)
    yield reg
    reg.clear_task_type_providers()


@pytest.fixture
def no_dbt_provider(monkeypatch):
    from ffengine.airflow import task_type_registry as reg

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    reg.clear_task_type_providers()
    yield reg
    reg.clear_task_type_providers()


def _dbt_flow_task(**overrides) -> dict:
    task = {
        "task_type": "dbt",
        "task_group_id": "dbt_build",
        "dbt_project_ref": "finance",
        "dbt_command": "build",
        "dbt_select": "tag:nightly",
        "depends_on": [],
    }
    task.update(overrides)
    return task


def _dbt_dag_payload(**task_overrides) -> dict:
    payload = _minimal_table_payload()
    payload["flow_tasks"] = [_dbt_flow_task(**task_overrides)]
    return payload


@pytest.mark.parametrize(
    "missing", ["dbt_project_ref", "dbt_command", "dbt_select"]
)
def test_dag_payload_dbt_requires_ref_command_select(dbt_provider, missing):
    from ffengine.ui.api_app import FlowTaskPayload

    task = _dbt_flow_task()
    task.pop(missing)
    with pytest.raises(ValidationError, match=missing):
        FlowTaskPayload(**task)


def test_dag_payload_dbt_rejects_invalid_command(dbt_provider):
    from ffengine.ui.api_app import FlowTaskPayload

    with pytest.raises(ValidationError, match="dbt_command"):
        FlowTaskPayload(**_dbt_flow_task(dbt_command="snapshot"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("script_sql", "SELECT 1"),
        ("script_run_environment", "target"),
        ("dag_task_dag_id", "other_dag"),
        ("where", "x = 1"),
        ("inline_sql", "SELECT 1"),
        ("mapping_file", "m.yaml"),
        (
            "bindings",
            [{"variable_name": "x", "binding_source": "default",
              "default_value": "1"}],
        ),
        ("partitioning_enabled", True),
        ("use_bulk_api", True),
        ("load_method", "upsert"),
    ],
)
def test_dag_payload_dbt_rejects_incompatible_fields(
    dbt_provider, field, value
):
    from ffengine.ui.api_app import FlowTaskPayload

    with pytest.raises(ValidationError):
        FlowTaskPayload(**_dbt_flow_task(**{field: value}))


def test_dag_payload_dbt_vars_reference_requires_declared_param(dbt_provider):
    payload = _dbt_dag_payload(
        dbt_vars={"run_date": "{{ dag.missing_param }}"}
    )
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
    ]
    with pytest.raises(ValidationError, match="not declared"):
        DagUpsertPayload(**payload)


def test_create_dag_dbt_rejected_with_422_when_no_provider(
    client, studio_paths, no_dbt_provider
):
    r = client.post("/api/create-dag", json=_dbt_dag_payload())
    assert r.status_code == 422, r.text
    assert "Enterprise" in r.text


def test_dbt_provider_does_not_unlock_community_edition(
    client, studio_paths, dbt_provider, monkeypatch
):
    monkeypatch.setenv("FFENGINE_EDITION", "community")
    response = client.post("/api/create-dag", json=_dbt_dag_payload())
    assert response.status_code == 422
    assert "Enterprise edition" in response.text


def test_create_dag_dbt_task_persists_narrow_yaml_and_roundtrips(
    client, studio_paths, dbt_provider
):
    payload = _dbt_dag_payload(
        depends_on=["bind_run_date"],
        dbt_vars={"run_date": "{{ dag.run_date }}", "full_refresh": False},
        dbt_target="prod",
        dbt_threads=2,
    )
    payload["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    payload["flow_tasks"].insert(0, {
        "task_type": "binding",
        "task_group_id": "bind_run_date",
        "depends_on": [],
        "bindings": [{
            "variable_name": "run_date",
            "binding_source": "default",
            "default_value": "2026-01-02",
        }],
    })

    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    config_path = Path(body["config_path"])
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    dbt_tasks = [
        t for t in cfg["flow_tasks"] if t.get("task_type") == "dbt"
    ]
    assert len(dbt_tasks) == 1
    task = dbt_tasks[0]
    # Narrow YAML: only the dbt contract keys, no engine fields.
    # F3.2b: dbt_execution is always persisted explicitly (default cosmos).
    # F6.4: dbt_target_platform follows the same explicit-over-implicit
    # rule in cosmos mode (default postgres, EX-D035).
    assert set(task) == {
        "task_type", "task_group_id", "depends_on", "tags",
        "dbt_project_ref", "dbt_command", "dbt_select", "dbt_target",
        "dbt_threads", "dbt_vars", "dbt_execution", "dbt_target_platform",
    }
    assert task["dbt_execution"] == "cosmos"
    assert task["dbt_target_platform"] == "postgres"
    assert task["dbt_project_ref"] == "finance"
    assert task["dbt_vars"] == {
        "run_date": "{{ dag.run_date }}", "full_refresh": False,
    }
    assert task["depends_on"] == ["bind_run_date"]

    dag_id = Path(body["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    preload = r2.json().get("payload") or {}
    preload_dbt = [
        t for t in (preload.get("flow_tasks") or [])
        if t.get("task_type") == "dbt"
    ][0]
    assert preload_dbt["dbt_project_ref"] == "finance"
    assert preload_dbt["dbt_command"] == "build"
    assert preload_dbt["dbt_select"] == "tag:nightly"
    assert preload_dbt["dbt_target"] == "prod"
    assert preload_dbt["dbt_threads"] == 2
    assert preload_dbt["dbt_vars"] == {
        "run_date": "{{ dag.run_date }}", "full_refresh": False,
    }

    # Resave rebuilt from the PRELOAD (the UI pattern): if
    # resolve_dag_config_for_update had dropped any dbt key, this rebuild
    # would lose it and the YAML asserts below would fail.
    def _clean_task(raw: dict) -> dict:
        keep = {
            "task_type", "task_group_id", "depends_on", "bindings",
            "dbt_project_ref", "dbt_command", "dbt_select", "dbt_target",
            "dbt_threads", "dbt_vars",
            "dbt_execution", "dbt_test_behavior", "emit_datasets",
        }
        return {
            key: value
            for key, value in raw.items()
            if key in keep and value not in (None, "", [])
        }

    update_payload = _minimal_table_payload()
    update_payload["dag_params"] = payload["dag_params"]
    update_payload["flow_tasks"] = [
        _clean_task(t) for t in (preload.get("flow_tasks") or [])
    ]
    r3 = client.post(f"/api/update-dag?dag_id={dag_id}", json=update_payload)
    assert r3.status_code == 200, r3.text
    cfg2 = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    task2 = [
        t for t in cfg2["flow_tasks"] if t.get("task_type") == "dbt"
    ][0]
    assert task2["dbt_project_ref"] == "finance"
    assert task2["dbt_select"] == "tag:nightly"
    assert task2["dbt_target"] == "prod"
    assert task2["dbt_threads"] == 2
    assert task2["dbt_vars"] == {
        "run_date": "{{ dag.run_date }}", "full_refresh": False,
    }


def test_legacy_payload_without_dbt_fields_accepted_and_yaml_has_no_dbt_keys(
    client, studio_paths
):
    r = client.post("/api/create-dag", json=_minimal_table_payload())
    assert r.status_code == 201, r.text
    cfg = yaml.safe_load(
        Path(r.json()["config_path"]).read_text(encoding="utf-8")
    )
    for task in cfg["flow_tasks"]:
        assert not any(key.startswith("dbt_") for key in task)


# --- F3.2: dbt card source-contract + edition gate (automated Studio smoke) --


def test_dbt_ui_card_markup_is_enterprise_gated():
    ui_root = Path(api_app_module.__file__).parent
    index_html = (
        ui_root / "templates" / "flow_studio" / "index.html"
    ).read_text(encoding="utf-8")
    style_css = (
        ui_root / "static" / "flow_studio" / "css" / "style.css"
    ).read_text(encoding="utf-8")

    assert (
        '<button class="task-type-chip enterprise-only" type="button" '
        'data-task-type="dbt"' in index_html
    )
    assert '<option value="dbt" class="enterprise-only">dbt</option>' in index_html
    assert 'class="dbt-fields enterprise-only hidden"' in index_html
    for control in (
        "dbt-project-ref", "dbt-command", "dbt-select",
        "dbt-target", "dbt-threads", "dbt-vars",
        # F3.2b (Cosmos): execution mode + cosmos-only knobs
        "dbt-execution", "dbt-test-behavior", "dbt-emit-datasets",
    ):
        assert control in index_html
    assert '<option value="cosmos">' in index_html
    assert '<option value="task">' in index_html
    # Edition gate CSS (F2.1 layer 1): enterprise-only surfaces stay hidden
    # unless the server stamped data-ffengine-edition="enterprise" on <body>.
    assert ".enterprise-only {" in style_css
    assert 'body[data-ffengine-edition="enterprise"] .enterprise-only' in style_css
    assert "app.js?v=100" in index_html


def test_dbt_ui_js_wiring_contract():
    app_js = (
        Path(api_app_module.__file__).parent
        / "static" / "flow_studio" / "js" / "app.js"
    ).read_text(encoding="utf-8")

    assert 'DBT: "dbt"' in app_js
    assert (
        'dbtFields?.classList.toggle("hidden", taskType !== TASK_TYPES.DBT);'
        in app_js
    )
    assert "function collectDbtFields(card, taskType)" in app_js
    assert "function taskParamExpression(task)" in app_js
    assert "function cardParamExpression(card)" in app_js
    assert "dbt Vars must be a flat JSON object" in app_js
    # F3.2b (Cosmos): execution mode wiring — collect + hydrate + the
    # task-mode sync that disables cosmos-only controls.
    assert "function syncDbtExecutionControls(card)" in app_js
    assert "out.dbt_execution = execution;" in app_js
    assert 'if (execution === "cosmos")' in app_js
    assert "out.emit_datasets = true;" in app_js


def test_served_studio_page_hides_dbt_in_community_edition(client):
    r = client.get("/")
    assert r.status_code == 200, r.text
    html = r.text
    assert "__FFENGINE_EDITION__" not in html  # placeholder substituted
    assert 'data-ffengine-edition="community"' in html  # Community default
    assert 'data-ffengine-edition="enterprise"' not in html
    assert 'data-task-type="dbt"' in html  # markup shipped, CSS-gated


# --- F3.2b (Cosmos, EX-D013/EX-D014): execution mode + Asset scheduling -----


def test_create_dag_dbt_persists_cosmos_default_execution(
    client, studio_paths, dbt_provider
):
    r = client.post("/api/create-dag", json=_dbt_dag_payload())
    assert r.status_code == 201, r.text
    cfg = yaml.safe_load(
        Path(r.json()["config_path"]).read_text(encoding="utf-8")
    )
    task = [t for t in cfg["flow_tasks"] if t.get("task_type") == "dbt"][0]
    assert task["dbt_execution"] == "cosmos"
    assert "dbt_test_behavior" not in task
    assert "emit_datasets" not in task


def test_create_dag_dbt_task_mode_roundtrips(
    client, studio_paths, dbt_provider
):
    r = client.post(
        "/api/create-dag", json=_dbt_dag_payload(dbt_execution="task")
    )
    assert r.status_code == 201, r.text
    body = r.json()
    cfg = yaml.safe_load(
        Path(body["config_path"]).read_text(encoding="utf-8")
    )
    task = [t for t in cfg["flow_tasks"] if t.get("task_type") == "dbt"][0]
    assert task["dbt_execution"] == "task"

    dag_id = Path(body["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    preload = [
        t for t in (r2.json()["payload"].get("flow_tasks") or [])
        if t.get("task_type") == "dbt"
    ][0]
    assert preload["dbt_execution"] == "task"


def test_create_dag_dbt_cosmos_knobs_persist_and_preload(
    client, studio_paths, dbt_provider
):
    r = client.post(
        "/api/create-dag",
        json=_dbt_dag_payload(
            dbt_test_behavior="after_each", emit_datasets=True
        ),
    )
    assert r.status_code == 201, r.text
    body = r.json()
    cfg = yaml.safe_load(
        Path(body["config_path"]).read_text(encoding="utf-8")
    )
    task = [t for t in cfg["flow_tasks"] if t.get("task_type") == "dbt"][0]
    assert task["dbt_execution"] == "cosmos"
    assert task["dbt_test_behavior"] == "after_each"
    assert task["emit_datasets"] is True

    dag_id = Path(body["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    preload = [
        t for t in (r2.json()["payload"].get("flow_tasks") or [])
        if t.get("task_type") == "dbt"
    ][0]
    assert preload["dbt_test_behavior"] == "after_each"
    assert preload["emit_datasets"] is True


def test_dag_payload_dbt_unknown_execution_rejected(dbt_provider):
    from ffengine.ui.api_app import FlowTaskPayload

    with pytest.raises(ValidationError, match="dbt_execution"):
        FlowTaskPayload(**_dbt_flow_task(dbt_execution="kubernetes"))


def test_dag_payload_dbt_emit_datasets_task_mode_rejected(dbt_provider):
    from ffengine.ui.api_app import FlowTaskPayload

    with pytest.raises(ValidationError, match="emit_datasets"):
        FlowTaskPayload(
            **_dbt_flow_task(dbt_execution="task", emit_datasets=True)
        )


def test_dag_payload_dbt_test_behavior_task_mode_rejected(dbt_provider):
    from ffengine.ui.api_app import FlowTaskPayload

    with pytest.raises(ValidationError, match="dbt_test_behavior"):
        FlowTaskPayload(
            **_dbt_flow_task(
                dbt_execution="task", dbt_test_behavior="after_each"
            )
        )


def test_scheduler_legacy_shape_has_no_new_keys(client, studio_paths):
    r = client.post("/api/create-dag", json=_minimal_table_payload())
    assert r.status_code == 201, r.text
    cfg = yaml.safe_load(
        Path(r.json()["config_path"]).read_text(encoding="utf-8")
    )
    scheduler = cfg.get("scheduler") or {}
    assert "trigger_type" not in scheduler
    assert "assets" not in scheduler


def test_scheduler_assets_require_asset_trigger(
    client, studio_paths, dbt_provider
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {"assets": ["postgres://db/analytics/fct_orders"]}
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "trigger_type" in r.text


def test_scheduler_asset_trigger_rejects_cron_together(
    client, studio_paths, dbt_provider
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "trigger_type": "asset",
        "assets": ["postgres://db/analytics/fct_orders"],
        "cron_expression": "0 3 * * *",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "cron" in r.text.lower()


def test_scheduler_asset_trigger_requires_nonempty_assets(
    client, studio_paths, dbt_provider
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {"trigger_type": "asset", "assets": []}
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "assets" in r.text


def test_scheduler_asset_duplicates_rejected(
    client, studio_paths, dbt_provider
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "trigger_type": "asset",
        "assets": ["postgres://db/a/t", "postgres://db/a/t"],
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "duplicate" in r.text.lower()


def test_scheduler_asset_trigger_is_enterprise_gated(
    client, studio_paths, dbt_provider, monkeypatch
):
    monkeypatch.setenv("FFENGINE_EDITION", "community")
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "trigger_type": "asset",
        "assets": ["postgres://db/analytics/fct_orders"],
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "Enterprise" in r.text


def test_scheduler_asset_trigger_gate_needs_provider_too(
    client, studio_paths, no_dbt_provider
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "trigger_type": "asset",
        "assets": ["postgres://db/analytics/fct_orders"],
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


# F3.2b Studio slice (EX-D016): Asset catalog capability + save-time guards.

_ASSET_CATALOG_ROWS = [
    {
        "model": "dim_customer",
        "unique_id": "model.finance.dim_customer",
        "uri": "postgres://db/analytics/dim_customer",
    },
    {
        "model": "fct_orders",
        "unique_id": "model.finance.fct_orders",
        "uri": "postgres://db/analytics/fct_orders",
    },
]


@pytest.fixture
def dbt_asset_capability(dbt_provider):
    def _capability(*, project_ref, target_conn_id, **_kw):
        if project_ref == "finance":
            return [dict(row) for row in _ASSET_CATALOG_ROWS]
        return []

    dbt_provider.register_task_type_capability(
        "dbt", "list_asset_uris", _capability
    )
    return dbt_provider


def _producer_dag_payload() -> dict:
    payload = _minimal_table_payload()
    payload["flow_tasks"] = [_dbt_flow_task(emit_datasets=True)]
    return payload


def _asset_consumer_payload(assets: list) -> dict:
    payload = _minimal_table_payload()
    payload["scheduler"] = {"trigger_type": "asset", "assets": assets}
    return payload


def test_scheduler_asset_trigger_persists_and_preloads(
    client, studio_paths, dbt_asset_capability
):
    rp = client.post("/api/create-dag", json=_producer_dag_payload())
    assert rp.status_code == 201, rp.text

    r = client.post(
        "/api/create-dag",
        json=_asset_consumer_payload(
            [
                "postgres://db/analytics/fct_orders",
                "postgres://db/analytics/dim_customer",
            ]
        ),
    )
    assert r.status_code == 201, r.text
    body = r.json()
    cfg = yaml.safe_load(
        Path(body["config_path"]).read_text(encoding="utf-8")
    )
    scheduler = cfg["scheduler"]
    assert scheduler["trigger_type"] == "asset"
    assert scheduler["assets"] == [
        "postgres://db/analytics/fct_orders",
        "postgres://db/analytics/dim_customer",
    ]
    assert scheduler["cron_expression"] is None

    dag_id = Path(body["dag_path"]).stem
    r2 = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert r2.status_code == 200, r2.text
    sched = (r2.json()["payload"]).get("scheduler") or {}
    assert sched["trigger_type"] == "asset"
    assert len(sched["assets"]) == 2


def test_asset_consumer_membership_rejects_underivable_uri(
    client, studio_paths, dbt_asset_capability
):
    # No producer DAG exists -> nothing is derivable -> fail-loud.
    r = client.post(
        "/api/create-dag",
        json=_asset_consumer_payload(["postgres://db/analytics/fct_orders"]),
    )
    assert r.status_code == 422
    assert "producer" in r.text


def test_asset_consumer_without_capability_fails_loud(
    client, studio_paths, dbt_provider
):
    # Provider registered but NO catalog capability (older provider): the
    # consumer save must refuse instead of skipping the membership check.
    r = client.post(
        "/api/create-dag",
        json=_asset_consumer_payload(["postgres://db/analytics/fct_orders"]),
    )
    assert r.status_code == 422
    assert "list_asset_uris" in r.text


def test_producer_change_that_orphans_consumer_rejected(
    client, studio_paths, dbt_asset_capability
):
    rp = client.post("/api/create-dag", json=_producer_dag_payload())
    assert rp.status_code == 201, rp.text
    producer_dag_id = Path(rp.json()["dag_path"]).stem

    rc = client.post(
        "/api/create-dag",
        json=_asset_consumer_payload(["postgres://db/analytics/fct_orders"]),
    )
    assert rc.status_code == 201, rc.text
    consumer_dag_id = Path(rc.json()["dag_path"]).stem

    # cosmos -> task mode change would stop the Asset emission entirely.
    upd = _minimal_table_payload()
    upd["flow_tasks"] = [_dbt_flow_task(dbt_execution="task")]
    r = client.post(f"/api/update-dag?dag_id={producer_dag_id}", json=upd)
    assert r.status_code == 422
    assert consumer_dag_id in r.text

    # emit_datasets kapatmak da ayni orphan mekanizmasina takilir.
    upd2 = _minimal_table_payload()
    upd2["flow_tasks"] = [_dbt_flow_task(emit_datasets=False)]
    r2 = client.post(f"/api/update-dag?dag_id={producer_dag_id}", json=upd2)
    assert r2.status_code == 422
    assert consumer_dag_id in r2.text

    # Positive control: keeping cosmos + emit_datasets stays saveable.
    r3 = client.post(
        f"/api/update-dag?dag_id={producer_dag_id}",
        json=_producer_dag_payload(),
    )
    assert r3.status_code in (200, 201), r3.text


def test_dbt_assets_endpoint_gated_without_provider(
    client, studio_paths, no_dbt_provider
):
    r = client.get("/api/dbt-assets")
    assert r.status_code == 422


def test_dbt_assets_endpoint_lists_producer_uris(
    client, studio_paths, dbt_asset_capability
):
    rp = client.post("/api/create-dag", json=_producer_dag_payload())
    assert rp.status_code == 201, rp.text
    producer_dag_id = Path(rp.json()["dag_path"]).stem

    r = client.get("/api/dbt-assets")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True and body["errors"] == []
    by_uri = {item["uri"]: item for item in body["options"]}
    assert set(by_uri) == {
        "postgres://db/analytics/dim_customer",
        "postgres://db/analytics/fct_orders",
    }
    assert by_uri["postgres://db/analytics/fct_orders"][
        "producer_dag_id"
    ] == producer_dag_id


def test_scheduler_manual_trigger_rejects_cron(client, studio_paths):
    payload = _minimal_table_payload()
    payload["scheduler"] = {
        "trigger_type": "manual",
        "cron_expression": "0 3 * * *",
    }
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "manual" in r.text


def test_scheduler_cron_trigger_requires_cron_expression(
    client, studio_paths
):
    payload = _minimal_table_payload()
    payload["scheduler"] = {"trigger_type": "cron"}
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "cron_expression" in r.text
