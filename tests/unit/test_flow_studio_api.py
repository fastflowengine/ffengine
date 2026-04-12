"""
C08_T13 â€” Flow Studio FastAPI endpoint unit/API testleri.
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


def _sql_mapping_yaml(columns: list[str]) -> str:
    lines = ["version: v1", "source_dialect: postgres", "target_dialect: postgres", "columns:"]
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
    assert "Select Source DB Connection" in r.text
    assert "Select Target DB Connection" in r.text
    assert "folder_picker_modal" in r.text
    assert "Group No" not in r.text
    assert "Filter & Bindings" in r.text
    assert "Task Group ID (Opsiyonel)" not in r.text
    assert "task-group-id-readonly" in r.text
    assert "Expand All" in r.text
    assert "Collapse All" in r.text
    assert "Save Configuration" in r.text
    assert "+ Add New Task" in r.text
    assert "Delete DAG" in r.text
    assert "delete_dag_modal" in r.text
    assert "Update DAG + YAML" not in r.text
    assert "Load Timeline" not in r.text
    assert "Timeline DAG ID (opsiyonel)" not in r.text
    assert "Timeline State (opsiyonel)" not in r.text
    assert "Timeline Limit" not in r.text


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
    with patch.object(api_app_module, "discover_schemas", side_effect=ConnectionError("db offline")):
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
    with patch.object(api_app_module, "generate_mapping_preview", return_value=mocked) as fn:
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
    assert body["generated_mapping_file"] == "mapping/1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml"
    fn.assert_called_once()


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
    with patch.object(api_app_module, "discover_airflow_variables", return_value=["k1", "k2"]) as mocked:
        r = client.get("/api/airflow-variables?q=k&limit=50")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["count"] == 2
    assert body["items"] == ["k1", "k2"]
    mocked.assert_called_once_with(search="k", limit=50)


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
    with patch.object(api_app_module, "discover_hierarchy_options", return_value=data) as mocked:
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
    (dag_root / "webhook" / "whk" / "level1" / "src_to_stg").mkdir(parents=True, exist_ok=True)
    (dag_root / "test" / "public_level1" / "src_to_odc").mkdir(parents=True, exist_ok=True)

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
    assert body["task_group_id"] == "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"
    flow = Path(body["flow_dir"])
    assert flow.as_posix().endswith("/projects/webhook/whk/level1/src_to_stg")
    assert (flow / ss.STUDIO_METADATA_NAME).is_file()
    meta = json.loads((flow / ss.STUDIO_METADATA_NAME).read_text(encoding="utf-8"))
    assert "user_tags" in meta
    assert "auto_tags" in meta
    dag_py = Path(body["dag_path"])
    yaml_name = "webhook_whk_level1_src_to_stg_group_1.yaml"
    assert (flow / yaml_name).is_file()
    assert dag_py.as_posix().endswith(
        "/dags/webhook/whk/level1/src_to_stg/whk_to_stg_level1_group_1_dag.py"
    )
    assert dag_py.is_file()
    dag_source = dag_py.read_text(encoding="utf-8")
    assert ss.STUDIO_DAG_MARKER in dag_source
    assert "yaml.safe_load" in dag_source
    assert "FFEngineOperator" in dag_source
    assert "_resolve_task_dependencies" in dag_source
    assert yaml_name in dag_source


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
        (flow / "webhook_whk_level1_src_to_stg_group_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]

    assert task["source_type"] == "view"
    assert task["column_mapping_mode"] == "mapping_file"
    assert task["task_group_id"] == "1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg"
    assert task["mapping_file"] == "mapping/1_1_src_c_public_orders_to_tgt_c_append_dwh_orders_stg.yaml"
    assert task["batch_size"] == 20000
    assert task["partitioning"]["enabled"] is True
    assert task["partitioning"]["mode"] == "explicit"
    assert task["partitioning"]["column"] is None
    assert task["partitioning"]["parts"] == 4
    assert task["partitioning"]["distinct_limit"] == 24
    assert task["partitioning"]["ranges"] == ["id < 100", "id >= 100"]


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
        (flow / "webhook_whk_level1_src_to_stg_group_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["partitioning"]["mode"] == "distinct"
    assert task["partitioning"]["column"] == "country_code"
    assert task["partitioning"]["distinct_limit"] == 9


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
    assert "partitioning.mode gecersiz" in r.text


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
        (flow / "webhook_whk_level1_src_to_stg_group_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]

    assert task["source_type"] == "sql"
    assert task["inline_sql"] == "SELECT id, amount FROM public.orders WHERE amount > 0"
    assert task["task_group_id"] == "1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg"
    assert task["mapping_file"] == "mapping/1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    assert (flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml").is_file()


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
    assert "SQL select kolonlari mapping ile uyumsuz" in r.text


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
    assert "SQL select kolonlari mapping ile uyumsuz" in r.text


def test_create_dag_with_bindings_persists_yaml(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "updated_at >= :last_sync",
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
        (flow / "webhook_whk_level1_src_to_stg_group_1.yaml").read_text(encoding="utf-8")
    )
    task = cfg["flow_tasks"][0]
    assert task["where"] == "updated_at >= :last_sync"
    assert task["bindings"][0]["variable_name"] == "last_sync"
    assert task["bindings"][0]["binding_source"] == "airflow_variable"
    assert task["bindings"][0]["airflow_variable_key"] == "etl.last_sync"


def test_create_dag_rejects_missing_binding_for_where_param(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "updated_at >= :last_sync",
            "bindings": [],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422
    assert "binding tanimi olmayan" in r.text


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
    assert "kullanilmayan" in r.text


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
    assert dag_py.name == "whk_to_delta_sync_level1_group_1_dag.py"


def test_create_dag_same_flow_creates_group_based_dags_and_yamls(client, studio_paths):
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
    assert (flow / "webhook_whk_level1_src_to_stg_group_1.yaml").is_file()
    assert (flow / "webhook_whk_level1_src_to_stg_group_2.yaml").is_file()
    assert Path(body1["dag_path"]).name == "whk_to_stg_level1_group_1_dag.py"
    assert Path(body2["dag_path"]).name == "whk_to_stg_level1_group_2_dag.py"


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


def test_update_dag_targets_selected_dag_when_same_flow_has_multiple_groups(client, studio_paths):
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


def test_dag_revisions_promote_roundtrip(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0]["load_method"] == "replace"

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    rev_items = r_rev.json()["items"]
    assert len(rev_items) >= 2
    revision_count_before = len(rev_items)
    create_revision = next((x["revision_id"] for x in rev_items if x.get("source") == "create_initial"), "")
    update_revision = next((x["revision_id"] for x in rev_items if x.get("source") == "update"), "")
    assert create_revision
    assert update_revision
    assert create_revision != update_revision

    r_promote_old = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
        json={},
    )
    assert r_promote_old.status_code == 200, r_promote_old.text
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0]["load_method"] == "append"

    r_rev_after = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev_after.status_code == 200, r_rev_after.text
    rev_items_after = r_rev_after.json()["items"]
    assert len(rev_items_after) == revision_count_before
    assert not any(str(x.get("source") or "") == "promote_before_switch" for x in rev_items_after)

    r_promote_new = client.post(
        f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={update_revision}",
        json={},
    )
    assert r_promote_new.status_code == 200, r_promote_new.text
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0]["load_method"] == "replace"


def test_promote_rejects_invalid_revision_id_format(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    r = client.post(f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id=bad_revision", json={})
    assert r.status_code == 422
    assert "revision_id" in r.text


def test_promote_returns_404_for_missing_revision(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    r = client.post(f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id=rev_999999", json={})
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
        assert any("DAG dosyasi silinemedi" in w for w in warnings)
    if cfg_deleted:
        assert not cfg_path.exists()
    else:
        assert any("YAML dosyasi silinemedi" in w for w in warnings)
    if mapping_deleted:
        assert not mapping_path.exists()
    else:
        assert any("Mapping dosyasi silinemedi" in w for w in warnings)
    if history_path.exists() and not history_deleted:
        assert any("History dizini silinemedi" in w for w in warnings)
    if metadata_deleted:
        assert not metadata_path.exists()
    elif metadata_path.exists():
        assert any("Metadata dosyasi silinemedi" in w for w in warnings)


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

    with patch.object(ss, "_cleanup_airflow_dag_metadata", side_effect=RuntimeError("db cleanup failed")):
        r2 = client.delete(f"/api/delete-dag?dag_id={dag_id}")

    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["ok"] is True
    assert body["airflow_cleanup"]["ok"] is False
    assert any("cleanup exception" in str(x).lower() for x in body.get("warnings", []))
    if dag_path.exists():
        assert any("DAG dosyasi silinemedi" in str(x) for x in body.get("warnings", []))
    if cfg_path.exists():
        assert any("YAML dosyasi silinemedi" in str(x) for x in body.get("warnings", []))


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


def test_promote_rolls_back_when_parse_verification_fails(client, studio_paths, monkeypatch):
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "1")
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    cfg_path = Path(r1.json()["config_path"])

    payload["load_method"] = "replace"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0]["load_method"] == "replace"

    r_rev = client.get(f"/api/dag-revisions?dag_id={dag_id}")
    assert r_rev.status_code == 200, r_rev.text
    create_revision = next((x["revision_id"] for x in r_rev.json()["items"] if x.get("source") == "create_initial"), "")
    assert create_revision

    with patch.object(ss, "_wait_for_parse_refresh", return_value=False):
        r_promote = client.post(
            f"/api/dag-revisions/promote?dag_id={dag_id}&revision_id={create_revision}",
            json={},
        )
    assert r_promote.status_code == 422
    assert "geri donuldu" in r_promote.text
    assert yaml.safe_load(cfg_path.read_text(encoding="utf-8"))["flow_tasks"][0]["load_method"] == "replace"


def test_update_dag_rejects_dag_id_payload_flow_mismatch(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem

    payload["flow"] = "src_to_dwh"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 422
    assert "hiyerarsisi uyusmuyor" in r2.text


def test_update_dag_rejects_full_scan_partitioning_mode(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201
    dag_id = Path(r1.json()["dag_path"]).stem
    payload["partitioning_enabled"] = True
    payload["partitioning_mode"] = "full_scan"
    r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 422
    assert "partitioning.mode gecersiz" in r2.text


def test_resolve_task_dependencies_depends_on_and_default_order():
    tasks = [
        {"task_group_id": "t1"},
        {"task_group_id": "t2", "depends_on": ["t1"]},
        {"task_group_id": "t3"},
    ]
    edges = ss.resolve_task_dependencies(tasks)
    assert ("t1", "t2") in edges
    assert ("t2", "t3") in edges


def test_resolve_task_dependencies_invalid_upstream():
    tasks = [
        {"task_group_id": "t1"},
        {"task_group_id": "t2", "depends_on": ["missing"]},
    ]
    with pytest.raises(ValueError, match="depends_on gecersiz"):
        ss.resolve_task_dependencies(tasks)


def test_resolve_task_dependencies_cycle_error():
    tasks = [
        {"task_group_id": "t1", "depends_on": ["t2"]},
        {"task_group_id": "t2", "depends_on": ["t1"]},
    ]
    with pytest.raises(ValueError, match="cycle"):
        ss.resolve_task_dependencies(tasks)


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
        "dag_id": "whk_to_stg_level1_group_1_dag",
        "payload": {"project": "webhook"},
        "dag_path": "/opt/airflow/dags/webhook/whk/level1/src_to_stg/whk_to_stg_level1_group_1_dag.py",
        "config_path": "/opt/airflow/projects/webhook/whk/level1/src_to_stg/webhook_whk_level1_src_to_stg_group_1.yaml",
    }
    with patch.object(api_app_module, "resolve_dag_config_for_update", return_value=mocked):
        r = client.get("/api/dag-config?dag_id=whk_to_stg_level1_group_1_dag")
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
        side_effect=FileNotFoundError("DAG bulunamadi: missing_dag"),
    ):
        r = client.get("/api/dag-config?dag_id=missing_dag")
    assert r.status_code == 404
    assert "missing_dag" in r.json()["detail"]


def test_resolve_dag_config_for_update_roundtrip(client, studio_paths):
    payload = _minimal_table_payload()
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
    with patch.object(ss, "extract_sql_select_columns_for_conn", return_value=[{"name": "id", "source_type": "INTEGER"}]):
        r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    flow = Path(r1.json()["flow_dir"])
    mapping_path = flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    before = mapping_path.stat().st_mtime_ns

    payload["mapping_content"] = textwrap.dedent(
        """\
        version: v1
        source_dialect: postgres
        target_dialect: postgres
        columns:
          - source_name: id
            target_name: id
            source_type: TEXT
            target_type: TEXT
            nullable: true
        """
    )
    with patch.object(ss, "extract_sql_select_columns_for_conn", return_value=[{"name": "id", "source_type": "INTEGER"}]):
        r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    after = mapping_path.stat().st_mtime_ns
    assert after == before


def test_update_dag_sql_mapping_task_group_change_moves_active_path_to_new_file(client, studio_paths):
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
    with patch.object(ss, "extract_sql_select_columns_for_conn", return_value=[{"name": "id", "source_type": "INTEGER"}]):
        r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201, r1.text
    dag_id = Path(r1.json()["dag_path"]).stem
    flow = Path(r1.json()["flow_dir"])
    old_path = flow / "mapping" / "1_1_src_c_sql_query_to_tgt_c_append_dwh_orders_stg.yaml"
    assert old_path.is_file()

    payload["task_group_id"] = "custom_sql_orders_task"
    with patch.object(ss, "extract_sql_select_columns_for_conn", return_value=[{"name": "id", "source_type": "INTEGER"}]):
        r2 = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)
    assert r2.status_code == 200, r2.text
    new_path = flow / "mapping" / "1_custom_sql_orders_task.yaml"
    assert new_path.is_file()
    cfg = yaml.safe_load((flow / "webhook_whk_level1_src_to_stg_group_1.yaml").read_text(encoding="utf-8"))
    assert cfg["flow_tasks"][0]["mapping_file"] == "mapping/1_custom_sql_orders_task.yaml"


def test_resolve_dag_config_for_update_roundtrip_bindings(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "where": "id > :min_id",
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
    with pytest.raises(FileNotFoundError, match="DAG bulunamadi"):
        ss.resolve_dag_config_for_update(dag_id)


def test_resolve_dag_config_for_update_with_nonstandard_dag_id_when_studio_dag_exists(studio_paths):
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
                            "mode": "auto",
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

