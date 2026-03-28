"""
C08_T13 — ETL Studio FastAPI endpoint unit/API testleri.
"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import ValidationError

import ffengine.ui.api_app as api_app_module
from ffengine.ui.api_app import DagUpsertPayload, etl_studio_app
from ffengine.errors import ConnectionError
from ffengine.ui import studio_service as ss


@pytest.fixture
def client():
    return TestClient(etl_studio_app)


@pytest.fixture
def studio_paths(monkeypatch):
    base = Path("logs") / "etl_studio_test_tmp" / f"case_{uuid.uuid4().hex}"
    proj = base / "projects"
    gen = base / "generated"
    proj.mkdir(parents=True, exist_ok=True)
    gen.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FFENGINE_STUDIO_PROJECTS_ROOT", str(proj))
    monkeypatch.setenv("FFENGINE_STUDIO_DAG_ROOT", str(gen))
    try:
        yield proj, gen
    finally:
        shutil.rmtree(base, ignore_errors=True)


def _minimal_table_payload():
    return {
        "source_conn_id": "src_c",
        "target_conn_id": "tgt_c",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "dwh",
        "target_table": "orders_stg",
        "source_type": "table",
        "load_method": "append",
        "project": "p1",
    }


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["service"] == "etl-studio"
    assert ss.STUDIO_DAG_MARKER in data.get("dag_marker", "")


def test_index_html_ok(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "ETL Configuration Studio" in r.text
    assert 'const base = "/etl-studio"' in r.text
    assert "tryAttachAirflowCss" in r.text
    assert "Filter & Bindings" in r.text
    assert "Create DAG + YAML" in r.text


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


def test_create_dag_writes_files(client, studio_paths):
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["ok"] is True
    assert "task_group_id" in body
    flow = Path(body["flow_dir"])
    assert (flow / "config.yaml").is_file()
    assert (flow / ss.STUDIO_METADATA_NAME).is_file()
    meta = json.loads((flow / ss.STUDIO_METADATA_NAME).read_text(encoding="utf-8"))
    assert "user_tags" in meta
    assert "auto_tags" in meta
    dag_py = Path(body["dag_path"])
    assert dag_py.is_file()
    assert ss.STUDIO_DAG_MARKER in dag_py.read_text(encoding="utf-8")


def test_create_dag_writes_yaml_with_supported_fields(client, studio_paths):
    payload = _minimal_table_payload()
    payload.update(
        {
            "source_type": "view",
            "column_mapping_mode": "mapping_file",
            "mapping_file": "mappings/orders_map.yaml",
            "where": "id > 10",
            "batch_size": 20000,
            "partitioning_enabled": True,
            "partitioning_mode": "percentile",
            "partitioning_column": "id",
            "partitioning_parts": 4,
            "partitioning_ranges": [{"min": 1, "max": 100}],
        }
    )
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 201, r.text

    flow = Path(r.json()["flow_dir"])
    cfg = yaml.safe_load((flow / "config.yaml").read_text(encoding="utf-8"))
    task = cfg["etl_tasks"][0]

    assert task["source_type"] == "view"
    assert task["column_mapping_mode"] == "mapping_file"
    assert task["mapping_file"] == "mappings/orders_map.yaml"
    assert task["batch_size"] == 20000
    assert task["partitioning"]["enabled"] is True
    assert task["partitioning"]["mode"] == "percentile"
    assert task["partitioning"]["column"] == "id"
    assert task["partitioning"]["parts"] == 4
    assert task["partitioning"]["ranges"] == [{"min": 1, "max": 100}]


def test_create_dag_rejects_removed_fields(client, studio_paths):
    payload = _minimal_table_payload()
    payload["reader_workers"] = 4
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 422


def test_create_dag_rejects_removed_tags_field(client, studio_paths):
    p = _minimal_table_payload()
    p["tags"] = ["prod", "nightly"]
    r = client.post("/api/create-dag", json=p)
    assert r.status_code == 422


def test_update_dag_requires_studio_marker(client, studio_paths):
    payload = _minimal_table_payload()
    r0 = client.post("/api/create-dag", json=payload)
    assert r0.status_code == 201
    dag_path = Path(r0.json()["dag_path"])
    dag_path.write_text("# manual dag\n", encoding="utf-8")
    r = client.post("/api/update-dag", json=payload)
    assert r.status_code == 400
    assert "ETL Studio" in r.json()["detail"]


def test_update_dag_ok(client, studio_paths):
    payload = _minimal_table_payload()
    r1 = client.post("/api/create-dag", json=payload)
    assert r1.status_code == 201
    payload["load_method"] = "replace"
    r2 = client.post("/api/update-dag", json=payload)
    assert r2.status_code == 200


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


def test_dag_payload_invalid_source_type():
    with pytest.raises(ValidationError):
        DagUpsertPayload(
            source_conn_id="a",
            target_conn_id="b",
            source_schema="s",
            target_schema="t",
            target_table="x",
            source_type="not_a_type",
        )


def test_api_key_required_when_env_set(client, studio_paths, monkeypatch):
    monkeypatch.setenv("ETL_STUDIO_API_KEY", "secret123")
    payload = _minimal_table_payload()
    r = client.post("/api/create-dag", json=payload)
    assert r.status_code == 401
    r2 = client.post(
        "/api/create-dag",
        json=payload,
        headers={"X-ETL-Studio-API-Key": "secret123"},
    )
    assert r2.status_code == 201
