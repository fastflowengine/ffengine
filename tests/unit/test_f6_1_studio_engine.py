"""F6.1 Flow Studio engine contract and API-boundary tests."""

import shutil
import uuid
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

import ffengine.ui.api_app as api_app_module
from ffengine.ui.api_app import DagUpsertPayload, flow_studio_app
from ffengine.ui.studio_service import (
    build_task_dict_for_validation,
    validate_pipeline_payload,
)


def _payload() -> dict:
    return {
        "project": "bank",
        "domain": "lake",
        "level": "raw",
        "flow": "accounts",
        "source_conn_id": "source_db",
        "target_conn_id": "lakehouse",
        "source_schema": "public",
        "source_table": "accounts",
        "target_schema": "lake",
        "target_table": "accounts",
        "source_type": "table",
        "target_type": "db",
        "load_method": "append",
    }


@pytest.fixture
def studio_paths(monkeypatch):
    base = Path("logs") / "f6_1_studio" / uuid.uuid4().hex
    projects = base / "projects"
    dags = base / "dags"
    projects.mkdir(parents=True, exist_ok=True)
    dags.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FFENGINE_STUDIO_PROJECTS_ROOT", str(projects))
    monkeypatch.setenv("FFENGINE_STUDIO_DAG_ROOT", str(dags))
    monkeypatch.setenv("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", "0")
    try:
        yield
    finally:
        shutil.rmtree(base, ignore_errors=True)


def test_studio_model_preserves_nested_engine_contract():
    payload = _payload()
    payload["engine"] = {
        "preference": "spark",
        "spark": {"submit_mode": "k8s", "conn_id": "spark_prod"},
    }

    dumped = DagUpsertPayload(**payload).model_dump(exclude_none=True)

    assert dumped["engine"] == payload["engine"]


def test_studio_validation_attaches_root_engine_to_task(monkeypatch):
    payload = _payload()
    payload["engine"] = {"preference": "standard"}
    captured = {}

    def _capture(_validator, task):
        captured.update(task)

    monkeypatch.setattr(
        "ffengine.ui.studio_service.ConfigValidator.validate", _capture
    )

    validate_pipeline_payload(payload)

    assert captured["_engine_preference"] == "standard"
    assert "_engine_spark" not in captured


def test_create_route_returns_422_for_invalid_spark_endpoint(monkeypatch):
    payload = _payload()
    payload["engine"] = {
        "preference": "spark",
        "spark": {"submit_mode": "local"},
    }
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")

    response = TestClient(flow_studio_app).post("/api/create-dag", json=payload)

    assert response.status_code == 422
    assert "Iceberg" in response.json()["detail"]


def test_task_builder_does_not_leak_root_engine_without_attachment():
    task = build_task_dict_for_validation(_payload())

    assert "_engine_preference" not in task
    assert "engine" not in task


def test_engine_block_round_trips_create_preload_and_update(studio_paths):
    payload = _payload()
    payload["engine"] = {"preference": "standard"}
    client = TestClient(flow_studio_app)

    created = client.post("/api/create-dag", json=payload)

    assert created.status_code == 201
    config_path = Path(created.json()["config_path"])
    assert yaml.safe_load(config_path.read_text(encoding="utf-8"))["engine"] == {
        "preference": "standard"
    }
    dag_id = created.json()["dag_id"]
    loaded = client.get(f"/api/dag-config?dag_id={dag_id}")
    assert loaded.json()["payload"]["engine"] == {"preference": "standard"}

    updated = client.post(f"/api/update-dag?dag_id={dag_id}", json=payload)

    assert updated.status_code == 200
    assert yaml.safe_load(config_path.read_text(encoding="utf-8"))["engine"] == {
        "preference": "standard"
    }


def test_studio_engine_controls_are_edition_gated_and_collected():
    ui_root = Path(api_app_module.__file__).parent
    html = (ui_root / "templates" / "flow_studio" / "index.html").read_text(
        encoding="utf-8"
    )
    js = (ui_root / "static" / "flow_studio" / "js" / "app.js").read_text(
        encoding="utf-8"
    )

    assert 'id="engine_preference"' in html
    assert 'value="spark" class="enterprise-only"' in html
    assert 'id="engine_spark_submit_mode"' in html
    assert 'id="engine_spark_conn_id"' in html
    assert "payload.engine = { preference: enginePreference };" in js
    assert "function syncEngineOptions()" in js
    assert "engineConfigExplicit = !!engine;" in js


def test_studio_spark_payload_fields_match_the_schema_constant():
    """m4: `engine.spark` alan kumesi bugun iki yerde tanimli --
    `VALID_ENGINE_SPARK_FIELDS` ve Pydantic `SparkEnginePayload`. Ikisi
    sessizce ayrisirsa Studio, YAML'in kabul ettigi bir alani reddeder."""
    from ffengine.config.schema import VALID_ENGINE_SPARK_FIELDS
    from ffengine.ui.api_app import SparkEnginePayload

    assert set(SparkEnginePayload.model_fields) == set(VALID_ENGINE_SPARK_FIELDS)


def test_submit_mode_is_normalized_into_the_task_contract(monkeypatch):
    """m3: validator case-insensitive dogruluyordu ama normalize degeri geri
    yazmiyordu; motor tarafi tam esleme ariyor -> 'K8s' dogrulamayi gecip
    submit'te "Unsupported mode" aliyordu."""
    from ffengine.config.schema import ENGINE_SPARK_KEY
    from ffengine.config.validator import ConfigValidator

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = {
        "task_group_id": "t",
        "source_type": "table",
        "target_type": "iceberg",
        "_engine_preference": "spark",
        ENGINE_SPARK_KEY: {"submit_mode": " K8s ", "conn_id": " spark_prod "},
    }
    ConfigValidator().validate_engine_selection(task)
    assert task[ENGINE_SPARK_KEY]["submit_mode"] == "k8s"
    assert task[ENGINE_SPARK_KEY]["conn_id"] == "spark_prod"
