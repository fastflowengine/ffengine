"""F6.3 — kafka+db kaynak dispatch'i (T-F6.3-22 runtime yarisi).

`source_type='kafka'` bir motor TERCIHI degildir: preflight dogrudan
Enterprise 'cdc' provider'ina gider; provider yoksa/unavailable ise
fail-loud. Sessiz StandardEngine dususu YOKTUR (Standard motorda kafka
okuyucusu yok).
"""

import pytest

from ffengine.airflow.operator import _engine_preflight
from ffengine.core import engine_registry
from ffengine.errors.exceptions import EngineError


def _task(**overrides):
    task = {
        "task_group_id": "cdc_orders",
        "source_type": "kafka",
        "kafka_topic": "pg.public.orders",
        "cdc_start_policy": "earliest",
        "target_schema": "ods",
        "target_table": "orders",
        "target_type": "db",
        "load_method": "cdc_apply",
        "upsert_match_columns": ["id"],
    }
    task.update(overrides)
    return task


def test_kafka_without_cdc_provider_is_fail_loud(monkeypatch):
    monkeypatch.setattr(
        engine_registry, "get_engine_provider", lambda name: None
    )
    with pytest.raises(EngineError, match="cdc"):
        _engine_preflight(_task(), mapped_path=False)


def test_kafka_with_unavailable_provider_is_fail_loud(monkeypatch):
    class _Unavailable:
        def is_available(self):
            return False

    monkeypatch.setattr(
        engine_registry,
        "get_engine_provider",
        lambda name: (_Unavailable if name == "cdc" else None),
    )
    with pytest.raises(EngineError, match="unavailable"):
        _engine_preflight(_task(), mapped_path=False)


def test_kafka_resolves_registered_cdc_provider(monkeypatch):
    class _Cdc:
        def is_available(self):
            return True

    monkeypatch.setattr(
        engine_registry,
        "get_engine_provider",
        lambda name: (_Cdc if name == "cdc" else None),
    )
    engine, engine_type = _engine_preflight(_task(), mapped_path=False)
    assert isinstance(engine, _Cdc)
    assert engine_type == "cdc"


def test_kafka_refuses_partitioned_mapped_path(monkeypatch):
    class _Cdc:
        def is_available(self):
            return True

    monkeypatch.setattr(
        engine_registry,
        "get_engine_provider",
        lambda name: (_Cdc if name == "cdc" else None),
    )
    with pytest.raises(EngineError, match="[Pp]artition"):
        _engine_preflight(_task(), mapped_path=True)


def test_continuous_scheduler_requires_double_gate(monkeypatch):
    """T-F6.3-17 config yarisi: continuous = Enterprise + cdc provider."""
    from ffengine.ui.studio_service import normalize_scheduler

    monkeypatch.setenv("FFENGINE_EDITION", "community")
    with pytest.raises(ValueError, match="continuous"):
        normalize_scheduler({"trigger_type": "continuous"})

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    monkeypatch.setattr(
        engine_registry, "get_engine_provider", lambda name: object()
    )
    normalized = normalize_scheduler({"trigger_type": "continuous"})
    assert normalized["trigger_type"] == "continuous"
    with pytest.raises(ValueError, match="cron_expression"):
        normalize_scheduler(
            {"trigger_type": "continuous", "cron_expression": "0 * * * *"}
        )


def test_kafka_iceberg_path_bypasses_cdc_dispatch(monkeypatch):
    """kafka+iceberg (F6.3b) normal spark preflight'ina gider."""
    calls = []

    monkeypatch.setattr(
        engine_registry,
        "get_engine_provider",
        lambda name: calls.append(name) or None,
    )
    task = _task(
        target_type="iceberg",
        catalog_type="jdbc",
        _engine_preference="spark",
        _engine_spark={"submit_mode": "local", "conn_id": "cat"},
    )
    with pytest.raises(EngineError):
        # spark provider'i da yok -> normal spark fail-loud'u; onemli olan
        # 'cdc' provider'inin HIC sorgulanmamasi.
        _engine_preflight(task, mapped_path=False)
    assert "cdc" not in calls
