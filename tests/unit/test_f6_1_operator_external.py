"""F6.1 additive operator/FlowResult contract tests."""

from unittest.mock import patch

import pytest

from ffengine.core import engine_registry
from ffengine.core.base_engine import FlowResult
from ffengine.errors.exceptions import EngineError, ValidationError


class _ContextEngine:
    contexts = []

    def is_available(self):
        return True

    def run(self, config_path, task_group_id):  # pragma: no cover - must not run
        raise AssertionError("legacy run must not be used")

    def run_with_context(self, config_path, task_group_id, *, context):
        self.contexts.append(context)
        return FlowResult(
            5,
            1.0,
            5.0,
            1,
            engine="spark",
            application_id="application-42",
            snapshot_id="987",
        )


class _LegacyEngine:
    def is_available(self):
        return True

    def run(self, config_path, task_group_id):
        return FlowResult(1, 1.0, 1.0, 1, engine="spark")


def _operator(**overrides):
    from ffengine.airflow.operator import FFEngineOperator

    values = {
        "config_path": "/tmp/flow.yaml",
        "task_group_id": "spark_load",
        "source_conn_id": "source_prod",
        "target_conn_id": "iceberg_prod",
    }
    values.update(overrides)
    return FFEngineOperator(**values)


def _task(**overrides):
    task = {
        "task_group_id": "spark_load",
        "source_type": "table",
        "target_type": "iceberg",
        "where": "business_dt = {{ dag.business_dt }} AND region = {{ airflow.region }}",
        "_engine_preference": "spark",
    }
    task.update(overrides)
    return task


def _runtime_context():
    return {
        "airflow_variables": {"region": "EU", "unused_secret": "do-not-copy"},
        "airflow_params": {"unused": "do-not-copy"},
        "binding_values": {"business_dt": "2026-08-12", "unused": 9},
        "dag_run_conf": {"business_dt": "older", "unused": 8},
    }


@pytest.fixture(autouse=True)
def _reset_registry():
    engine_registry.clear_engine_providers()
    _ContextEngine.contexts.clear()
    yield
    engine_registry.clear_engine_providers()


def test_flow_result_application_and_snapshot_are_additive():
    legacy = FlowResult(1, 1.0, 1.0, 1)
    assert legacy.application_id is None
    assert legacy.snapshot_id is None

    result = FlowResult(
        1, 1.0, 1.0, 1, application_id="app-1", snapshot_id="snap-1"
    )
    assert result.application_id == "app-1"
    assert result.snapshot_id == "snap-1"


def test_external_engine_receives_only_consumed_runtime_values(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    engine_registry.register_engine_provider("spark", _ContextEngine)
    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _task()
        summary = _operator().execute(_runtime_context())

    contract = _ContextEngine.contexts[0]
    assert contract == {
        "schema_version": 1,
        "source_conn_id": "source_prod",
        "target_conn_id": "iceberg_prod",
        "binding_values": {"business_dt": "2026-08-12"},
        "dag_run_conf": {},
        "airflow_params": {},
        "airflow_variables": {"region": "EU"},
    }
    assert summary["application_id"] == "application-42"
    assert summary["snapshot_id"] == "987"


def test_legacy_engine_fails_when_task_consumes_runtime_tokens(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    engine_registry.register_engine_provider("spark", _LegacyEngine)
    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _task()
        with pytest.raises(EngineError, match="run_with_context"):
            _operator().execute(_runtime_context())


def test_unrelated_runtime_values_do_not_require_context_capability(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    engine_registry.register_engine_provider("spark", _LegacyEngine)
    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _task(where="status = 'ACTIVE'")
        summary = _operator().execute(_runtime_context())
    assert summary["rows"] == 1


class _SpyEngine:
    """Records whether the operator ever reached dispatch."""

    ran = False

    def is_available(self):
        return True

    def run(self, config_path, task_group_id):
        type(self).ran = True
        return FlowResult(1, 1.0, 1.0, 1, engine="spark")

    def run_with_context(self, config_path, task_group_id, *, context):
        type(self).ran = True
        return FlowResult(1, 1.0, 1.0, 1, engine="spark")


def _arg_task(**overrides):
    """A task the validator accepted because its YAML carried no engine block."""
    task = {
        "task_group_id": "spark_load",
        "source_type": "table",
        "target_type": "db",
        "writer_workers": None,
        "use_bulk_api": False,
        "partitioning": {"enabled": False},
    }
    task.update(overrides)
    return task


def test_operator_engine_argument_cannot_bypass_spark_config_gates(monkeypatch):
    """BLOCKER-1: `_check_engine` only sees the YAML block, so an explicit
    operator argument used to reach SparkEngine with the knob lock, the
    submit_mode requirement and the endpoint matrix all unevaluated."""
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _SpyEngine.ran = False
    engine_registry.register_engine_provider("spark", _SpyEngine)

    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _arg_task(use_bulk_api=True)
        with pytest.raises(ValidationError):
            _operator(engine="spark").execute(_runtime_context())

    assert _SpyEngine.ran is False


def test_operator_engine_argument_requires_nested_spark_block(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _SpyEngine.ran = False
    engine_registry.register_engine_provider("spark", _SpyEngine)

    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _arg_task(target_type="iceberg")
        with pytest.raises(ValidationError, match="engine.spark"):
            _operator(engine="spark").execute(_runtime_context())

    assert _SpyEngine.ran is False


def test_operator_engine_argument_edition_gate_applies(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "community")
    _SpyEngine.ran = False
    engine_registry.register_engine_provider("spark", _SpyEngine)

    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _arg_task()
        with pytest.raises(ValidationError, match="Enterprise"):
            _operator(engine="spark").execute(_runtime_context())

    assert _SpyEngine.ran is False


def test_yaml_engine_block_is_not_revalidated_at_runtime(monkeypatch):
    """Root YAML stays the single authority: `ConfigLoader.load()` already ran
    the engine rules, so the preflight must not re-run them and reject a task
    the config layer accepted."""
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _SpyEngine.ran = False
    engine_registry.register_engine_provider("spark", _SpyEngine)

    with patch("ffengine.config.loader.ConfigLoader") as loader:
        loader.return_value.load.return_value = _arg_task(
            _engine_preference="spark", target_type="iceberg"
        )
        summary = _operator().execute(_runtime_context())

    assert _SpyEngine.ran is True
    assert summary["engine_type"] == "spark"


def test_on_kill_calls_cancel_once_and_never_leaks_exception():
    class BrokenCancel:
        calls = 0

        def cancel(self):
            self.calls += 1
            raise RuntimeError("credential-like internal detail")

    operator = _operator()
    engine = BrokenCancel()
    operator._active_engine = engine
    operator.on_kill()
    operator.on_kill()
    assert engine.calls == 1
    assert operator._active_engine is None
