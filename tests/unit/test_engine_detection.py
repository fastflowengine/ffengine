"""F2.3-C — capability-based engine detection (DECISIONS: F2.3 engine detection).

standard/auto -> StandardEngine when no Enterprise provider is registered;
explicit pipeline/spark without a provider -> actionable EngineError;
legacy aliases warn. Community never imports/names the Enterprise package.
"""

from __future__ import annotations

import pytest

from ffengine.core import engine_registry
from ffengine.core.base_engine import BaseEngine, FlowResult
from ffengine.core.flow_manager import StandardEngine
from ffengine.errors.exceptions import ConfigError, EngineError


@pytest.fixture(autouse=True)
def _clean_registry():
    engine_registry.clear_engine_providers()
    yield
    engine_registry.clear_engine_providers()


class _FakePipelineEngine(BaseEngine):
    available = True

    def run(self, config_path: str, task_group_id: str) -> FlowResult:
        raise NotImplementedError

    def is_available(self) -> bool:
        return self.available


class _UnavailableEngine(_FakePipelineEngine):
    available = False


def test_standard_returns_standard_engine():
    engine = BaseEngine.detect("standard")
    assert isinstance(engine, StandardEngine)


def test_auto_without_provider_returns_standard_engine():
    engine = BaseEngine.detect("auto")
    assert isinstance(engine, StandardEngine)


def test_default_preference_is_auto():
    assert isinstance(BaseEngine.detect(), StandardEngine)


def test_explicit_pipeline_without_provider_fails_loud():
    with pytest.raises(EngineError) as excinfo:
        BaseEngine.detect("pipeline")
    message = str(excinfo.value)
    assert "pipeline" in message
    assert "provider" in message


def test_explicit_spark_without_provider_fails_loud():
    with pytest.raises(EngineError):
        BaseEngine.detect("spark")


def test_auto_with_available_provider_returns_it():
    engine_registry.register_engine_provider("pipeline", _FakePipelineEngine)
    engine = BaseEngine.detect("auto")
    assert isinstance(engine, _FakePipelineEngine)


def test_auto_with_unavailable_provider_falls_back_to_standard():
    engine_registry.register_engine_provider("pipeline", _UnavailableEngine)
    engine = BaseEngine.detect("auto")
    assert isinstance(engine, StandardEngine)


def test_explicit_provider_unavailable_fails_loud():
    engine_registry.register_engine_provider("pipeline", _UnavailableEngine)
    with pytest.raises(EngineError) as excinfo:
        BaseEngine.detect("pipeline")
    assert "unavailable" in str(excinfo.value)


def test_explicit_provider_available_is_returned():
    engine_registry.register_engine_provider("spark", _FakePipelineEngine)
    engine = BaseEngine.detect("spark")
    assert isinstance(engine, _FakePipelineEngine)


def test_legacy_community_alias_warns_and_returns_standard():
    with pytest.warns(DeprecationWarning):
        engine = BaseEngine.detect("community")
    assert isinstance(engine, StandardEngine)


def test_legacy_enterprise_alias_warns_and_behaves_like_auto():
    with pytest.warns(DeprecationWarning):
        engine = BaseEngine.detect("enterprise")
    assert isinstance(engine, StandardEngine)  # no provider -> auto fallback

    engine_registry.clear_engine_providers()
    engine_registry.register_engine_provider("pipeline", _FakePipelineEngine)
    with pytest.warns(DeprecationWarning):
        engine = BaseEngine.detect("enterprise")
    assert isinstance(engine, _FakePipelineEngine)


def test_unknown_preference_fails_loud():
    with pytest.raises(EngineError) as excinfo:
        BaseEngine.detect("warp-drive")
    assert "warp-drive" in str(excinfo.value)


def test_duplicate_provider_registration_fails_loud():
    engine_registry.register_engine_provider("pipeline", _FakePipelineEngine)
    engine_registry.register_engine_provider("pipeline", _FakePipelineEngine)
    with pytest.raises(ConfigError):
        engine_registry.register_engine_provider(
            "pipeline", _UnavailableEngine
        )


def test_standard_engine_is_python_engine_alias():
    from ffengine.core.flow_manager import PythonEngine

    assert issubclass(StandardEngine, PythonEngine) or (
        StandardEngine is PythonEngine
    )
