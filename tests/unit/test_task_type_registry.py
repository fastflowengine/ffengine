"""
F3.2 — task-type provider registry. Mirrors the F2.1 bulk-provider registry
model: keyed by task_type; duplicate-different is fail-loud (no "last-wins");
Community ships no providers (dbt provider is Enterprise). Maps to T-F3.2-3
(additive task type via provider seam) and the Community-422 edition gate.
"""

import pytest

from ffengine.errors.exceptions import ConfigError
from ffengine.airflow import task_type_registry as reg


@pytest.fixture(autouse=True)
def _clean_registry():
    reg.clear_task_type_providers()
    yield
    reg.clear_task_type_providers()


def _provider_a(**kwargs):
    return ("operator_a", kwargs)


def _provider_b(**kwargs):
    return ("operator_b", kwargs)


def test_register_and_lookup():
    reg.register_task_type_provider("dbt", _provider_a)
    assert reg.get_task_type_provider("dbt") is _provider_a
    assert reg.has_task_type_provider("dbt") is True


def test_lookup_missing_returns_none():
    assert reg.get_task_type_provider("dbt") is None
    assert reg.has_task_type_provider("dbt") is False


def test_duplicate_same_provider_is_idempotent():
    reg.register_task_type_provider("dbt", _provider_a)
    reg.register_task_type_provider("dbt", _provider_a)
    assert reg.get_task_type_provider("dbt") is _provider_a


def test_duplicate_different_provider_is_fail_loud():
    reg.register_task_type_provider("dbt", _provider_a)
    with pytest.raises(ConfigError, match="zaten kay"):
        reg.register_task_type_provider("dbt", _provider_b)


def test_register_non_callable_is_fail_loud():
    with pytest.raises(ConfigError, match="callable"):
        reg.register_task_type_provider("dbt", object())


def test_register_empty_task_type_is_fail_loud():
    with pytest.raises(ConfigError, match="task_type"):
        reg.register_task_type_provider("   ", _provider_a)


def test_key_is_case_insensitive_and_stripped():
    reg.register_task_type_provider(" DBT ", _provider_a)
    assert reg.get_task_type_provider("dbt") is _provider_a
