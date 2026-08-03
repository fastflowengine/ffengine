"""
F2.1 — bulk provider registry. Keyed by (conn_type, method); duplicate-different
is fail-loud (no "last-wins"); Community ships no providers. Maps to T-F2.1-4
(unsupported target fail-loud) and the (conn_type, method) capability model.
"""

import pytest

from ffengine.errors.exceptions import ConfigError
from ffengine.pipeline import bulk_registry as reg


@pytest.fixture(autouse=True)
def _clean_registry():
    reg.clear_bulk_providers()
    yield
    reg.clear_bulk_providers()


class _ProviderA:
    pass


class _ProviderB:
    pass


def test_register_and_lookup():
    reg.register_bulk_provider("postgres", "postgres_copy", _ProviderA)
    assert reg.get_bulk_provider("postgres", "postgres_copy") is _ProviderA
    assert reg.is_bulk_supported("postgres", "postgres_copy") is True


def test_lookup_missing_returns_none():
    assert reg.get_bulk_provider("postgres", "postgres_copy") is None
    assert reg.is_bulk_supported("postgres", "postgres_copy") is False


def test_duplicate_same_class_is_idempotent():
    reg.register_bulk_provider("postgres", "postgres_copy", _ProviderA)
    reg.register_bulk_provider("postgres", "postgres_copy", _ProviderA)
    assert reg.get_bulk_provider("postgres", "postgres_copy") is _ProviderA


def test_duplicate_different_class_is_fail_loud():
    reg.register_bulk_provider("postgres", "postgres_copy", _ProviderA)
    with pytest.raises(ConfigError, match="zaten kayıtlı"):
        reg.register_bulk_provider("postgres", "postgres_copy", _ProviderB)


def test_register_non_callable_is_fail_loud():
    with pytest.raises(ConfigError, match="callable"):
        reg.register_bulk_provider("postgres", "postgres_copy", object())


def test_key_is_case_insensitive_and_stripped():
    reg.register_bulk_provider(" Postgres ", " Postgres_Copy ", _ProviderA)
    assert reg.get_bulk_provider("postgres", "postgres_copy") is _ProviderA


def test_valid_methods_for_and_registered_methods():
    reg.register_bulk_provider("postgres", "postgres_copy", _ProviderA)
    reg.register_bulk_provider("oracle", "oracle_direct_path", _ProviderB)
    assert reg.valid_methods_for("postgres") == {"postgres_copy"}
    assert reg.valid_methods_for("oracle") == {"oracle_direct_path"}
    assert reg.valid_methods_for("mssql") == set()
    assert reg.registered_methods() == {"postgres_copy", "oracle_direct_path"}


def test_community_has_no_providers_by_default():
    # Community wheel registers nothing (entry-point discovery finds no
    # ffengine.bulk_providers group) -> any use_bulk_api fails loud downstream.
    assert reg.registered_methods() == set()
