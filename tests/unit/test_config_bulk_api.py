"""
F2.1 — ConfigValidator._check_bulk_api: edition + capability gating (fail-loud).

Maps to T-F2.1-5 (edition: bulk API requires Enterprise; Community rejects) and
T-F2.1-4 (unsupported / unregistered method -> fail-loud, no silent executemany).
"""

import pytest

from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ValidationError
from ffengine.pipeline import bulk_registry as reg


class _FakeProvider:
    pass


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    reg.clear_bulk_providers()
    monkeypatch.delenv("FFENGINE_EDITION", raising=False)
    yield
    reg.clear_bulk_providers()


def _valid_task(**extra):
    task = {
        "task_group_id": "t1",
        "source_schema": "public",
        "source_table": "orders",
        "source_type": "table",
        "target_schema": "dwh",
        "target_table": "orders_stg",
        "load_method": "append",
    }
    task.update(extra)
    return task


# ---------------------------------------------------------------------------
# Off path (default) — unchanged behavior (INV-7)
# ---------------------------------------------------------------------------


def test_bulk_off_default_passes():
    ConfigValidator()._check_bulk_api(_valid_task())


def test_method_without_use_bulk_api_is_fail_loud():
    with pytest.raises(ValidationError, match="use_bulk_api=False"):
        ConfigValidator()._check_bulk_api(
            _valid_task(bulk_api_method="postgres_copy")
        )


# ---------------------------------------------------------------------------
# Edition gate — T-F2.1-5
# ---------------------------------------------------------------------------


def test_use_bulk_api_in_community_is_fail_loud(monkeypatch):
    monkeypatch.delenv("FFENGINE_EDITION", raising=False)  # community
    with pytest.raises(ValidationError, match="Enterprise gerektirir"):
        ConfigValidator()._check_bulk_api(
            _valid_task(use_bulk_api=True, bulk_api_method="postgres_copy")
        )


def test_enterprise_missing_method_is_fail_loud(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="zorunludur"):
        ConfigValidator()._check_bulk_api(_valid_task(use_bulk_api=True))


def test_enterprise_unregistered_method_is_fail_loud(monkeypatch):
    # T-F2.1-4: no provider registered -> reject, never silently executemany.
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="Desteklenmeyen bulk_api_method"):
        ConfigValidator()._check_bulk_api(
            _valid_task(use_bulk_api=True, bulk_api_method="postgres_copy")
        )


def test_enterprise_registered_method_passes(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    reg.register_bulk_provider("postgres", "postgres_copy", _FakeProvider)
    ConfigValidator()._check_bulk_api(
        _valid_task(use_bulk_api=True, bulk_api_method="postgres_copy")
    )


# ---------------------------------------------------------------------------
# Full validate() wiring — the edition gate is reached via validate()
# ---------------------------------------------------------------------------


def test_full_validate_rejects_bulk_in_community(monkeypatch):
    monkeypatch.delenv("FFENGINE_EDITION", raising=False)
    with pytest.raises(ValidationError, match="Enterprise gerektirir"):
        ConfigValidator().validate(
            _valid_task(use_bulk_api=True, bulk_api_method="postgres_copy")
        )
