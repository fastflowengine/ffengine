from __future__ import annotations

import pytest

from ffengine.config.binding_resolver import BindingResolver
from ffengine.errors.exceptions import ConfigError


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)
        self._idx = 0
        self.executed_sql = None

    def execute(self, sql):
        self.executed_sql = sql

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        val = self._rows[self._idx]
        self._idx += 1
        return val

    def close(self):
        return None


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.last_cursor = None

    def cursor(self):
        self.last_cursor = _FakeCursor(self._rows)
        return self.last_cursor


def test_resolve_sql_bindings_default_value():
    resolver = BindingResolver()
    cfg = {
        "where": "id > :min_id",
        "bindings": [
            {
                "variable_name": "min_id",
                "binding_source": "default",
                "default_value": "100",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=_FakeSession([]),
        target_session=_FakeSession([]),
    )
    assert out["_resolved_where"] == "id > '100'"


def test_resolve_sql_bindings_airflow_variable():
    resolver = BindingResolver()
    cfg = {
        "where": "updated_at >= :last_sync",
        "bindings": [
            {
                "variable_name": "last_sync",
                "binding_source": "airflow_variable",
                "airflow_variable_key": "etl.last_sync",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={"etl.last_sync": "2026-01-01"},
        source_session=_FakeSession([]),
        target_session=_FakeSession([]),
    )
    assert out["_resolved_where"] == "updated_at >= '2026-01-01'"


def test_resolve_sql_bindings_source_scalar_query():
    resolver = BindingResolver()
    src = _FakeSession([(42,), None])
    cfg = {
        "where": "id > :min_id",
        "bindings": [
            {
                "variable_name": "min_id",
                "binding_source": "source",
                "sql": "SELECT 42",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=src,
        target_session=_FakeSession([]),
    )
    assert src.last_cursor.executed_sql == "SELECT 42"
    assert out["_resolved_where"] == "id > 42"


def test_resolve_sql_bindings_rejects_non_1x1():
    resolver = BindingResolver()
    cfg = {
        "where": "id > :min_id",
        "bindings": [
            {
                "variable_name": "min_id",
                "binding_source": "source",
                "sql": "SELECT a, b",
            }
        ],
    }
    with pytest.raises(ConfigError, match="1x1"):
        resolver.resolve_sql_bindings(
            cfg,
            context={},
            source_session=_FakeSession([(1, 2)]),
            target_session=_FakeSession([]),
        )


def test_resolve_sql_bindings_without_bindings_keeps_where_untouched():
    resolver = BindingResolver()
    cfg = {
        "where": "id > :min_id",
        "bindings": [],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=_FakeSession([]),
        target_session=_FakeSession([]),
    )
    assert "_resolved_where" not in out
