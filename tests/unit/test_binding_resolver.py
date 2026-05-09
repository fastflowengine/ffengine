from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

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


def test_resolve_sql_bindings_datetime_value_normalized_to_utc_timestamp6():
    resolver = BindingResolver()
    target_dt = datetime(2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3)))
    cfg = {
        "where": '"SystemEntryDateTime" >= :last_sync',
        "bindings": [
            {
                "variable_name": "last_sync",
                "binding_source": "target",
                "sql": "SELECT MAX(ts) FROM t",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=_FakeSession([]),
        target_session=_FakeSession([(target_dt,), None]),
    )
    assert out["_resolved_where"] == '"SystemEntryDateTime" >= TIMESTAMP \'2026-01-01 00:00:00.120000\''


def test_resolve_sql_bindings_datetime_value_for_postgres_uses_timestamptz():
    resolver = BindingResolver()
    target_dt = datetime(2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3)))

    class PostgresDialect:
        pass

    cfg = {
        "where": '"SystemEntryDateTime" > :last_sync',
        "bindings": [
            {
                "variable_name": "last_sync",
                "binding_source": "target",
                "sql": "SELECT MAX(ts) FROM t",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=_FakeSession([]),
        target_session=_FakeSession([(target_dt,), None]),
        where_dialect=PostgresDialect(),
    )
    assert (
        out["_resolved_where"]
        == '"SystemEntryDateTime" > TIMESTAMPTZ \'2026-01-01 00:00:00.120000+00:00\''
    )


def test_resolve_sql_bindings_date_value_uses_date_literal():
    resolver = BindingResolver()
    cfg = {
        "where": "event_date >= :min_date",
        "bindings": [
            {
                "variable_name": "min_date",
                "binding_source": "target",
                "sql": "SELECT MIN(event_date) FROM t",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={},
        source_session=_FakeSession([]),
        target_session=_FakeSession([(date(2026, 1, 1),), None]),
    )
    assert out["_resolved_where"] == "event_date >= DATE '2026-01-01'"
