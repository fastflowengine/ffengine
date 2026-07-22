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
        "where": "id > {{ min_id }}",
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


def test_canonical_simple_parameter_uses_task_local_binding():
    resolver = BindingResolver()
    cfg = {
        "where": "business_date = {{ run_date }}",
        "bindings": [
            {
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-01",
            }
        ],
    }
    out = resolver.resolve_sql_bindings(
        cfg,
        context={
            "binding_values": {"run_date": "2026-01-03"},
            "dag_run_conf": {"run_date": "2026-01-04"},
            "airflow_variables": {},
        },
        source_session=_FakeSession([]),
        target_session=_FakeSession([]),
    )
    assert out["_resolved_where"] == "business_date = '2026-01-01'"


def test_legacy_simple_dag_parameter_without_local_binding_is_supported():
    resolver = BindingResolver()
    out = resolver.resolve_sql_bindings(
        {"where": "business_date = {{ run_date }}", "bindings": []},
        context={
            "binding_values": {"run_date": "2026-01-03"},
            "dag_run_conf": {},
        },
        source_session=None,
        target_session=None,
    )
    assert out["_resolved_where"] == "business_date = '2026-01-03'"


def test_namespaces_keep_local_dag_and_airflow_values_independent():
    resolver = BindingResolver()
    cfg = {
        "where": (
            "local_col = {{ shared }} AND dag_col = {{ dag.shared }} "
            "AND airflow_col = {{ airflow.team.shared }}"
        ),
        "bindings": [
            {
                "variable_name": "shared",
                "binding_source": "default",
                "default_value": "local-value",
            }
        ],
    }

    out = resolver.resolve_sql_bindings(
        cfg,
        context={
            "binding_values": {"shared": "dag-value"},
            "dag_run_conf": {},
            "airflow_variables": {"team.shared": "airflow-value"},
        },
        source_session=_FakeSession([]),
        target_session=_FakeSession([]),
    )

    assert out["_resolved_where"] == (
        "local_col = 'local-value' AND dag_col = 'dag-value' "
        "AND airflow_col = 'airflow-value'"
    )


def test_dag_namespace_prefers_latest_binding_xcom_over_trigger_initial_value():
    resolver = BindingResolver()
    out = resolver.resolve_sql_bindings(
        {"where": "business_date = {{ dag.run_date }}", "bindings": []},
        context={
            "binding_values": {"run_date": "2026-01-03"},
            "dag_run_conf": {"run_date": "2026-01-04"},
            "airflow_variables": {},
        },
        source_session=None,
        target_session=None,
    )
    assert out["_resolved_where"] == "business_date = '2026-01-03'"


def test_dag_namespace_renders_normalized_integer_without_quotes():
    resolver = BindingResolver()
    out = resolver.resolve_sql_bindings(
        {"where": "batch_limit = {{ dag.batch_limit }}", "bindings": []},
        context={
            "binding_values": {},
            "dag_run_conf": {"batch_limit": 3},
            "airflow_variables": {},
        },
        source_session=None,
        target_session=None,
    )

    assert out["_resolved_where"] == "batch_limit = 3"


def test_dag_namespace_does_not_fall_back_to_airflow_param_defaults():
    resolver = BindingResolver()

    with pytest.raises(ConfigError, match="has no declared/runtime value"):
        resolver.resolve_sql_bindings(
            {"where": "business_date = {{ dag.run_date }}", "bindings": []},
            context={
                "binding_values": {},
                "dag_run_conf": {},
                "dag_param_defaults": {"run_date": "2026-01-02"},
                "airflow_variables": {},
            },
            source_session=None,
            target_session=None,
        )


def test_binding_values_ignore_dagrun_override_and_exclude_unbound_conf():
    values = BindingResolver().resolve_binding_values(
        [
            {
                "variable_name": "test1",
                "binding_source": "default",
                "default_value": "selam task icinden",
            }
        ],
        context={
            "binding_values": {"upstream_only": "ignore"},
            "dag_run_conf": {
                "test1": "selam dag trigger",
                "log_level": "default",
            },
            "airflow_variables": {},
        },
        source_session=None,
        target_session=None,
    )

    assert values == {"test1": "selam task icinden"}


def test_binding_scalar_sql_executes_despite_same_name_dagrun_conf():
    source = _FakeSession([(42,), None])
    values = BindingResolver().resolve_binding_values(
        [
            {
                "variable_name": "batch_limit",
                "binding_source": "source",
                "sql": "SELECT 42",
            }
        ],
        context={
            "dag_run_conf": {"batch_limit": 99},
            "airflow_variables": {},
        },
        source_session=source,
        target_session=None,
    )

    assert source.last_cursor.executed_sql == "SELECT 42"
    assert values == {"batch_limit": 42}


def test_airflow_namespace_supports_dotted_real_key():
    resolver = BindingResolver()
    out = resolver.resolve_sql_bindings(
        {"where": "business_date >= {{ airflow.etl.business_date }}"},
        context={"airflow_variables": {"etl.business_date": "2026-01-05"}},
        source_session=None,
        target_session=None,
    )
    assert out["_resolved_where"] == "business_date >= '2026-01-05'"


def test_airflow_namespace_missing_key_fails_loudly():
    resolver = BindingResolver()
    with pytest.raises(ConfigError, match="Airflow Variable 'missing.key'"):
        resolver.resolve_sql_bindings(
            {"where": "id > {{ airflow.missing.key }}"},
            context={"airflow_variables": {}},
            source_session=None,
            target_session=None,
        )


def test_colon_parameter_fails_with_manual_migration_message_before_sql():
    resolver = BindingResolver()
    src = _FakeSession([(42,)])
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
    with pytest.raises(ConfigError, match=r"replace :min_id with \{\{ min_id \}\}"):
        resolver.resolve_sql_bindings(
            cfg,
            context={},
            source_session=src,
            target_session=_FakeSession([]),
        )
    assert src.last_cursor is None


def test_resolve_sql_bindings_airflow_variable():
    resolver = BindingResolver()
    cfg = {
        "where": "updated_at >= {{ last_sync }}",
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
        "where": "id > {{ min_id }}",
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
        "where": "id > {{ min_id }}",
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


def test_resolve_sql_bindings_without_value_fails_loudly():
    resolver = BindingResolver()
    cfg = {
        "where": "id > {{ min_id }}",
        "bindings": [],
    }
    with pytest.raises(ConfigError, match="no declared/runtime value"):
        resolver.resolve_sql_bindings(
            cfg,
            context={},
            source_session=_FakeSession([]),
            target_session=_FakeSession([]),
        )


def test_resolve_sql_bindings_datetime_value_normalized_to_utc_timestamp6():
    resolver = BindingResolver()
    target_dt = datetime(
        2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))
    )
    cfg = {
        "where": '"SystemEntryDateTime" >= {{ last_sync }}',
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
    assert (
        out["_resolved_where"]
        == "\"SystemEntryDateTime\" >= TIMESTAMP '2026-01-01 00:00:00.120000'"
    )


def test_resolve_sql_bindings_datetime_value_for_postgres_uses_timestamptz():
    resolver = BindingResolver()
    target_dt = datetime(
        2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))
    )

    class PostgresDialect:
        pass

    cfg = {
        "where": '"SystemEntryDateTime" > {{ last_sync }}',
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
        == "\"SystemEntryDateTime\" > TIMESTAMPTZ '2026-01-01 00:00:00.120000+00:00'"
    )


def test_resolve_sql_bindings_date_value_uses_date_literal():
    resolver = BindingResolver()
    cfg = {
        "where": "event_date >= {{ min_date }}",
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
