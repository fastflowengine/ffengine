"""
C06 - Partitioner unit tests.

Scope: all strategies, disabled shortcut, and SQL generation details.
No real DB connection is required; src_conn and src_dialect are mocked.
"""

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from ffengine.errors.exceptions import PartitionError
from ffengine.partition.partitioner import Partitioner


def _dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    d.get_pagination_query.side_effect = (
        lambda query, limit, offset: f"{query} LIMIT {limit} OFFSET {offset}"
    )
    return d


def _conn(fetchone=None, fetchall=None):
    conn = MagicMock()
    cursor = MagicMock()
    if fetchone is not None:
        cursor.fetchone.return_value = fetchone
    if fetchall is not None:
        cursor.fetchall.return_value = fetchall
    conn.cursor.return_value = cursor
    return conn


def _task(part_override=None, **overrides) -> dict:
    base = {
        "source_type": "table",
        "source_schema": "public",
        "source_table": "orders",
        "partitioning": {
            "enabled": True,
            "mode": "auto_numeric",
            "parts": 4,
            "distinct_limit": 16,
            "column": "id",
            "ranges": [],
        },
    }
    if part_override:
        base["partitioning"].update(part_override)
    base.update(overrides)
    return base


class TestPartitionerDisabled:
    def test_disabled_returns_single_partition_spec(self):
        result = Partitioner().plan(_task({"enabled": False}), MagicMock(), _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_disabled_spec_where_is_none(self):
        result = Partitioner().plan(_task({"enabled": False}), MagicMock(), _dialect())
        assert result[0]["where"] is None

    def test_disabled_no_db_call(self):
        conn = MagicMock()
        Partitioner().plan(_task({"enabled": False}), conn, _dialect())
        conn.cursor.assert_not_called()


class TestPartitionerUnsupportedMode:
    def test_full_scan_mode_is_rejected(self):
        task = _task({"mode": "full_scan", "column": None})
        with pytest.raises(PartitionError, match="Bilinmeyen partition modu"):
            Partitioner().plan(task, MagicMock(), _dialect())


class TestPartitionerExplicit:
    def test_explicit_three_ranges(self):
        ranges = ["id < 100", "id >= 100 AND id < 200", "id >= 200"]
        task = _task({"mode": "explicit", "ranges": ranges, "column": None})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert result[0] == {"part_id": 0, "where": "id < 100"}
        assert result[1] == {"part_id": 1, "where": "id >= 100 AND id < 200"}
        assert result[2] == {"part_id": 2, "where": "id >= 200"}

    def test_explicit_empty_ranges_raises_partition_error(self):
        task = _task({"mode": "explicit", "ranges": [], "column": None})
        with pytest.raises(PartitionError, match="ranges"):
            Partitioner().plan(task, MagicMock(), _dialect())

    def test_explicit_no_db_call(self):
        conn = MagicMock()
        task = _task({"mode": "explicit", "ranges": ["id < 10"], "column": None})
        Partitioner().plan(task, conn, _dialect())
        conn.cursor.assert_not_called()

    def test_explicit_non_string_clause_raises_partition_error(self):
        task = _task({"mode": "explicit", "ranges": [{"min": 1, "max": 10}], "column": None})
        with pytest.raises(PartitionError, match="string"):
            Partitioner().plan(task, MagicMock(), _dialect())


class TestPartitionerAutoNumeric:
    def test_auto_numeric_returns_n_parts(self):
        result = Partitioner().plan(_task({"parts": 4}), _conn(fetchone=(1, 1000)), _dialect())
        assert len(result) == 4

    def test_auto_numeric_part_ids_sequential(self):
        result = Partitioner().plan(_task({"parts": 4}), _conn(fetchone=(1, 1000)), _dialect())
        assert [s["part_id"] for s in result] == [0, 1, 2, 3]

    def test_auto_numeric_last_partition_uses_lte(self):
        result = Partitioner().plan(_task({"parts": 2}), _conn(fetchone=(0, 100)), _dialect())
        assert "<=" in result[-1]["where"]

    def test_auto_numeric_first_partitions_use_lt(self):
        result = Partitioner().plan(_task({"parts": 3}), _conn(fetchone=(0, 100)), _dialect())
        for spec in result[:-1]:
            assert " < " in spec["where"]
            assert "<=" not in spec["where"]

    def test_auto_numeric_last_partition_hi_pins_to_max_value(self):
        result = Partitioner().plan(_task({"parts": 3}), _conn(fetchone=(0, 5)), _dialect())
        assert result[-1]["where"].endswith("<= 5")

    def test_auto_numeric_empty_table_falls_back_to_single_partition(self):
        result = Partitioner().plan(_task(), _conn(fetchone=(None, None)), _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_auto_numeric_single_value_falls_back_to_single_partition(self):
        result = Partitioner().plan(_task(), _conn(fetchone=(42, 42)), _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_auto_numeric_uses_quoted_identifiers(self):
        conn = _conn(fetchone=(1, 100))
        dialect = _dialect()
        Partitioner().plan(_task(), conn, dialect)
        called_args = [c.args[0] for c in dialect.quote_identifier.call_args_list]
        assert "id" in called_args
        assert "public" in called_args
        assert "orders" in called_args

    def test_auto_numeric_applies_resolved_where_to_sampling_query(self):
        conn = _conn(fetchone=(1, 100))
        task = _task(_resolved_where="status = 'ACTIVE'")
        Partitioner().plan(task, conn, _dialect())
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "WHERE status = 'ACTIVE'" in sql

    def test_auto_numeric_uses_where_when_resolved_where_missing(self):
        conn = _conn(fetchone=(1, 100))
        task = _task(where="created_at >= '2026-01-01'")
        Partitioner().plan(task, conn, _dialect())
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "WHERE created_at >= '2026-01-01'" in sql

    def test_auto_numeric_uses_inline_sql_relation_when_source_type_sql(self):
        conn = _conn(fetchone=(1, 100))
        task = _task(
            source_type="sql",
            source_schema=None,
            source_table=None,
            inline_sql="SELECT id, amount FROM public.orders",
            _resolved_where="id > 10",
        )
        Partitioner().plan(task, conn, _dialect())
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert 'FROM (SELECT id, amount FROM public.orders) AS ffengine_inline_sql' in sql
        assert "WHERE id > 10" in sql


class TestPartitionerAutoDatetime:
    def test_auto_datetime_returns_n_parts(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 2}),
            _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0), datetime(2026, 1, 3, 0, 0, 0))),
            _dialect(),
        )
        assert len(result) == 2

    def test_auto_datetime_where_uses_timestamp_literals(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 2}),
            _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0), datetime(2026, 1, 3, 0, 0, 0))),
            _dialect(),
        )
        assert "TIMESTAMP '" in result[0]["where"]
        assert '"created_at" >=' in result[0]["where"]

    def test_auto_datetime_applies_resolved_where_to_sampling_query(self):
        conn = _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0), datetime(2026, 1, 3, 0, 0, 0)))
        task = _task(
            {"mode": "auto_datetime", "column": "created_at", "parts": 2},
            _resolved_where="created_at >= '2026-01-01'",
        )
        Partitioner().plan(task, conn, _dialect())
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "WHERE created_at >= '2026-01-01'" in sql

    def test_auto_datetime_literals_use_fixed_six_digit_precision(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 2}),
            _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0, 120000), datetime(2026, 1, 1, 0, 0, 1, 320000))),
            _dialect(),
        )
        assert ".120000" in result[0]["where"]

    def test_auto_datetime_boundaries_are_contiguous_without_gap_or_overlap(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 4}),
            _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0, 100000), datetime(2026, 1, 1, 0, 0, 4, 900000))),
            _dialect(),
        )

        boundary_re = re.compile(
            r">= TIMESTAMP '([^']+)' AND .* (?:<|<=) TIMESTAMP '([^']+)'"
        )
        parsed = []
        for spec in result:
            match = boundary_re.search(spec["where"])
            assert match is not None
            lo = datetime.fromisoformat(match.group(1))
            hi = datetime.fromisoformat(match.group(2))
            parsed.append((lo, hi, spec["where"]))

        for idx in range(len(parsed) - 1):
            assert parsed[idx][1] == parsed[idx + 1][0]

    def test_auto_datetime_aware_values_are_normalized_to_utc(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 2}),
            _conn(
                fetchone=(
                    datetime(2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))),
                    datetime(2026, 1, 1, 5, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))),
                )
            ),
            _dialect(),
        )
        assert "TIMESTAMP '2026-01-01 00:00:00.120000'" in result[0]["where"]

    def test_auto_datetime_aware_values_use_timestamptz_for_postgres(self):
        class PostgresDialect:
            def quote_identifier(self, name):
                return f'"{name}"'

        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 2}),
            _conn(
                fetchone=(
                    datetime(2026, 1, 1, 3, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))),
                    datetime(2026, 1, 1, 5, 0, 0, 120000, tzinfo=timezone(timedelta(hours=3))),
                )
            ),
            PostgresDialect(),
        )
        assert "TIMESTAMPTZ '2026-01-01 00:00:00.120000+00:00'" in result[0]["where"]

    def test_auto_datetime_last_partition_hi_pins_to_source_max(self):
        result = Partitioner().plan(
            _task({"mode": "auto_datetime", "column": "created_at", "parts": 4}),
            _conn(fetchone=(datetime(2026, 1, 1, 0, 0, 0, 0), datetime(2026, 1, 1, 0, 0, 0, 5))),
            _dialect(),
        )
        assert "TIMESTAMP '2026-01-01 00:00:00.000005'" in result[-1]["where"]


class TestPartitionerHashMod:
    def test_hash_mod_returns_n_parts(self):
        task = _task({"mode": "hash_mod", "parts": 3})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert len(result) == 3

    def test_hash_mod_part_ids_sequential(self):
        task = _task({"mode": "hash_mod", "parts": 3})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert [s["part_id"] for s in result] == [0, 1, 2]

    def test_hash_mod_where_contains_mod_syntax(self):
        task = _task({"mode": "hash_mod", "parts": 3})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        for spec in result:
            where = spec["where"]
            assert "MOD(" in where or "%" in where

    def test_hash_mod_no_db_call(self):
        conn = MagicMock()
        task = _task({"mode": "hash_mod", "parts": 2})
        Partitioner().plan(task, conn, _dialect())
        conn.cursor.assert_not_called()

    def test_hash_mod_mssql_uses_percent_operator(self):
        class _FakeMSSQLDialect:
            __name__ = "MSSQLDialect"

            def quote_identifier(self, name):
                return f'"{name}"'

        _FakeMSSQLDialect.__name__ = "MSSQLDialect"
        result = Partitioner().plan(
            _task({"mode": "hash_mod", "parts": 2}),
            MagicMock(),
            _FakeMSSQLDialect(),
        )
        for spec in result:
            assert "%" in spec["where"]


class TestPartitionerDistinct:
    def test_distinct_groups_numeric_values(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        result = Partitioner().plan(_task({"mode": "distinct", "parts": 2}), conn, _dialect())
        assert len(result) == 2
        assert "IN" in result[0]["where"]

    def test_distinct_string_values_quoted(self):
        conn = _conn(fetchall=[("US",), ("EU",)])
        result = Partitioner().plan(_task({"mode": "distinct", "parts": 2}), conn, _dialect())
        assert "'US'" in result[0]["where"] or "'EU'" in result[0]["where"]

    def test_distinct_empty_table_falls_back_to_single_partition(self):
        result = Partitioner().plan(
            _task({"mode": "distinct", "parts": 4}),
            _conn(fetchall=[]),
            _dialect(),
        )
        assert result == [{"part_id": 0, "where": None}]

    def test_distinct_fewer_values_than_parts(self):
        conn = _conn(fetchall=[(10,), (20,)])
        result = Partitioner().plan(_task({"mode": "distinct", "parts": 4}), conn, _dialect())
        assert len(result) == 2

    def test_distinct_part_ids_sequential(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        result = Partitioner().plan(_task({"mode": "distinct", "parts": 2}), conn, _dialect())
        assert [s["part_id"] for s in result] == [0, 1]

    def test_distinct_limit_applies_pagination_limit(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        dialect = _dialect()
        Partitioner().plan(_task({"mode": "distinct", "parts": 2, "distinct_limit": 2}), conn, dialect)
        dialect.get_pagination_query.assert_called_once()
        args = dialect.get_pagination_query.call_args.args
        assert args[1] == 2
        assert args[2] == 0

    def test_distinct_invalid_distinct_limit_raises_partition_error(self):
        task = _task({"mode": "distinct", "parts": 2, "distinct_limit": 0})
        with pytest.raises(PartitionError, match="distinct_limit"):
            Partitioner().plan(task, _conn(fetchall=[(1,), (2,)]), _dialect())

    def test_distinct_applies_resolved_where_to_sampling_query(self):
        conn = _conn(fetchall=[(1,), (2,)])
        task = _task({"mode": "distinct", "parts": 2}, _resolved_where="status = 'ACTIVE'")
        Partitioner().plan(task, conn, _dialect())
        sql = conn.cursor.return_value.execute.call_args.args[0]
        assert "WHERE status = 'ACTIVE'" in sql


class TestPartitionerPercentile:
    def test_percentile_falls_back_to_auto_numeric_on_error(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (1, 1000)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        result = Partitioner().plan(_task({"mode": "percentile", "parts": 4}), conn, _dialect())
        assert len(result) == 4

    def test_percentile_query_attempted_before_fallback(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 100)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch.object(Partitioner, "_query_percentiles", side_effect=Exception("forced")) as mock_qp:
            result = Partitioner().plan(_task({"mode": "percentile", "parts": 2}), conn, _dialect())

        mock_qp.assert_called_once()
        assert len(result) == 2

    def test_percentile_applies_resolved_where_to_sampling_queries(self):
        class PostgresDialect:
            def quote_identifier(self, name):
                return f'"{name}"'

        percentile_cursor = MagicMock()
        percentile_cursor.fetchone.return_value = (50,)
        minmax_cursor = MagicMock()
        minmax_cursor.fetchone.return_value = (0, 100)
        conn = MagicMock()
        conn.cursor.side_effect = [percentile_cursor, minmax_cursor]

        task = _task({"mode": "percentile", "parts": 2}, _resolved_where="id > 10")
        result = Partitioner().plan(task, conn, PostgresDialect())

        assert len(result) == 2
        percentile_sql = percentile_cursor.execute.call_args.args[0]
        minmax_sql = minmax_cursor.execute.call_args.args[0]
        assert "WHERE id > 10" in percentile_sql
        assert "WHERE id > 10" in minmax_sql


class TestPartitionerPercentileDialectSql:
    def test_percentile_supports_postgres_dialect_name(self):
        class PostgresDialect:
            def quote_identifier(self, name):
                return f'"{name}"'

        percentile_cursor = MagicMock()
        percentile_cursor.fetchone.return_value = (50,)
        minmax_cursor = MagicMock()
        minmax_cursor.fetchone.return_value = (0, 100)
        conn = MagicMock()
        conn.cursor.side_effect = [percentile_cursor, minmax_cursor]

        result = Partitioner().plan(_task({"mode": "percentile", "parts": 2}), conn, PostgresDialect())

        assert len(result) == 2
        assert "50" in result[0]["where"]
        assert conn.cursor.call_count == 2
        executed_sql = percentile_cursor.execute.call_args.args[0]
        assert "PERCENTILE_CONT" in executed_sql
        assert "OVER ()" not in executed_sql
        assert "LIMIT 1" not in executed_sql

    def test_percentile_mssql_uses_top_1_not_limit(self):
        p = Partitioner()
        cursor = MagicMock()
        cursor.fetchone.return_value = (42,)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        p._query_percentiles(
            dialect_name="MSSQLDialect",
            fractions=[0.5],
            q_col="[id]",
            relation="[dbo].[orders]",
            planned_where=None,
            src_conn=conn,
        )

        sql = cursor.execute.call_args.args[0]
        assert "TOP 1" in sql
        assert "OVER ()" in sql
        assert "LIMIT 1" not in sql
