"""
C06 — Partitioner birim testleri.

Kapsam: tüm 6 strateji, disabled kısayol, hata senaryoları.
DB bağlantısı gerektirmez — src_conn ve src_dialect MagicMock ile simüle edilir.
"""

import pytest
from unittest.mock import MagicMock, call

from ffengine.partition.partitioner import Partitioner
from ffengine.errors.exceptions import PartitionError


# ---------------------------------------------------------------------------
# Yardımcılar
# ---------------------------------------------------------------------------


def _dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    d.get_pagination_query.side_effect = (
        lambda query, limit, offset: f"{query} LIMIT {limit} OFFSET {offset}"
    )
    return d


def _conn(fetchone=None, fetchall=None):
    """Basit bir DB bağlantısı mock'u."""
    conn = MagicMock()
    cursor = MagicMock()
    if fetchone is not None:
        cursor.fetchone.return_value = fetchone
    if fetchall is not None:
        cursor.fetchall.return_value = fetchall
    conn.cursor.return_value = cursor
    return conn


def _task(part_override=None) -> dict:
    base = {
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
    return base


# ---------------------------------------------------------------------------
# Disabled (partitioning kapalı)
# ---------------------------------------------------------------------------


class TestPartitionerDisabled:
    def test_disabled_returns_single_partition_spec(self):
        task = _task({"enabled": False})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_disabled_spec_where_is_none(self):
        task = _task({"enabled": False})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert result[0]["where"] is None

    def test_disabled_no_db_call(self):
        conn = MagicMock()
        task = _task({"enabled": False})
        Partitioner().plan(task, conn, _dialect())
        conn.cursor.assert_not_called()


# ---------------------------------------------------------------------------
# unsupported mode
# ---------------------------------------------------------------------------


class TestPartitionerUnsupportedMode:
    def test_full_scan_mode_is_rejected(self):
        task = _task({"mode": "full_scan", "column": None})
        with pytest.raises(PartitionError, match="Bilinmeyen partition modu"):
            Partitioner().plan(task, MagicMock(), _dialect())


# ---------------------------------------------------------------------------
# explicit
# ---------------------------------------------------------------------------


class TestPartitionerExplicit:
    def test_explicit_three_ranges(self):
        ranges = ["id < 100", "id >= 100 AND id < 200", "id >= 200"]
        task = _task({"mode": "explicit", "ranges": ranges, "column": None})
        result = Partitioner().plan(task, MagicMock(), _dialect())
        assert len(result) == 3
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


# ---------------------------------------------------------------------------
# auto_numeric
# ---------------------------------------------------------------------------


class TestPartitionerAutoNumeric:
    def test_auto_numeric_returns_n_parts(self):
        conn = _conn(fetchone=(1, 1000))
        result = Partitioner().plan(_task({"parts": 4}), conn, _dialect())
        assert len(result) == 4

    def test_auto_numeric_part_ids_sequential(self):
        conn = _conn(fetchone=(1, 1000))
        result = Partitioner().plan(_task({"parts": 4}), conn, _dialect())
        assert [s["part_id"] for s in result] == [0, 1, 2, 3]

    def test_auto_numeric_last_partition_uses_lte(self):
        conn = _conn(fetchone=(0, 100))
        result = Partitioner().plan(_task({"parts": 2}), conn, _dialect())
        assert "<=" in result[-1]["where"]

    def test_auto_numeric_first_partitions_use_lt(self):
        conn = _conn(fetchone=(0, 100))
        result = Partitioner().plan(_task({"parts": 3}), conn, _dialect())
        for spec in result[:-1]:
            assert " < " in spec["where"]
            assert "<=" not in spec["where"]

    def test_auto_numeric_empty_table_falls_back_to_single_partition(self):
        conn = _conn(fetchone=(None, None))
        result = Partitioner().plan(_task(), conn, _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_auto_numeric_single_value_falls_back_to_single_partition(self):
        conn = _conn(fetchone=(42, 42))
        result = Partitioner().plan(_task(), conn, _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_auto_numeric_uses_quoted_identifiers(self):
        conn = _conn(fetchone=(1, 100))
        dialect = _dialect()
        Partitioner().plan(_task(), conn, dialect)
        called_args = [c.args[0] for c in dialect.quote_identifier.call_args_list]
        assert "id" in called_args
        assert "public" in called_args
        assert "orders" in called_args


# ---------------------------------------------------------------------------
# hash_mod
# ---------------------------------------------------------------------------


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
        # type(dialect).__name__ == "MSSQLDialect" olması gerekiyor;
        # MagicMock yerine gerçek bir sınıf kullanıyoruz
        class _FakeMSSQLDialect:
            __name__ = "MSSQLDialect"
            def quote_identifier(self, name):
                return f'"{name}"'

        _FakeMSSQLDialect.__name__ = "MSSQLDialect"
        dialect = _FakeMSSQLDialect()
        task = _task({"mode": "hash_mod", "parts": 2})
        result = Partitioner().plan(task, MagicMock(), dialect)
        for spec in result:
            assert "%" in spec["where"]


# ---------------------------------------------------------------------------
# distinct
# ---------------------------------------------------------------------------


class TestPartitionerDistinct:
    def test_distinct_groups_numeric_values(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        task = _task({"mode": "distinct", "parts": 2})
        result = Partitioner().plan(task, conn, _dialect())
        assert len(result) == 2
        assert "IN" in result[0]["where"]

    def test_distinct_string_values_quoted(self):
        conn = _conn(fetchall=[("US",), ("EU",)])
        task = _task({"mode": "distinct", "parts": 2})
        result = Partitioner().plan(task, conn, _dialect())
        # Her iki değer de tek tırnak içinde olmalı
        assert "'US'" in result[0]["where"] or "'EU'" in result[0]["where"]

    def test_distinct_empty_table_falls_back_to_single_partition(self):
        conn = _conn(fetchall=[])
        task = _task({"mode": "distinct", "parts": 4})
        result = Partitioner().plan(task, conn, _dialect())
        assert result == [{"part_id": 0, "where": None}]

    def test_distinct_fewer_values_than_parts(self):
        # 2 distinct değer, parts=4 → 2 spec (boş grup yok)
        conn = _conn(fetchall=[(10,), (20,)])
        task = _task({"mode": "distinct", "parts": 4})
        result = Partitioner().plan(task, conn, _dialect())
        assert len(result) == 2

    def test_distinct_part_ids_sequential(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        task = _task({"mode": "distinct", "parts": 2})
        result = Partitioner().plan(task, conn, _dialect())
        assert [s["part_id"] for s in result] == [0, 1]

    def test_distinct_limit_applies_pagination_limit(self):
        conn = _conn(fetchall=[(1,), (2,), (3,), (4,)])
        dialect = _dialect()
        task = _task({"mode": "distinct", "parts": 2, "distinct_limit": 2})
        Partitioner().plan(task, conn, dialect)
        dialect.get_pagination_query.assert_called_once()
        args = dialect.get_pagination_query.call_args.args
        assert args[1] == 2
        assert args[2] == 0

    def test_distinct_invalid_distinct_limit_raises_partition_error(self):
        conn = _conn(fetchall=[(1,), (2,)])
        task = _task({"mode": "distinct", "parts": 2, "distinct_limit": 0})
        with pytest.raises(PartitionError, match="distinct_limit"):
            Partitioner().plan(task, conn, _dialect())


# ---------------------------------------------------------------------------
# percentile
# ---------------------------------------------------------------------------


class TestPartitionerPercentile:
    def test_percentile_falls_back_to_auto_numeric_on_error(self):
        # MagicMock dialect → _query_percentiles NotImplementedError → auto_numeric fallback
        # auto_numeric için tek bir cursor yeterli
        cursor = MagicMock()
        cursor.fetchone.return_value = (1, 1000)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        task = _task({"mode": "percentile", "parts": 4})
        result = Partitioner().plan(task, conn, _dialect())
        assert len(result) == 4

    def test_percentile_query_attempted_before_fallback(self):
        # _query_percentiles çağrıldıktan sonra fallback'e geçiyor
        from unittest.mock import patch

        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 100)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        task = _task({"mode": "percentile", "parts": 2})
        with patch.object(
            Partitioner, "_query_percentiles", side_effect=Exception("forced")
        ) as mock_qp:
            result = Partitioner().plan(task, conn, _dialect())

        mock_qp.assert_called_once()   # _query_percentiles denendi
        assert len(result) == 2        # auto_numeric fallback başarılı
