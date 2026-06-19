"""Unit tests for MappingResolver."""

from __future__ import annotations

import textwrap
from unittest.mock import MagicMock

import pytest

from ffengine.dialects.base import ColumnInfo
from ffengine.errors.exceptions import MappingError
from ffengine.mapping.resolver import MappingResolver, MappingResult, _dialect_name


def _make_dialect(class_name: str, cols: list[ColumnInfo] | None = None):
    class _D:
        pass

    _D.__name__ = class_name
    _D.get_table_schema = lambda self, *a, **kw: (cols or [])
    return _D()


def _src_dialect(cols: list[ColumnInfo], dialect_class: str = "PostgresDialect"):
    return _make_dialect(dialect_class, cols)


def _tgt_dialect(dialect_class: str = "PostgresDialect"):
    return _make_dialect(dialect_class)


def _conn():
    return MagicMock()


def _task(**overrides) -> dict:
    base = {
        "source_schema": "public",
        "source_table": "orders",
        "column_mapping_mode": "source",
        "passthrough_full": True,
        "source_columns": None,
    }
    base.update(overrides)
    return base


class TestDialectNameHelper:
    def test_postgres_dialect(self):
        class PostgresDialect:
            pass

        assert _dialect_name(PostgresDialect()) == "postgres"

    def test_mssql_dialect(self):
        class MSSQLDialect:
            pass

        assert _dialect_name(MSSQLDialect()) == "mssql"

    def test_oracle_dialect(self):
        class OracleDialect:
            pass

        assert _dialect_name(OracleDialect()) == "oracle"

    def test_unknown_dialect_returns_class_name(self):
        class FooBar:
            pass

        assert _dialect_name(FooBar()) == "foobar"


class TestMappingResolverSourceMode:
    def test_passthrough_full_returns_all_columns(self):
        cols = [
            ColumnInfo("id", "INTEGER"),
            ColumnInfo("name", "VARCHAR"),
            ColumnInfo("age", "SMALLINT"),
        ]
        src = _src_dialect(cols)
        tgt = _tgt_dialect()
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert isinstance(result, MappingResult)
        assert result.source_columns == ["id", "name", "age"]
        assert result.target_columns == ["id", "name", "age"]
        assert len(result.target_columns_meta) == 3

    def test_passthrough_full_translates_types(self):
        cols = [ColumnInfo("amount", "NUMBER", precision=10, scale=2)]
        src = _src_dialect(cols, "OracleDialect")
        tgt = _tgt_dialect("PostgresDialect")
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert result.target_columns_meta[0].data_type == "NUMERIC"

    def test_passthrough_full_preserves_column_order(self):
        cols = [
            ColumnInfo("c", "INTEGER"),
            ColumnInfo("a", "INTEGER"),
            ColumnInfo("b", "INTEGER"),
        ]
        src = _src_dialect(cols)
        tgt = _tgt_dialect()
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert result.source_columns == ["c", "a", "b"]

    def test_integer_precision_not_carried_to_mssql(self):
        cols = [ColumnInfo("id", "INTEGER", False, 32, 0)]
        src = _src_dialect(cols, "PostgresDialect")
        tgt = _tgt_dialect("MSSQLDialect")
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert result.target_columns_meta[0].data_type == "INT"
        assert result.target_columns_meta[0].precision is None
        assert result.target_columns_meta[0].scale is None

    def test_mssql_decimal_precision_overflow_raises(self):
        cols = [ColumnInfo("amount", "NUMERIC", True, 65, 10)]
        src = _src_dialect(cols, "PostgresDialect")
        tgt = _tgt_dialect("MSSQLDialect")
        with pytest.raises(MappingError, match="precision limiti"):
            MappingResolver().resolve(_task(), _conn(), src, tgt)

    def test_mssql_to_oracle_varchar_char_length_preserved(self):
        cols = [
            ColumnInfo("city", "NVARCHAR", True, 120, None),
            ColumnInfo("code", "CHAR", False, 3, None),
        ]
        src = _src_dialect(cols, "MSSQLDialect")
        tgt = _tgt_dialect("OracleDialect")
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert result.target_columns_meta[0].data_type == "VARCHAR2"
        assert result.target_columns_meta[0].precision == 120
        assert result.target_columns_meta[0].scale is None
        assert result.target_columns_meta[1].data_type == "CHAR"
        assert result.target_columns_meta[1].precision == 3
        assert result.target_columns_meta[1].scale is None

    def test_mssql_to_oracle_invalid_length_falls_back_to_none(self):
        cols = [
            ColumnInfo("free_text", "NVARCHAR", True, -1, None),
            ColumnInfo("blob_hint", "VARCHAR", True, 0, None),
        ]
        src = _src_dialect(cols, "MSSQLDialect")
        tgt = _tgt_dialect("OracleDialect")
        result = MappingResolver().resolve(_task(), _conn(), src, tgt)
        assert result.target_columns_meta[0].data_type == "VARCHAR2"
        assert result.target_columns_meta[0].precision is None
        assert result.target_columns_meta[0].scale is None
        assert result.target_columns_meta[1].data_type == "VARCHAR2"
        assert result.target_columns_meta[1].precision is None
        assert result.target_columns_meta[1].scale is None

    def test_passthrough_partial_filters_columns(self):
        cols = [
            ColumnInfo("id", "INTEGER"),
            ColumnInfo("name", "VARCHAR"),
            ColumnInfo("secret", "TEXT"),
        ]
        src = _src_dialect(cols)
        tgt = _tgt_dialect()
        task = _task(passthrough_full=False, source_columns=["id", "name"])
        result = MappingResolver().resolve(task, _conn(), src, tgt)
        assert result.source_columns == ["id", "name"]
        assert "secret" not in result.source_columns

    def test_passthrough_partial_missing_column_raises(self):
        cols = [ColumnInfo("id", "INTEGER")]
        src = _src_dialect(cols)
        tgt = _tgt_dialect()
        task = _task(passthrough_full=False, source_columns=["nonexistent"])
        with pytest.raises(MappingError, match="nonexistent"):
            MappingResolver().resolve(task, _conn(), src, tgt)

    def test_unsupported_type_raises_mapping_error(self):
        cols = [ColumnInfo("col1", "XMLTYPE")]
        src = _src_dialect(cols, "OracleDialect")
        tgt = _tgt_dialect("PostgresDialect")
        with pytest.raises(MappingError, match="col1"):
            MappingResolver().resolve(_task(), _conn(), src, tgt)

    def test_get_table_schema_exception_raises_mapping_error(self):
        src = MagicMock()
        src.get_table_schema.side_effect = Exception("db baglanti hatasi")
        tgt = _tgt_dialect()
        with pytest.raises(MappingError, match="semasi"):
            MappingResolver().resolve(_task(), _conn(), src, tgt)

    def test_empty_source_metadata_raises_mapping_error(self):
        src = _src_dialect([])
        tgt = _tgt_dialect()
        with pytest.raises(MappingError, match="semasi bos"):
            MappingResolver().resolve(_task(), _conn(), src, tgt)


_VALID_MAPPING_YAML = textwrap.dedent(
    """\
    version: "v1"
    source_dialect: oracle
    target_dialect: postgres
    columns:
      - source_name: ORDER_ID
        target_name: order_id
        source_type: "NUMBER(10)"
        target_type: INTEGER
        nullable: false
      - source_name: ORDER_AMT
        target_name: order_amt
        source_type: "NUMBER(18,4)"
        target_type: "NUMERIC(18,4)"
        nullable: true
"""
)


class TestMappingResolverMappingFileMode:
    def test_loads_columns_correctly(self, tmp_path):
        p = tmp_path / "mapping.yaml"
        p.write_text(_VALID_MAPPING_YAML, encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        result = MappingResolver().resolve(
            task, _conn(), _src_dialect([]), _tgt_dialect()
        )
        assert result.source_columns == ["ORDER_ID", "ORDER_AMT"]
        assert result.target_columns == ["order_id", "order_amt"]

    def test_column_rename_reflected(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text(_VALID_MAPPING_YAML, encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        result = MappingResolver().resolve(
            task, _conn(), _src_dialect([]), _tgt_dialect()
        )
        assert result.source_columns[0] == "ORDER_ID"
        assert result.target_columns[0] == "order_id"

    def test_target_type_from_file(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text(_VALID_MAPPING_YAML, encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        result = MappingResolver().resolve(
            task, _conn(), _src_dialect([]), _tgt_dialect()
        )
        assert result.target_columns_meta[0].data_type == "INTEGER"
        assert result.target_columns_meta[1].data_type == "NUMERIC(18,4)"

    def test_nullable_false_preserved(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text(_VALID_MAPPING_YAML, encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        result = MappingResolver().resolve(
            task, _conn(), _src_dialect([]), _tgt_dialect()
        )
        assert result.target_columns_meta[0].nullable is False

    def test_nullable_defaults_to_true(self, tmp_path):
        yaml_no_nullable = textwrap.dedent(
            """\
            version: "v1"
            columns:
              - source_name: col1
                target_name: col1
                source_type: INTEGER
                target_type: INTEGER
        """
        )
        p = tmp_path / "m.yaml"
        p.write_text(yaml_no_nullable, encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        result = MappingResolver().resolve(
            task, _conn(), _src_dialect([]), _tgt_dialect()
        )
        assert result.target_columns_meta[0].nullable is True

    def test_missing_file_raises_mapping_error(self, tmp_path):
        task = _task(
            column_mapping_mode="mapping_file", mapping_file=str(tmp_path / "nope.yaml")
        )
        with pytest.raises(MappingError, match="bulunamadi"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())

    def test_invalid_yaml_raises_mapping_error(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(": bad\n  broken:", encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        with pytest.raises(MappingError, match="YAML"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())

    def test_missing_version_raises_mapping_error(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text("columns: []\n", encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        with pytest.raises(MappingError, match="version"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())

    def test_unknown_version_raises_mapping_error(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text("version: v99\ncolumns: []\n", encoding="utf-8")
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        with pytest.raises(MappingError, match="version"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())

    def test_mapping_file_rejects_paramless_length_target_type(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text(
            textwrap.dedent(
                """\
                version: "v1"
                columns:
                  - source_name: IATA_CODE
                    target_name: IATA_CODE
                    source_type: CHAR(3)
                    target_type: CHAR
                    nullable: false
                """
            ),
            encoding="utf-8",
        )
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        with pytest.raises(MappingError, match="requires explicit length"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())

    def test_mapping_file_rejects_paramless_numeric_target_type(self, tmp_path):
        p = tmp_path / "m.yaml"
        p.write_text(
            textwrap.dedent(
                """\
                version: "v1"
                columns:
                  - source_name: AMOUNT
                    target_name: AMOUNT
                    source_type: NUMERIC(18,4)
                    target_type: NUMBER
                    nullable: true
                """
            ),
            encoding="utf-8",
        )
        task = _task(column_mapping_mode="mapping_file", mapping_file=str(p))
        with pytest.raises(MappingError, match="requires explicit precision/scale"):
            MappingResolver().resolve(task, _conn(), _src_dialect([]), _tgt_dialect())


class TestMappingResolverDispatch:
    def test_source_mode_dispatched(self):
        cols = [ColumnInfo("id", "INTEGER")]
        src = _src_dialect(cols)
        tgt = _tgt_dialect()
        result = MappingResolver().resolve(
            _task(column_mapping_mode="source"), _conn(), src, tgt
        )
        assert isinstance(result, MappingResult)

    def test_unknown_mode_raises_mapping_error(self):
        with pytest.raises(MappingError, match="column_mapping_mode"):
            MappingResolver().resolve(
                _task(column_mapping_mode="custom_mode"),
                _conn(),
                _src_dialect([]),
                _tgt_dialect(),
            )
