"""Unit tests for MappingGenerator."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from ffengine.dialects.base import ColumnInfo
from ffengine.errors.exceptions import MappingError
from ffengine.mapping.generator import MappingGenerator
from ffengine.mapping.resolver import MappingResolver


def _make_dialect(class_name: str, cols=None):
    class _D:
        pass

    _D.__name__ = class_name
    _D.get_table_schema = lambda self, *a, **kw: (cols or [])
    return _D()


def _oracle_dialect(cols: list[ColumnInfo]):
    return _make_dialect("OracleDialect", cols)


def _postgres_dialect():
    return _make_dialect("PostgresDialect")


def _mssql_dialect(cols: list[ColumnInfo]):
    return _make_dialect("MSSQLDialect", cols)


def _conn():
    return MagicMock()


class TestMappingGenerator:
    def test_generate_returns_correct_keys(self):
        cols = [ColumnInfo("id", "NUMBER", precision=10)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "orders")
        assert "version" in result
        assert "source_dialect" in result
        assert "target_dialect" in result
        assert "columns" in result

    def test_generate_version_is_v1(self):
        src = _oracle_dialect([ColumnInfo("id", "NUMBER", precision=10)])
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["version"] == "v1"

    def test_generate_dialect_names(self):
        src = _oracle_dialect([ColumnInfo("id", "NUMBER", precision=10)])
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["source_dialect"] == "oracle"
        assert result["target_dialect"] == "postgres"

    def test_generate_column_order_preserved(self):
        cols = [
            ColumnInfo("col_c", "NUMBER", precision=10),
            ColumnInfo("col_a", "VARCHAR2", precision=140),
            ColumnInfo("col_b", "DATE"),
        ]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        names = [c["source_name"] for c in result["columns"]]
        assert names == ["col_c", "col_a", "col_b"]

    def test_generate_source_type_is_parameterized(self):
        cols = [ColumnInfo("amount", "NUMBER", precision=18, scale=4)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["columns"][0]["source_type"] == "NUMBER(18,4)"

    def test_generate_target_type_translated_with_params(self):
        cols = [ColumnInfo("status", "VARCHAR2", precision=140)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["columns"][0]["target_type"] == "VARCHAR(140)"

    def test_generate_postgres_character_varying_length_is_preserved(self):
        cols = [ColumnInfo("account_no", "CHARACTER VARYING", precision=64)]
        src = _postgres_dialect()
        src.get_table_schema = lambda *args, **kwargs: cols
        tgt = _postgres_dialect()

        result = MappingGenerator().generate(_conn(), src, tgt, "ocn_iss", "accounts")

        assert result["columns"][0]["source_type"] == "CHARACTER VARYING(64)"
        assert result["columns"][0]["target_type"] == "VARCHAR(64)"

    def test_generate_mssql_to_oracle_char_length_is_preserved(self):
        cols = [ColumnInfo("iata_code", "CHAR", precision=3)]
        src = _mssql_dialect(cols)
        tgt = _oracle_dialect([])
        result = MappingGenerator().generate(_conn(), src, tgt, "dbo", "airports")
        assert result["columns"][0]["source_type"] == "CHAR(3)"
        assert result["columns"][0]["target_type"] == "CHAR(3)"

    def test_generate_unparameterized_length_target_rejected_by_strict(self):
        cols = [ColumnInfo("iata_code", "CHAR")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="requires explicit length"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t")

    def test_generate_unparameterized_numeric_target_rejected_by_strict(self):
        cols = [ColumnInfo("amount", "NUMBER")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="requires explicit precision/scale"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t")

    def test_generate_cross_dialect_lenient_blanks_bare_numeric(self):
        # strict=False, different Connection Type: an unsized numeric is blanked
        # for the developer to fill (no guessed precision/scale).
        cols = [ColumnInfo("amount", "NUMBER")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(
            _conn(), src, tgt, "public", "t", strict=False
        )
        assert result["columns"][0]["target_type"] == ""

    def test_generate_same_dialect_lenient_keeps_bare_numeric(self):
        # strict=False, same Connection Type: bare numeric is a lossless max-size
        # passthrough; kept as-is, no raise.
        cols = [ColumnInfo("amount", "NUMBER")]
        src = _oracle_dialect(cols)
        tgt = _oracle_dialect([])
        result = MappingGenerator().generate(
            _conn(), src, tgt, "public", "t", strict=False
        )
        row = result["columns"][0]
        assert row["target_name"] == "amount"
        from ffengine.mapping.type_contract import parse_type

        _base, params = parse_type(row["target_type"])
        assert params is None

    def test_generate_unsupported_type_raises_mapping_error(self):
        cols = [ColumnInfo("col1", "XMLTYPE")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="col1"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t")

    def test_generate_same_dialect_lenient_unmapped_type_identity(self):
        # strict=False, same Connection Type: an un-cross-mappable type (Postgres
        # array) is copied through as identity, lossless, no raise.
        src = _make_dialect("PostgresDialect", [ColumnInfo("flow_steps", "TEXT[]")])
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(
            _conn(), src, tgt, "public", "t", strict=False
        )
        assert result["columns"][0]["target_type"] == "TEXT[]"

    def test_generate_invalid_version_raises_mapping_error(self):
        src = _oracle_dialect([ColumnInfo("id", "NUMBER", precision=10)])
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="versiyonu"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t", version="v99")

    def test_save_writes_valid_yaml(self, tmp_path):
        cols = [ColumnInfo("id", "NUMBER", precision=10)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        gen = MappingGenerator()
        mapping = gen.generate(_conn(), src, tgt, "public", "t")
        path = str(tmp_path / "out.yaml")
        gen.save(mapping, path)

        with open(path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
        assert loaded["version"] == "v1"
        assert loaded["columns"][0]["source_name"] == "id"

    def test_save_nonexistent_directory_raises_mapping_error(self, tmp_path):
        cols = [ColumnInfo("id", "NUMBER", precision=10)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        gen = MappingGenerator()
        mapping = gen.generate(_conn(), src, tgt, "public", "t")
        with pytest.raises(MappingError, match="dizin"):
            gen.save(mapping, str(tmp_path / "nonexistent" / "out.yaml"))

    def test_save_preserves_column_order(self, tmp_path):
        cols = [
            ColumnInfo("z_col", "NUMBER", precision=10),
            ColumnInfo("a_col", "VARCHAR2", precision=10),
        ]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        gen = MappingGenerator()
        mapping = gen.generate(_conn(), src, tgt, "public", "t")
        path = str(tmp_path / "ordered.yaml")
        gen.save(mapping, path)

        with open(path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
        assert [c["source_name"] for c in loaded["columns"]] == ["z_col", "a_col"]

    def test_roundtrip_generate_save_resolve(self, tmp_path):
        cols = [
            ColumnInfo("order_id", "NUMBER", precision=10),
            ColumnInfo("amount", "NUMBER", precision=18, scale=4),
        ]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        gen = MappingGenerator()
        mapping = gen.generate(_conn(), src, tgt, "public", "orders")
        path = str(tmp_path / "orders.yaml")
        gen.save(mapping, path)

        task = {
            "column_mapping_mode": "mapping_file",
            "mapping_file": path,
            "source_schema": "public",
            "source_table": "orders",
        }
        result = MappingResolver().resolve(task, _conn(), src, tgt)
        assert result.source_columns == ["order_id", "amount"]
        assert result.target_columns == ["order_id", "amount"]
        assert len(result.target_columns_meta) == 2
