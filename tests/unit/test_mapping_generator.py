"""
C09 — MappingGenerator birim testleri.

Kapsam: generate yapısı, kolon sırası, tür çevirisi, kaydetme, hata senaryoları,
        generate→save→resolve roundtrip.
"""

import yaml
import pytest
from unittest.mock import MagicMock

from ffengine.mapping.generator import MappingGenerator
from ffengine.mapping.resolver import MappingResolver
from ffengine.dialects.base import ColumnInfo
from ffengine.errors.exceptions import MappingError


# ---------------------------------------------------------------------------
# Yardımcılar
# ---------------------------------------------------------------------------


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


def _conn():
    return MagicMock()


# ---------------------------------------------------------------------------
# MappingGenerator testleri
# ---------------------------------------------------------------------------


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
        src = _oracle_dialect([ColumnInfo("id", "NUMBER")])
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["version"] == "v1"

    def test_generate_dialect_names(self):
        src = _oracle_dialect([ColumnInfo("id", "NUMBER")])
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["source_dialect"] == "oracle"
        assert result["target_dialect"] == "postgres"

    def test_generate_column_order_preserved(self):
        cols = [
            ColumnInfo("col_c", "NUMBER"),
            ColumnInfo("col_a", "VARCHAR2"),
            ColumnInfo("col_b", "DATE"),
        ]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        names = [c["source_name"] for c in result["columns"]]
        assert names == ["col_c", "col_a", "col_b"]

    def test_generate_source_type_preserved(self):
        cols = [ColumnInfo("amount", "NUMBER", precision=18, scale=4)]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        assert result["columns"][0]["source_type"] == "NUMBER"

    def test_generate_target_type_translated(self):
        cols = [ColumnInfo("status", "VARCHAR2")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        result = MappingGenerator().generate(_conn(), src, tgt, "public", "t")
        # Oracle VARCHAR2 → canonical VARCHAR → Postgres VARCHAR
        assert result["columns"][0]["target_type"] == "VARCHAR"

    def test_generate_unsupported_type_raises_mapping_error(self):
        cols = [ColumnInfo("col1", "XMLTYPE")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="col1"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t")

    def test_generate_invalid_version_raises_mapping_error(self):
        src = _oracle_dialect([ColumnInfo("id", "NUMBER")])
        tgt = _postgres_dialect()
        with pytest.raises(MappingError, match="versiyon"):
            MappingGenerator().generate(_conn(), src, tgt, "public", "t", version="v99")

    def test_save_writes_valid_yaml(self, tmp_path):
        cols = [ColumnInfo("id", "NUMBER")]
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
        cols = [ColumnInfo("id", "NUMBER")]
        src = _oracle_dialect(cols)
        tgt = _postgres_dialect()
        gen = MappingGenerator()
        mapping = gen.generate(_conn(), src, tgt, "public", "t")
        with pytest.raises(MappingError, match="dizin"):
            gen.save(mapping, str(tmp_path / "nonexistent" / "out.yaml"))

    def test_save_preserves_column_order(self, tmp_path):
        cols = [ColumnInfo("z_col", "NUMBER"), ColumnInfo("a_col", "VARCHAR2")]
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
        """generate → save → MappingResolver(mapping_file) roundtrip."""
        cols = [
            ColumnInfo("order_id", "NUMBER"),
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
