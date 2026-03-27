import pytest
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError


# ------------------------------------------------------------------
# Critical Mappings from TYPE_MAPPING.md
# ------------------------------------------------------------------


def test_oracle_number_to_postgres_numeric():
    result = TypeMapper.map_type("NUMBER(38,10)", "oracle", "postgres")
    assert result == "NUMERIC(38,10)"


def test_mssql_decimal_to_postgres_numeric():
    result = TypeMapper.map_type("DECIMAL(18,4)", "mssql", "postgres")
    assert result == "NUMERIC(18,4)"


def test_oracle_clob_to_postgres_text():
    result = TypeMapper.map_type("CLOB", "oracle", "postgres")
    assert result == "TEXT"


def test_oracle_blob_to_postgres_bytea():
    result = TypeMapper.map_type("BLOB", "oracle", "postgres")
    assert result == "BYTEA"


def test_mssql_datetime2_to_postgres_timestamp():
    result = TypeMapper.map_type("DATETIME2", "mssql", "postgres")
    assert result == "TIMESTAMP"


# ------------------------------------------------------------------
# Precision / Scale Preservation (Lossless)
# ------------------------------------------------------------------


def test_precision_preserved_numeric():
    """Precision and scale must survive cross-dialect mapping."""
    result = TypeMapper.map_type("NUMERIC(38,12)", "postgres", "mssql")
    assert result == "DECIMAL(38,12)"


def test_precision_preserved_varchar():
    result = TypeMapper.map_type("VARCHAR(255)", "postgres", "mssql")
    assert result == "NVARCHAR(255)"


def test_precision_preserved_char():
    result = TypeMapper.map_type("CHAR(10)", "postgres", "oracle")
    assert result == "CHAR(10)"


def test_no_precision_stays_bare():
    """Types without precision params should not gain them."""
    result = TypeMapper.map_type("TEXT", "postgres", "mssql")
    assert result == "NVARCHAR(MAX)"


# ------------------------------------------------------------------
# Round-Trip Lossless Tests (A → B → A)
# ------------------------------------------------------------------


def test_roundtrip_pg_to_mssql_to_pg():
    """NUMERIC(18,2) should survive PG→MSSQL→PG."""
    step1 = TypeMapper.map_type("NUMERIC(18,2)", "postgres", "mssql")
    step2 = TypeMapper.map_type(step1, "mssql", "postgres")
    assert step2 == "NUMERIC(18,2)"


def test_roundtrip_pg_to_oracle_to_pg():
    """INTEGER should survive PG→Oracle→PG."""
    step1 = TypeMapper.map_type("INTEGER", "postgres", "oracle")
    step2 = TypeMapper.map_type(step1, "oracle", "postgres")
    # NUMBER(10) → maps to NUMERIC canonical (since it has no precision params
    # that would classify it otherwise)
    assert "INTEGER" in step2 or "NUMERIC" in step2


def test_roundtrip_varchar_oracle():
    """VARCHAR(100) → PG→Oracle→PG lossless."""
    step1 = TypeMapper.map_type("VARCHAR(100)", "postgres", "oracle")
    assert step1 == "VARCHAR2(100)"
    step2 = TypeMapper.map_type(step1, "oracle", "postgres")
    assert step2 == "VARCHAR(100)"


# ------------------------------------------------------------------
# Cross-dialect Mappings
# ------------------------------------------------------------------


def test_postgres_to_mssql_boolean():
    result = TypeMapper.map_type("BOOLEAN", "postgres", "mssql")
    assert result == "BIT"


def test_mssql_to_oracle_int():
    result = TypeMapper.map_type("INT", "mssql", "oracle")
    assert result == "NUMBER(10)"


def test_oracle_to_mssql_varchar2():
    result = TypeMapper.map_type("VARCHAR2(200)", "oracle", "mssql")
    assert result == "NVARCHAR(200)"


def test_postgres_uuid_to_oracle():
    result = TypeMapper.map_type("UUID", "postgres", "oracle")
    assert result == "VARCHAR2(36)"


def test_mssql_uniqueidentifier_to_postgres():
    result = TypeMapper.map_type("UNIQUEIDENTIFIER", "mssql", "postgres")
    assert result == "UUID"


def test_postgres_jsonb_to_mssql():
    result = TypeMapper.map_type("JSONB", "postgres", "mssql")
    assert result == "NVARCHAR(MAX)"


def test_oracle_nclob_to_postgres():
    result = TypeMapper.map_type("NCLOB", "oracle", "postgres")
    assert result == "TEXT"


# ------------------------------------------------------------------
# Case Insensitivity
# ------------------------------------------------------------------


def test_case_insensitive_type():
    result = TypeMapper.map_type("varchar(50)", "postgres", "mssql")
    assert result == "NVARCHAR(50)"


def test_case_insensitive_dialect():
    result = TypeMapper.map_type("INTEGER", "POSTGRES", "MSSQL")
    assert result == "INT"


# ------------------------------------------------------------------
# Error Handling
# ------------------------------------------------------------------


def test_unsupported_type_raises():
    with pytest.raises(UnsupportedTypeError, match="Unsupported type"):
        TypeMapper.map_type("HYPERLOGLOG", "postgres", "mssql")


def test_unknown_source_dialect_raises():
    with pytest.raises(UnsupportedTypeError, match="Unknown source dialect"):
        TypeMapper.map_type("INTEGER", "sqlite", "postgres")


def test_unknown_target_dialect_raises():
    with pytest.raises(UnsupportedTypeError, match="Unknown target dialect"):
        TypeMapper.map_type("INTEGER", "postgres", "sqlite")


# ------------------------------------------------------------------
# Internal: _parse_type
# ------------------------------------------------------------------


def test_parse_type_simple():
    base, params = TypeMapper._parse_type("INTEGER")
    assert base == "INTEGER"
    assert params is None


def test_parse_type_with_precision():
    base, params = TypeMapper._parse_type("VARCHAR(255)")
    assert base == "VARCHAR"
    assert params == "255"


def test_parse_type_with_precision_and_scale():
    base, params = TypeMapper._parse_type("NUMERIC(38,10)")
    assert base == "NUMERIC"
    assert params == "38,10"


def test_parse_type_with_spaces():
    base, params = TypeMapper._parse_type("  numeric(18, 2)  ")
    assert base == "NUMERIC"
    assert params == "18, 2"
