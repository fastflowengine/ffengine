"""Unit tests for Flow Studio mapping strictness and SQL metadata extraction."""

from __future__ import annotations

import textwrap

import pytest

from ffengine.ui import studio_service as ss


class _Cursor:
    def __init__(self, *, rows=None, description=None, execute_exc: Exception | None = None):
        self._rows = list(rows or [])
        self.description = description
        self._execute_exc = execute_exc
        self.executed_query = None
        self.executed_params = None
        self.closed = False

    def execute(self, query, params=None):
        self.executed_query = query
        self.executed_params = params
        if self._execute_exc is not None:
            raise self._execute_exc

    def fetchall(self):
        return list(self._rows)

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor

    def cursor(self, server_side=False):
        return self._cursor


class _DescriptionColumn:
    def __init__(
        self,
        name,
        type_code,
        *,
        type_display=None,
        display_size=None,
        precision=None,
        scale=None,
    ):
        self.name = name
        self.type_code = type_code
        self.type_display = type_display
        self.display_size = display_size
        self.internal_size = None
        self.precision = precision
        self.scale = scale
        self.null_ok = None


def _dialect(class_name: str):
    class _D:
        pass

    _D.__name__ = class_name
    return _D()


def test_parse_yaml_mapping_rejects_paramless_length_target_type():
    content = textwrap.dedent(
        """\
        version: v1
        columns:
          - source_name: IATA_CODE
            target_name: IATA_CODE
            source_type: CHAR(3)
            target_type: CHAR
            nullable: false
        """
    )
    with pytest.raises(ValueError, match="requires explicit length"):
        ss._parse_yaml_mapping_text(content, label="mapping/test.yaml")


def test_parse_yaml_mapping_accepts_parameterized_types():
    content = textwrap.dedent(
        """\
        version: v1
        columns:
          - source_name: IATA_CODE
            target_name: IATA_CODE
            source_type: CHAR(3)
            target_type: CHAR(3)
            nullable: false
          - source_name: AIRPORT_NAME
            target_name: AIRPORT_NAME
            source_type: NVARCHAR(140)
            target_type: VARCHAR2(140)
            nullable: true
        """
    )
    parsed = ss._parse_yaml_mapping_text(content, label="mapping/test.yaml")
    assert parsed["version"] == "v1"
    assert len(parsed["columns"]) == 2


def test_build_mapping_from_columns_preserves_mssql_lengths_for_oracle_target():
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[
            {
                "name": "IATA_CODE",
                "source_type": "CHAR(3)",
                "source_length": 3,
                "nullable": False,
            },
            {
                "name": "AIRPORT_NAME",
                "source_type": "NVARCHAR(140)",
                "source_length": 140,
                "nullable": True,
            },
        ],
        src_dialect_name="mssql",
        tgt_dialect_name="oracle",
    )
    assert warnings == []
    rows = mapping_obj["columns"]
    assert rows[0]["target_type"] == "CHAR(3)"
    assert rows[1]["target_type"] == "VARCHAR2(140)"


def test_build_mapping_from_columns_uses_bounded_fallback_for_unbounded_source():
    mapping_obj, _warnings = ss._build_mapping_from_columns(
        columns=[
            {
                "name": "NOTES",
                "source_type": "NVARCHAR",
                "source_length": None,
                "nullable": True,
            }
        ],
        src_dialect_name="mssql",
        tgt_dialect_name="oracle",
    )
    assert mapping_obj["columns"][0]["target_type"] == "VARCHAR2(4000)"


def test_build_mapping_from_columns_fail_fast_for_unsupported_source_type():
    with pytest.raises(ValueError, match="cannot be mapped"):
        ss._build_mapping_from_columns(
            columns=[{"name": "COL_X", "source_type": "CUSTOM_UNMAPPED_TYPE"}],
            src_dialect_name="mssql",
            tgt_dialect_name="oracle",
        )


def test_extract_sql_select_columns_mssql_returns_parameterized_source_types():
    rows = [
        (1, "IATA_CODE", "char(3)", False, 3, None, None, None, None),
        (2, "AIRPORT_NAME", "nvarchar(140)", True, 140, None, None, None, None),
        (3, "AMOUNT", "decimal(12,2)", True, None, 12, 2, None, None),
        (4, "NOTES", "nvarchar(max)", True, -1, None, None, None, None),
    ]
    cursor = _Cursor(rows=rows)
    session = _Session(cursor)
    mssql = _dialect("MSSQLDialect")

    cols = ss.extract_sql_select_columns(session, mssql, "SELECT * FROM dbo.airports")

    assert [c["name"] for c in cols] == ["IATA_CODE", "AIRPORT_NAME", "AMOUNT", "NOTES"]
    assert cols[0]["source_type"] == "CHAR(3)"
    assert cols[1]["source_type"] == "NVARCHAR(140)"
    assert cols[2]["source_type"] == "DECIMAL(12,2)"
    assert cols[3]["source_type"] == "NVARCHAR"
    assert cursor.closed is True


def test_extract_sql_select_columns_mssql_fail_fast_on_describe_error_row():
    rows = [
        (None, None, None, None, None, None, None, 102, "Incorrect syntax near 'FROM'.")
    ]
    cursor = _Cursor(rows=rows)
    session = _Session(cursor)
    mssql = _dialect("MSSQLDialect")

    with pytest.raises(ValueError, match="SQL metadata extraction failed"):
        ss.extract_sql_select_columns(session, mssql, "SELECT FROM")


def test_extract_sql_select_columns_postgres_uses_type_display_and_oid_metadata():
    cursor = _Cursor(
        description=[
            _DescriptionColumn(
                "txn_guid",
                1700,
                type_display="numeric(16,0)",
                precision=16,
                scale=0,
            ),
            _DescriptionColumn(
                "cavv_data",
                1043,
                type_display="varchar(48)",
                display_size=48,
            ),
        ]
    )
    session = _Session(cursor)
    postgres = _dialect("PostgresDialect")

    cols = ss.extract_sql_select_columns(session, postgres, "SELECT * FROM t")

    assert cols[0]["source_type"] == "NUMERIC(16,0)"
    assert cols[1]["source_type"] == "VARCHAR(48)"
    assert cursor.executed_query == (
        "SELECT * FROM (SELECT * FROM t) AS ffengine_inline_sql LIMIT 0"
    )


def test_extract_sql_select_columns_postgres_requires_numeric_expression_precision():
    cursor = _Cursor(
        description=[
            _DescriptionColumn(
                "insert_date",
                1700,
                type_display="numeric",
            )
        ]
    )
    session = _Session(cursor)
    postgres = _dialect("PostgresDialect")

    with pytest.raises(ValueError, match="::numeric\\(8,0\\)"):
        ss.extract_sql_select_columns(session, postgres, "SELECT current_date")
