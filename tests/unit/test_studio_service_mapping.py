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


def test_build_mapping_from_columns_cross_dialect_unbounded_source_is_blanked():
    # Cross Connection Type + unsized source: FFEngine no longer guesses a
    # bounded fallback; it leaves the target Data Type blank for the developer
    # to fill (Apply/Save block on empty).
    mapping_obj, warnings = ss._build_mapping_from_columns(
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
        strict=False,
    )
    assert mapping_obj["columns"][0]["target_type"] == ""
    assert any("NOTES" in w for w in warnings)


def test_build_mapping_from_columns_fail_fast_for_unsupported_source_type():
    with pytest.raises(ValueError, match="cannot be mapped"):
        ss._build_mapping_from_columns(
            columns=[{"name": "COL_X", "source_type": "CUSTOM_UNMAPPED_TYPE"}],
            src_dialect_name="mssql",
            tgt_dialect_name="oracle",
        )


def test_build_mapping_from_columns_same_dialect_keeps_bare_numeric_strict():
    # Same Connection Type: an unsized numeric is a lossless "max size"
    # passthrough (non-narrowing), so it stays bare and passes even the strict
    # gate (strict=True default).
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[{"name": "amount", "source_type": "NUMERIC", "nullable": True}],
        src_dialect_name="postgres",
        tgt_dialect_name="postgres",
    )
    row = mapping_obj["columns"][0]
    assert row["target_name"] == "amount"
    assert row["target_type"] == "NUMERIC"
    assert warnings == []


def test_build_mapping_from_columns_cross_dialect_rejects_bare_numeric_strict():
    # Different Connection Type: "bare" does not mean max on every DB, so the
    # strict gate still fails loud (the target would be blanked, and an empty
    # target_type is rejected).
    with pytest.raises(ValueError):
        ss._build_mapping_from_columns(
            columns=[{"name": "amount", "source_type": "NUMERIC", "nullable": True}],
            src_dialect_name="postgres",
            tgt_dialect_name="oracle",
        )


def test_build_mapping_from_columns_same_dialect_lenient_keeps_bare_numeric():
    # Lenient scaffold, same dialect: bare numeric preserved as max-size.
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[{"name": "amount", "source_type": "NUMERIC", "nullable": True}],
        src_dialect_name="postgres",
        tgt_dialect_name="postgres",
        strict=False,
    )
    row = mapping_obj["columns"][0]
    assert row["target_name"] == "amount"
    from ffengine.mapping.type_contract import parse_type

    _base, params = parse_type(row["target_type"])
    assert params is None
    assert warnings == []


def test_build_mapping_from_columns_cross_dialect_lenient_blanks_bare_numeric():
    # Lenient scaffold, different dialect: unsized numeric is blanked for the
    # developer to fill, with a warning.
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[{"name": "amount", "source_type": "NUMERIC", "nullable": True}],
        src_dialect_name="postgres",
        tgt_dialect_name="oracle",
        strict=False,
    )
    assert mapping_obj["columns"][0]["target_type"] == ""
    assert any("amount" in w for w in warnings)


def test_build_mapping_from_columns_same_dialect_unmapped_type_identity():
    # Same dialect + a type TypeMapper cannot cross-map (Postgres array): copy
    # the source type through (identity), lossless, no raise, no blank.
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[{"name": "flow_steps", "source_type": "TEXT[]", "nullable": True}],
        src_dialect_name="postgres",
        tgt_dialect_name="postgres",
        strict=False,
    )
    assert mapping_obj["columns"][0]["target_type"] == "TEXT[]"
    assert warnings == []


def test_build_mapping_from_columns_cross_dialect_unmapped_type_blanked():
    # Different dialect + un-cross-mappable type: blank + warning (developer fills).
    mapping_obj, warnings = ss._build_mapping_from_columns(
        columns=[{"name": "flow_steps", "source_type": "TEXT[]", "nullable": True}],
        src_dialect_name="postgres",
        tgt_dialect_name="oracle",
        strict=False,
    )
    assert mapping_obj["columns"][0]["target_type"] == ""
    assert any("flow_steps" in w for w in warnings)


def test_incomplete_type_warnings_flags_only_blank_target_types():
    # Only a blank target_type (FFEngine could not fill it) is flagged. A bare
    # numeric/length is a valid same-dialect "max size" passthrough, not a draft
    # gap, so it must NOT warn.
    warns = ss._incomplete_type_warnings(
        [
            {"target_name": "flow_steps", "source_type": "TEXT[]", "target_type": ""},
            {"target_name": "amount", "target_type": "numeric"},
            {"target_name": "note", "target_type": "varchar"},
            {"target_name": "ok_num", "target_type": "numeric(18,2)"},
            {"target_name": "id", "target_type": "integer"},
        ]
    )
    joined = " | ".join(warns)
    assert "flow_steps" in joined and "TEXT[]" in joined  # blank -> warned
    assert "amount" not in joined  # bare numeric (max size) -> no warning
    assert "note" not in joined  # bare varchar (max size) -> no warning
    assert "ok_num" not in joined  # parameterized -> no warning
    assert "id" not in joined  # not a param-bearing type -> no warning


_DIALECTLESS_BARE_NUMERIC_YAML = textwrap.dedent(
    """\
    version: v1
    columns:
      - source_name: amount
        target_name: amount
        source_type: NUMERIC
        target_type: NUMERIC
        nullable: true
    """
)


def test_stamp_mapping_dialects_makes_same_dialect_bare_numeric_pass():
    # The row editor serializes without dialects, so the same-dialect waiver
    # cannot fire. Stamping the authoritative connection types lets a
    # same-dialect bare NUMERIC pass the Save gate.
    stamped = ss._stamp_mapping_dialects(
        _DIALECTLESS_BARE_NUMERIC_YAML,
        source_dialect="postgres",
        target_dialect="postgres",
    )
    assert "source_dialect: postgres" in stamped
    assert "target_dialect: postgres" in stamped
    parsed = ss._parse_yaml_mapping_text(stamped, label="mapping/test.yaml")
    assert parsed["columns"][0]["target_type"] == "NUMERIC"


def test_stamp_mapping_dialects_cross_dialect_bare_numeric_still_rejected():
    stamped = ss._stamp_mapping_dialects(
        _DIALECTLESS_BARE_NUMERIC_YAML,
        source_dialect="postgres",
        target_dialect="oracle",
    )
    with pytest.raises(ValueError, match="precision/scale"):
        ss._parse_yaml_mapping_text(stamped, label="mapping/test.yaml")


def test_stamp_mapping_dialects_overwrites_client_supplied_dialects():
    content = textwrap.dedent(
        """\
        version: v1
        source_dialect: oracle
        target_dialect: mssql
        columns:
          - source_name: id
            target_name: id
            target_type: INTEGER
            nullable: false
        """
    )
    stamped = ss._stamp_mapping_dialects(
        content, source_dialect="postgres", target_dialect="postgres"
    )
    import yaml as _yaml

    obj = _yaml.safe_load(stamped)
    assert obj["source_dialect"] == "postgres"
    assert obj["target_dialect"] == "postgres"
    # columns preserved
    assert obj["columns"][0]["target_name"] == "id"


def test_stamp_mapping_dialects_returns_malformed_content_unchanged():
    # A non-dict / unparseable body is left untouched so the normal gate raises
    # the proper shape error downstream.
    assert (
        ss._stamp_mapping_dialects(
            "- just\n- a\n- list\n", source_dialect="postgres", target_dialect="postgres"
        )
        == "- just\n- a\n- list\n"
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


def test_extract_sql_select_columns_postgres_lenient_on_unparameterized_numeric():
    # Lenient scaffold: an unparameterized numeric (Postgres bare `numeric`)
    # keeps its bare base type here instead of raising. The developer sets an
    # explicit precision in the Mapping Editor; strict validation enforces it at
    # Apply/Save, not at generate (scaffold) time.
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

    cols = ss.extract_sql_select_columns(session, postgres, "SELECT current_date")

    assert cols[0]["source_type"] == "NUMERIC"
    assert cols[0]["source_precision"] is None


def test_extract_sql_select_columns_unsized_string_falls_back_to_draft_varchar():
    # CASE/fonksiyon ifadelerinde surucu uzunluk bildirmez (display_size None).
    # Taslak uretimi durmaz: guvenli genislikte VARCHAR verilir, kullanici
    # Mapping Editor'de daraltir (unparameterized numeric ile ayni yaklasim).
    cursor = _Cursor(
        description=[
            _DescriptionColumn("pool_type", 1043, type_display="varchar"),
            _DescriptionColumn("label", 25, type_display="text"),
        ]
    )
    session = _Session(cursor)
    postgres = _dialect("PostgresDialect")

    cols = ss.extract_sql_select_columns(
        session, postgres, "SELECT CASE WHEN x THEN 'a' ELSE y END pool_type, l label FROM t"
    )

    expected = f"VARCHAR({ss.DEFAULT_EXPRESSION_VARCHAR_LENGTH})"
    assert cols[0]["source_type"] == expected
    # TEXT uzunluk tasimaz; dokunulmadan gecer.
    assert cols[1]["source_type"] == "TEXT"
