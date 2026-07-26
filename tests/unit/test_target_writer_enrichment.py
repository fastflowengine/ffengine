"""F1.2 - TargetWriter push-down enrichment path (real PG dialect, mock session)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ffengine.dialects.postgres import PostgresDialect
from ffengine.errors import ValidationError
from ffengine.mapping import expression as ex
from ffengine.mapping.resolver import DerivedExpr
from ffengine.pipeline.target_writer import TargetWriter

_COLS = {"id", "first_name", "last_name", "email"}


def _derived(text, target_type):
    ast = ex.compile_expression(text, _COLS)
    return DerivedExpr(ast=ast, refs=tuple(ex.column_refs(ast)), target_type=target_type)


@pytest.fixture
def session():
    s = MagicMock()
    s.conn = MagicMock()
    s.cursor.return_value = MagicMock()
    return s


@pytest.fixture
def writer(session):
    return TargetWriter(session, PostgresDialect())


def _executed(session):
    call = session.cursor.return_value.executemany.call_args
    return call[0][0], call[0][1]


def test_derived_value_computed_in_sql_not_python(writer, session):
    cfg = {
        "target_schema": "", "target_table": "t",
        "target_columns": ["first_name", "full_name"],
        "source_columns": ["first_name", "last_name"],
        "plain_source_by_target": {"first_name": "first_name"},
        "target_value_exprs": {
            "full_name": _derived("concat(first_name, ' ', last_name)", "varchar(220)")
        },
        "load_method": "append",
    }
    n = writer.write_batch([("Ada", "Lovelace"), ("Alan", "Turing")], cfg)
    assert n == 2
    sql, sent = _executed(session)
    assert "CAST((%s || ' ' || %s) AS VARCHAR(220))" in sql
    # bind order = [first_name(plain), first_name(ref), last_name(ref)]
    assert sent == [("Ada", "Ada", "Lovelace"), ("Alan", "Alan", "Turing")]
    # the derived value is NOT concatenated in Python
    assert all("Ada Lovelace" not in str(v) for row in sent for v in row)


def test_update_existing_field_expression(writer, session):
    cfg = {
        "target_schema": "", "target_table": "t",
        "target_columns": ["email"],
        "source_columns": ["email"],
        "plain_source_by_target": {},
        "target_value_exprs": {"email": _derived("lower(email)", "varchar(120)")},
        "load_method": "append",
    }
    writer.write_batch([("Ada@X.COM",)], cfg)
    sql, sent = _executed(session)
    assert "CAST(LOWER(%s) AS VARCHAR(120))" in sql
    assert sent == [("Ada@X.COM",)]  # raw value; lower() runs in DB


def test_enriched_upsert_plain_match(writer, session):
    cfg = {
        "target_schema": "", "target_table": "t",
        "target_columns": ["id", "email_lower"],
        "source_columns": ["id", "email"],
        "plain_source_by_target": {"id": "id"},
        "target_value_exprs": {"email_lower": _derived("lower(email)", "varchar(120)")},
        "load_method": "upsert",
        "upsert_match_columns": ["id"],
    }
    writer.write_batch([(1, "A@X.COM")], cfg)
    sql, sent = _executed(session)
    assert "ON CONFLICT" in sql and "EXCLUDED" in sql
    assert sent == [(1, "A@X.COM")]


def test_enriched_upsert_derived_match_rejected(writer):
    cfg = {
        "target_schema": "", "target_table": "t",
        "target_columns": ["id", "full_name"],
        "source_columns": ["id", "first_name", "last_name"],
        "plain_source_by_target": {"id": "id"},
        "target_value_exprs": {
            "full_name": _derived("concat(first_name, last_name)", "varchar(220)")
        },
        "load_method": "upsert",
        "upsert_match_columns": ["full_name"],
    }
    with pytest.raises(ValidationError, match="turetilmis"):
        writer.write_batch([(1, "a", "b")], cfg)


def test_enriched_upsert_null_match_rejected(writer):
    cfg = {
        "target_schema": "", "target_table": "t",
        "target_columns": ["id", "email_lower"],
        "source_columns": ["id", "email"],
        "plain_source_by_target": {"id": "id"},
        "target_value_exprs": {"email_lower": _derived("lower(email)", "varchar(120)")},
        "load_method": "upsert",
        "upsert_match_columns": ["id"],
    }
    with pytest.raises(ValidationError, match="NULL"):
        writer.write_batch([(None, "a@x.com")], cfg)
