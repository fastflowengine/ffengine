"""F1.2 - controlled push-down expression model tests."""

from __future__ import annotations

import pytest

from ffengine.errors.exceptions import MappingError
from ffengine.mapping import expression as ex

COLS = {"first_name", "last_name", "email", "amount", "qty"}


def _bind_ops(dialect="postgres"):
    """Bind-mode ops that record bound column names in order."""
    bound: list[str] = []

    def alloc(name: str) -> str:
        bound.append(name)
        return "%s"

    ops = ex.ops_for(dialect, mode="bind", alloc=alloc)
    return ops, bound


def _colref_ops(dialect="postgres"):
    return ex.ops_for(dialect, mode="colref", quote=lambda n: f'"{n}"')


def _render(text, dialect="postgres", mode="bind"):
    expr = ex.compile_expression(text, COLS)
    if mode == "bind":
        ops, bound = _bind_ops(dialect)
        return ex.render(expr, ops), bound
    return ex.render(expr, _colref_ops(dialect)), None


# --- happy path ------------------------------------------------------------

def test_concat_bind_mode_pg():
    sql, bound = _render("concat(first_name, ' ', last_name)")
    assert sql == "(%s || ' ' || %s)"
    assert bound == ["first_name", "last_name"]


def test_concat_mssql_uses_concat_func():
    sql, bound = _render("concat(first_name, ' ', last_name)", dialect="mssql")
    assert sql == "CONCAT(%s, ' ', %s)"
    assert bound == ["first_name", "last_name"]


def test_concat_oracle_uses_double_pipe():
    sql, _ = _render("concat(first_name, last_name)", dialect="oracle")
    assert sql == "(%s || %s)"


def test_current_date_per_dialect():
    assert _render("current_date")[0] == "CURRENT_DATE"
    assert _render("current_date", dialect="mssql")[0] == "CAST(GETDATE() AS DATE)"
    assert _render("current_date", dialect="oracle")[0] == "TRUNC(SYSDATE)"


def test_current_timestamp_per_dialect():
    assert _render("current_timestamp")[0] == "CURRENT_TIMESTAMP"
    assert _render("current_timestamp", dialect="mssql")[0] == "SYSDATETIME()"
    assert _render("current_timestamp", dialect="oracle")[0] == "SYSTIMESTAMP"


def test_substring_oracle_becomes_substr():
    assert _render("substring(email, 1, 3)", dialect="oracle")[0] == "SUBSTR(%s, 1, 3)"
    assert _render("substring(email, 1, 3)")[0] == "SUBSTRING(%s, 1, 3)"


def test_update_existing_field_lower():
    sql, bound = _render("lower(email)")
    assert sql == "LOWER(%s)"
    assert bound == ["email"]


def test_arithmetic_and_coalesce():
    sql, bound = _render("coalesce(amount, 0) / qty")
    assert sql == "(COALESCE(%s, 0) / %s)"
    assert bound == ["amount", "qty"]


def test_cast_renders_type_literal():
    sql, _ = _render("cast(amount as varchar(20))")
    assert sql == "CAST(%s AS VARCHAR(20))"


def test_colref_mode_quotes_columns():
    sql, _ = _render("concat(first_name, last_name)", mode="colref")
    assert sql == '("first_name" || "last_name")'


def test_column_refs_preserves_order_and_duplicates():
    expr = ex.compile_expression("concat(first_name, ' ', first_name, last_name)", COLS)
    assert ex.column_refs(expr) == ["first_name", "first_name", "last_name"]


def test_render_is_pure_and_stable():
    expr = ex.compile_expression("concat(first_name, ' ', last_name)", COLS)
    ops1, _ = _bind_ops()
    ops2, _ = _bind_ops()
    assert ex.render(expr, ops1) == ex.render(expr, ops2)


# --- fail-loud rejections --------------------------------------------------

@pytest.mark.parametrize("bad", [
    "first_name; drop table x",
    "first_name -- comment",
    "first_name /* c */",
    "concat(first_name, last_name)\\",
])
def test_injection_sequences_rejected(bad):
    with pytest.raises(MappingError):
        ex.compile_expression(bad, COLS)


def test_subquery_keyword_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("(select 1)", COLS)


def test_schema_qualified_identifier_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("public.email", COLS)


def test_unknown_function_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("md5(email)", COLS)


def test_unknown_column_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("concat(first_name, missing_col)", COLS)


def test_bad_arity_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("upper(first_name, last_name)", COLS)


def test_empty_expression_rejected():
    with pytest.raises(MappingError):
        ex.compile_expression("   ", COLS)
