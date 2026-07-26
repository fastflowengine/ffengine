"""F1.2 - per-dialect push-down enrichment SQL generation."""

from __future__ import annotations

from ffengine.dialects.mssql import MSSQLDialect
from ffengine.dialects.oracle import OracleDialect
from ffengine.dialects.postgres import PostgresDialect
from ffengine.mapping import expression as ex
from ffengine.mapping.resolver import DerivedExpr

COLS = {"first_name", "last_name", "email", "amount", "qty", "id"}


def _derived(text, target_type):
    ast = ex.compile_expression(text, COLS)
    return DerivedExpr(ast=ast, refs=tuple(ex.column_refs(ast)), target_type=target_type)


# full_name = concat(first_name, ' ', last_name); plain first_name kept.
def _full_name_case():
    target_columns = ["first_name", "full_name"]
    value_exprs = {"full_name": _derived("concat(first_name, ' ', last_name)", "varchar(220)")}
    plain = {"first_name": "first_name"}
    return target_columns, value_exprs, plain


def test_pg_enriched_insert():
    sql, binds = PostgresDialect().generate_enriched_insert_query("t", *_full_name_case())
    assert sql == (
        'INSERT INTO t ("first_name", "full_name") '
        "VALUES (%s, CAST((%s || ' ' || %s) AS VARCHAR(220)))"
    )
    assert binds == ["first_name", "first_name", "last_name"]


def test_mssql_enriched_insert_uses_concat_and_qmark():
    sql, binds = MSSQLDialect().generate_enriched_insert_query("t", *_full_name_case())
    assert sql == (
        "INSERT INTO t ([first_name], [full_name]) "
        "VALUES (?, CAST(CONCAT(?, ' ', ?) AS VARCHAR(220)))"
    )
    assert binds == ["first_name", "first_name", "last_name"]


def test_oracle_enriched_insert_numbered_binds_and_uppercase():
    sql, binds = OracleDialect().generate_enriched_insert_query("t", *_full_name_case())
    assert sql == (
        'INSERT INTO t ("FIRST_NAME", "FULL_NAME") '
        "VALUES (:1, CAST((:2 || ' ' || :3) AS VARCHAR(220)))"
    )
    assert binds == ["first_name", "first_name", "last_name"]


def test_current_date_cast_per_dialect():
    tc = (["load_date"], {"load_date": _derived("current_date", "date")}, {})
    assert PostgresDialect().generate_enriched_insert_query("t", *tc)[0].endswith(
        "VALUES (CAST(CURRENT_DATE AS DATE))"
    )
    assert "CAST(GETDATE() AS DATE)" in MSSQLDialect().generate_enriched_insert_query("t", *tc)[0]
    assert "TRUNC(SYSDATE)" in OracleDialect().generate_enriched_insert_query("t", *tc)[0]


def test_substring_oracle_substr_in_insert():
    tc = (["code"], {"code": _derived("substring(email, 1, 3)", "varchar(3)")}, {})
    sql = OracleDialect().generate_enriched_insert_query("t", *tc)[0]
    assert "SUBSTR(:1, 1, 3)" in sql


def test_pg_enriched_upsert_on_conflict():
    target_columns = ["id", "email_lower"]
    value_exprs = {"email_lower": _derived("lower(email)", "varchar(120)")}
    plain = {"id": "id"}
    sql, binds = PostgresDialect().generate_enriched_upsert_query(
        "t", target_columns, value_exprs, plain,
        match_columns=["id"], update_columns=["email_lower"],
    )
    assert sql == (
        'INSERT INTO t ("id", "email_lower") '
        "VALUES (%s, CAST(LOWER(%s) AS VARCHAR(120))) "
        'ON CONFLICT ("id") DO UPDATE SET "email_lower" = EXCLUDED."email_lower"'
    )
    assert binds == ["id", "email"]


def test_oracle_enriched_upsert_merge():
    target_columns = ["id", "email_lower"]
    value_exprs = {"email_lower": _derived("lower(email)", "varchar(120)")}
    plain = {"id": "id"}
    sql, binds = OracleDialect().generate_enriched_upsert_query(
        "t", target_columns, value_exprs, plain,
        match_columns=["id"], update_columns=["email_lower"],
    )
    assert "MERGE INTO t target" in sql
    assert 'SELECT :1 AS "ID", CAST(LOWER(:2) AS VARCHAR(120)) AS "EMAIL_LOWER" FROM DUAL' in sql
    assert binds == ["id", "email"]


def test_promotion_select_colref_mode():
    sql = PostgresDialect().generate_promotion_select(
        "raw_staging", "final_t", *_full_name_case()
    )
    assert sql == (
        'INSERT INTO final_t ("first_name", "full_name") '
        'SELECT "first_name", CAST(("first_name" || \' \' || "last_name") AS VARCHAR(220)) '
        "FROM raw_staging"
    )
