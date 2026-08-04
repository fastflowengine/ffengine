"""F3.2 - guards: push-down works WITHOUT dbt (T-F3.2-1) and stays
single-source / join-free (T-F3.2-5).

These are characterization guards over the F1.2 push-down layer: they name
and lock structural properties that already hold at v0.1.2. A regression
here means engine code started depending on dbt or grew a multi-source
channel - both are architecture violations (INV-3/INV-4), not test debt.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

import ffengine
from ffengine.dialects.mssql import MSSQLDialect
from ffengine.dialects.oracle import OracleDialect
from ffengine.dialects.postgres import PostgresDialect
from ffengine.errors.exceptions import MappingError
from ffengine.mapping import expression as ex
from ffengine.mapping.resolver import DerivedExpr

_DIALECTS = (PostgresDialect, MSSQLDialect, OracleDialect)
_COLS = {"first_name", "last_name", "email", "id"}

_DBT_IMPORT_RE = re.compile(r"^\s*(?:import|from)\s+dbt\b", re.MULTILINE)


def _derived(text: str, target_type: str) -> DerivedExpr:
    ast = ex.compile_expression(text, _COLS)
    return DerivedExpr(
        ast=ast, refs=tuple(ex.column_refs(ast)), target_type=target_type
    )


def _enrichment_case():
    target_columns = ["first_name", "full_name"]
    value_exprs = {
        "full_name": _derived("concat(first_name, ' ', last_name)", "varchar(220)")
    }
    plain = {"first_name": "first_name"}
    return target_columns, value_exprs, plain


# --- T-F3.2-1: push-down is structurally dbt-free ---------------------------

def test_no_ffengine_module_imports_dbt():
    package_root = Path(ffengine.__file__).parent
    offenders = [
        str(path)
        for path in sorted(package_root.rglob("*.py"))
        if _DBT_IMPORT_RE.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == [], f"ffengine modulleri dbt import edemez: {offenders}"


def test_enrichment_import_path_does_not_load_dbt_module():
    # Import the full push-down chain and render both write paths; none of
    # this may pull a dbt module into the process.
    import ffengine.airflow.generated_factory  # noqa: F401
    import ffengine.pipeline.target_writer  # noqa: F401

    target_columns, value_exprs, plain = _enrichment_case()
    for dialect_cls in _DIALECTS:
        dialect = dialect_cls()
        dialect.generate_enriched_insert_query(
            "t", target_columns, value_exprs, plain
        )
        dialect.generate_promotion_select(
            "t_staging", "t", target_columns, value_exprs, plain
        )

    loaded = [
        name for name in sys.modules
        if name == "dbt" or name.startswith("dbt.")
    ]
    assert loaded == [], f"push-down yolu dbt modulu yukledi: {loaded}"


def test_generated_dag_with_dbt_task_parses_without_dbt_binary(monkeypatch):
    # DAG parse must never resolve the dbt binary or spawn a process; the
    # provider seam defers all of that to Enterprise task runtime.
    import shutil
    import subprocess

    from ffengine.airflow import task_type_registry as reg
    from ffengine.airflow.generated_factory import build_generated_dag
    from airflow.providers.standard.operators.empty import EmptyOperator

    def _explode(*_args, **_kwargs):
        raise AssertionError("DAG parse dbt binary'sine dokunamaz")

    monkeypatch.setattr(shutil, "which", _explode)
    monkeypatch.setattr(subprocess, "run", _explode)
    monkeypatch.setattr(subprocess, "Popen", _explode)

    reg.clear_task_type_providers()
    try:
        reg.register_task_type_provider(
            "dbt",
            lambda **kwargs: EmptyOperator(task_id=kwargs["task_id"]),
        )
        dag = build_generated_dag(
            dag_id="dbt_parse_guard",
            dag_tags=[],
            upstream_dag_ids=[],
            raw_config_snapshot={
                "source_db_var": "src_conn",
                "target_db_var": "tgt_conn",
                "scheduler": {
                    "cron_expression": None,
                    "timezone": "UTC",
                    "active": True,
                    "start_date": "2024-01-01T00:00:00",
                },
                "__config_path": "/opt/airflow/projects/demo/demo.yaml",
                "flow_tasks": [{
                    "task_group_id": "dbt_build",
                    "task_type": "dbt",
                    "depends_on": [],
                    "dbt_project_ref": "finance",
                    "dbt_command": "build",
                    "dbt_select": "tag:nightly",
                }],
            },
        )
    finally:
        reg.clear_task_type_providers()

    assert "dbt__dbt_build" in dag.task_dict
    loaded = [
        name for name in sys.modules
        if name == "dbt" or name.startswith("dbt.")
    ]
    assert loaded == []


# --- T-F3.2-5: no in-engine join / multi-source channel ---------------------

def test_join_keyword_rejected_as_expression_column():
    # Infix join syntax dies at the grammar (trailing tokens); the bare
    # keyword dies at the deny-list. Either way: fail-loud, never SQL.
    with pytest.raises(
        MappingError,
        match=r"reserved keyword 'join'|trailing tokens near 'join'",
    ):
        ex.compile_expression("first_name join last_name", _COLS)


@pytest.mark.parametrize(
    "keyword",
    ["select", "from", "where", "union", "join", "merge", "into", "values"],
)
def test_from_where_union_select_keywords_rejected(keyword):
    # Denied even in column-ref position, and even when a physical source
    # column with that name exists (defense-in-depth, expression._DENY).
    with pytest.raises(MappingError, match="reserved keyword"):
        ex.compile_expression(keyword, _COLS | {keyword})


def test_enriched_sql_and_promotion_contain_no_join_across_dialects():
    target_columns, value_exprs, plain = _enrichment_case()
    rendered: list[str] = []
    for dialect_cls in _DIALECTS:
        dialect = dialect_cls()
        insert_sql, _ = dialect.generate_enriched_insert_query(
            "t", target_columns, value_exprs, plain
        )
        promotion_sql = dialect.generate_promotion_select(
            "t_staging", "t", target_columns, value_exprs, plain
        )
        upsert_sql, _ = dialect.generate_enriched_upsert_query(
            "t",
            ["id", "full_name"],
            {"full_name": value_exprs["full_name"]},
            {"id": "id"},
            match_columns=["id"],
            update_columns=["full_name"],
        )
        rendered.extend([insert_sql, promotion_sql, upsert_sql])

    offenders = [sql for sql in rendered if "JOIN" in sql.upper()]
    assert offenders == [], f"motor SQL'inde JOIN uretilemez: {offenders}"
