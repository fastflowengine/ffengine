"""
F1.2 integration: PostgreSQL push-down enrichment (v1.1 derived columns).

Verifies: derived values computed in the target DB (flat source SELECT), the
one-step vs two-step (staging-promotion) value-equality gate (R5 / T-F1.2-1),
derived upsert, and mapping_file drift fail-loud.

Run: FFENGINE_ENABLE_PG_TESTS=1 pytest tests/integration/test_pg_enrichment.py -v
"""

from __future__ import annotations

import os
import textwrap

import pytest

from ffengine.errors.exceptions import MappingError


def _should_skip():
    if os.getenv("FFENGINE_ENABLE_PG_TESTS", "0").strip() != "1":
        return True, "FFENGINE_ENABLE_PG_TESTS=1 olmadığı için skip."
    return False, ""


_SKIP, _SKIP_REASON = _should_skip()
pytestmark = [pytest.mark.integration]
if _SKIP:
    pytestmark.append(pytest.mark.skip(reason=_SKIP_REASON))


@pytest.fixture(scope="module")
def pg_conn_params():
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5435")),
        "user": os.getenv("POSTGRES_TEST_USER", "ffengine_test"),
        "password": os.getenv("POSTGRES_TEST_PASS", "ffengine_pg_pass"),
        "database": os.getenv("POSTGRES_TEST_DB", "ffengine_test_db"),
    }


@pytest.fixture(scope="module")
def pg_dialect():
    from ffengine.dialects import PostgresDialect

    return PostgresDialect()


@pytest.fixture(scope="module")
def sessions(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession

    with DBSession(pg_conn_params, pg_dialect) as src, \
            DBSession(pg_conn_params, pg_dialect) as tgt:
        yield src, tgt


@pytest.fixture(autouse=True)
def clean_tables(sessions):
    src, tgt = sessions
    tgt_names = [
        "ff_enr_tgt", "ff_enr_single", "ff_enr_two", "ff_enr_stg", "ff_enr_up",
    ]
    src_names = ["ff_enr_src"]
    _drop(src, src_names)
    _drop(tgt, tgt_names)
    yield
    _drop(src, src_names)
    _drop(tgt, tgt_names)


def _drop(session, names):
    # Clear any idle-in-transaction state (server-side read cursors) first so
    # DROP does not wait on a self-held lock.
    try:
        session.conn.rollback()
    except Exception:
        pass
    cur = session.cursor()
    try:
        for n in names:
            cur.execute(f"DROP TABLE IF EXISTS public.{n}")
        session.conn.commit()
    except Exception:
        session.conn.rollback()
    finally:
        cur.close()


def _exec(session, *stmts):
    cur = session.cursor()
    try:
        for s in stmts:
            cur.execute(s)
        session.conn.commit()
    finally:
        cur.close()


def _rows(session, sql):
    cur = session.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


_MAPPING = textwrap.dedent(
    """\
    version: v1.1
    columns:
      - source_name: first_name
        target_name: first_name
        target_type: varchar(50)
      - target_name: full_name
        target_type: varchar(220)
        expression: concat(first_name, ' ', last_name)
      - target_name: email_lower
        target_type: varchar(120)
        expression: lower(email)
    """
)


def _resolve_cfg(task_config, src_session, dialect):
    from ffengine.mapping.resolver import MappingResolver

    m = MappingResolver().resolve(
        task_config, src_session.conn, dialect, dialect
    )
    cfg = dict(task_config)
    cfg["source_columns"] = m.source_columns
    cfg["target_columns"] = m.target_columns
    cfg["target_columns_meta"] = m.target_columns_meta
    cfg["target_value_exprs"] = m.target_value_exprs
    cfg["plain_source_by_target"] = m.plain_source_by_target
    return cfg, m


def _seed_source(src, tgt):
    _exec(
        src,
        "CREATE TABLE public.ff_enr_src "
        "(first_name varchar(50), last_name varchar(50), email varchar(120))",
        "INSERT INTO public.ff_enr_src VALUES "
        "('Ada','Lovelace','Ada@X.COM'),('Alan','Turing','ALAN@y.com')",
    )


def test_derived_values_computed_in_target(sessions, pg_dialect, tmp_path):
    src, tgt = sessions
    _seed_source(src, tgt)
    _exec(
        tgt,
        "CREATE TABLE public.ff_enr_tgt "
        "(first_name varchar(50), full_name varchar(220), email_lower varchar(120))",
    )
    mfile = tmp_path / "m.yaml"
    mfile.write_text(_MAPPING, encoding="utf-8")
    cfg = {
        "load_method": "append",
        "source_schema": "public", "source_table": "ff_enr_src",
        "column_mapping_mode": "mapping_file", "mapping_file": str(mfile),
        "target_schema": "public", "target_table": "ff_enr_tgt",
        "batch_size": 1000,
    }
    from ffengine.core.flow_manager import FlowManager

    cfg, m = _resolve_cfg(cfg, src, pg_dialect)
    # flat source SELECT: only physical columns, no expression (T-F1.2-3)
    assert m.source_columns == ["first_name", "last_name", "email"]
    FlowManager().run_flow_task(
        src_session=src, tgt_session=tgt,
        src_dialect=pg_dialect, tgt_dialect=pg_dialect, task_config=cfg,
    )
    rows = _rows(
        tgt,
        "SELECT first_name, full_name, email_lower "
        "FROM public.ff_enr_tgt ORDER BY first_name",
    )
    assert rows == [
        ("Ada", "Ada Lovelace", "ada@x.com"),
        ("Alan", "Alan Turing", "alan@y.com"),
    ]


def test_one_step_vs_two_step_value_equality(sessions, pg_dialect, tmp_path):
    """R5 / T-F1.2-1: single-step INSERT == staging-promotion, same expressions."""
    src, tgt = sessions
    _seed_source(src, tgt)
    mfile = tmp_path / "m.yaml"
    mfile.write_text(_MAPPING, encoding="utf-8")
    cfg = {
        "load_method": "append",
        "source_schema": "public", "source_table": "ff_enr_src",
        "column_mapping_mode": "mapping_file", "mapping_file": str(mfile),
        "target_schema": "public", "target_table": "ff_enr_single",
        "batch_size": 1000,
    }
    cfg, m = _resolve_cfg(cfg, src, pg_dialect)
    ddl = ("(first_name varchar(50), full_name varchar(220), "
           "email_lower varchar(120))")
    _exec(tgt, f"CREATE TABLE public.ff_enr_single {ddl}")
    _exec(tgt, f"CREATE TABLE public.ff_enr_two {ddl}")
    _exec(
        tgt,
        "CREATE TABLE public.ff_enr_stg "
        "(first_name varchar(50), last_name varchar(50), email varchar(120))",
    )

    # single-step: enriched executemany insert
    from ffengine.pipeline.target_writer import TargetWriter

    src_rows = _rows(
        src, "SELECT first_name, last_name, email FROM public.ff_enr_src"
    )
    single_cfg = dict(cfg)
    single_cfg["target_table"] = "ff_enr_single"
    TargetWriter(tgt, pg_dialect).write_batch(list(src_rows), single_cfg)

    # two-step: fill raw staging by ordinary insert, then promotion SELECT
    stg_insert = pg_dialect.generate_bulk_insert_query(
        "public.ff_enr_stg", ["first_name", "last_name", "email"]
    )
    cur = tgt.cursor()
    try:
        cur.executemany(stg_insert, list(src_rows))
        promo = pg_dialect.generate_promotion_select(
            "public.ff_enr_stg", "public.ff_enr_two",
            m.target_columns, m.target_value_exprs, m.plain_source_by_target,
        )
        cur.execute(promo)
        tgt.conn.commit()
    finally:
        cur.close()

    order = "ORDER BY first_name"
    one = _rows(tgt, f"SELECT * FROM public.ff_enr_single {order}")
    two = _rows(tgt, f"SELECT * FROM public.ff_enr_two {order}")
    assert one == two
    assert one == [
        ("Ada", "Ada Lovelace", "ada@x.com"),
        ("Alan", "Alan Turing", "alan@y.com"),
    ]


def test_derived_upsert_updates_expression(sessions, pg_dialect):
    src, tgt = sessions
    _exec(
        tgt,
        "CREATE TABLE public.ff_enr_up "
        "(id int PRIMARY KEY, email_lower varchar(120))",
    )
    from ffengine.mapping import expression as ex
    from ffengine.mapping.resolver import DerivedExpr
    from ffengine.pipeline.target_writer import TargetWriter

    ast = ex.compile_expression("lower(email)", {"id", "email"})
    cfg = {
        "target_schema": "public", "target_table": "ff_enr_up",
        "target_columns": ["id", "email_lower"],
        "source_columns": ["id", "email"],
        "plain_source_by_target": {"id": "id"},
        "target_value_exprs": {
            "email_lower": DerivedExpr(
                ast=ast, refs=("email",), target_type="varchar(120)"
            )
        },
        "load_method": "upsert",
        "upsert_match_columns": ["id"],
    }
    writer = TargetWriter(tgt, pg_dialect)
    writer.write_batch([(1, "FIRST@X.COM")], cfg)
    writer.write_batch([(1, "SECOND@Y.COM")], cfg)  # same id -> update
    rows = _rows(tgt, "SELECT id, email_lower FROM public.ff_enr_up")
    assert rows == [(1, "second@y.com")]


def test_mapping_file_drift_fails_loud(sessions, pg_dialect, tmp_path):
    src, tgt = sessions
    _seed_source(src, tgt)
    mapping = textwrap.dedent(
        """\
        version: v1.1
        columns:
          - source_name: ghost_col
            target_name: ghost
            target_type: varchar(10)
        """
    )
    mfile = tmp_path / "drift.yaml"
    mfile.write_text(mapping, encoding="utf-8")
    cfg = {
        "source_schema": "public", "source_table": "ff_enr_src",
        "column_mapping_mode": "mapping_file", "mapping_file": str(mfile),
    }
    from ffengine.mapping.resolver import MappingResolver

    with pytest.raises(MappingError, match="drift"):
        MappingResolver().resolve(cfg, src.conn, pg_dialect, pg_dialect)
