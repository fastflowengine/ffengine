"""
C11_T04 - Mapping zinciri entegrasyon testi.

Akış:
mapping_generator -> config -> DAG generate -> operator run -> verify
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
import yaml

from ffengine.airflow.dag_generator import generate_dags
from ffengine.airflow.operator import FFEngineOperator
from ffengine.db.session import DBSession
from ffengine.dialects import PostgresDialect
from ffengine.mapping.generator import MappingGenerator


_REQUIRED_ENV = [
    "PG_TEST_HOST",
    "PG_TEST_PORT",
    "PG_TEST_USER",
    "PG_TEST_PASSWORD",
    "PG_TEST_DB",
]


def _integration_enable_state() -> tuple[bool, str]:
    if os.getenv("FFENGINE_ENABLE_CROSS_DB_TESTS", "0").strip() != "1":
        return False, "FFENGINE_ENABLE_CROSS_DB_TESTS=1 olmadığı için skip."
    missing = [k for k in _REQUIRED_ENV if not (os.getenv(k) or "").strip()]
    if missing:
        return False, f"Eksik environment değişkenleri: {', '.join(missing)}"
    return True, ""


_ENABLED, _SKIP_REASON = _integration_enable_state()
pytestmark = [pytest.mark.integration]
if not _ENABLED:
    pytestmark.append(pytest.mark.skip(reason=_SKIP_REASON))


def _pg_params() -> dict:
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5432")),
        "user": os.getenv("PG_TEST_USER", "ffengine"),
        "password": os.getenv("PG_TEST_PASSWORD", "ffengine"),
        "database": os.getenv("PG_TEST_DB", "ffengine_test"),
        "conn_type": "postgres",
    }


@pytest.fixture()
def pg_session():
    dialect = PostgresDialect()
    session = DBSession(_pg_params(), dialect)
    session.__enter__()
    try:
        yield session, dialect
    finally:
        session.__exit__(None, None, None)


def test_mapping_to_dag_to_run_chain(pg_session, tmp_path):
    (session, dialect) = pg_session
    src_table = "chain_src"
    tgt_table = "chain_tgt"
    schema = "public"
    q_schema = dialect.quote_identifier(schema)
    q_src = dialect.quote_identifier(src_table)
    q_tgt = dialect.quote_identifier(tgt_table)

    cur = session.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_src}")
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_tgt}")
        cur.execute(
            f"CREATE TABLE {q_schema}.{q_src} "
            "(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
        )
        cur.execute(
            f"CREATE TABLE {q_schema}.{q_tgt} "
            "(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
        )
        cur.executemany(
            f"INSERT INTO {q_schema}.{q_src} (id, name, amount) VALUES (%s, %s, %s)",
            [(1, "A", 10.10), (2, "B", 20.20), (3, "C", 30.30)],
        )
        session.conn.commit()
    finally:
        cur.close()

    mapping_path = tmp_path / "orders_mapping.yaml"
    mapping = MappingGenerator().generate(
        session.conn,
        dialect,
        PostgresDialect(),
        schema,
        src_table,
    )
    MappingGenerator().save(mapping, str(mapping_path))

    config_path = tmp_path / "orders_chain.yaml"
    config = {
        "source_db_var": "src_pg",
        "target_db_var": "tgt_pg",
        "etl_tasks": [
            {
                "task_group_id": "chain_task",
                "source_schema": schema,
                "source_table": src_table,
                "source_type": "table",
                "target_schema": schema,
                "target_table": tgt_table,
                "load_method": "append",
                "column_mapping_mode": "mapping_file",
                "mapping_file": str(mapping_path),
                "passthrough_full": False,
            }
        ],
    }
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    dags = generate_dags(str(tmp_path))
    assert "ffengine_orders_chain_chain_task" in dags

    with patch(
        "ffengine.db.airflow_adapter.AirflowConnectionAdapter.get_connection_params",
        return_value=_pg_params(),
    ):
        result = FFEngineOperator(
            config_path=str(config_path),
            task_group_id="chain_task",
            source_conn_id="src_pg",
            target_conn_id="tgt_pg",
        ).execute({})

    assert result["rows"] == 3
    assert result["errors"] == []

    cur = session.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {q_schema}.{q_tgt}")
        assert cur.fetchone()[0] == 3
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_src}")
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_tgt}")
        session.conn.commit()
        cur.close()
