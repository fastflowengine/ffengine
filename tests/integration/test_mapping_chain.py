"""
C11_T04 - Mapping zinciri entegrasyon testi.

Akış:
  ff_test_data (kalıcı kaynak, 100 satır) →
  MappingGenerator → YAML config → DAG generate → FFEngineOperator.execute() →
  ff_test_target → satır sayısı ve veri bütünlüğü doğrulama

Çalıştırma:
  FFENGINE_ENABLE_CROSS_DB_TESTS=1 pytest tests/integration/test_mapping_chain.py -v
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


_REQUIRED_ENV: list[str] = []  # Tüm bağlantı değerlerinin container-uyumlu default'u var; enable flag yeterli.


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
        "port": int(os.getenv("PG_TEST_PORT", "5435")),
        "user": os.getenv("POSTGRES_TEST_USER", "ffengine_test"),
        "password": os.getenv("POSTGRES_TEST_PASS", "ffengine_pg_pass"),
        "database": os.getenv("POSTGRES_TEST_DB", "ffengine_test_db"),
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
    """ff_test_data → MappingGenerator → FFEngineOperator → ff_test_target: 100 satır."""
    session, dialect = pg_session
    src_table = "ff_test_data"   # kalıcı kaynak tablo
    tgt_table = "ff_test_target"
    schema = "public"
    q_schema = dialect.quote_identifier(schema)
    q_tgt = dialect.quote_identifier(tgt_table)

    # Hedef tabloyu oluştur
    cur = session.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_tgt}")
        cur.execute(
            f"CREATE TABLE {q_schema}.{q_tgt} "
            "(id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL)"
        )
        session.conn.commit()
    finally:
        cur.close()

    # Kaynak tablo şemasından mapping üret
    mapping_path = tmp_path / "ff_mapping.yaml"
    mapping = MappingGenerator().generate(
        session.conn,
        dialect,
        PostgresDialect(),
        schema,
        src_table,
    )
    MappingGenerator().save(mapping, str(mapping_path))

    # ETL YAML config
    config_path = tmp_path / "ff_chain.yaml"
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

    # DAG üretimi
    dags = generate_dags(str(tmp_path))
    assert "ffengine_ff_chain_chain_task" in dags

    # Operator çalıştır
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

    assert result["rows"] == 100
    assert result["errors"] == []

    # Hedef tabloda 100 satır olmalı
    cur = session.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {q_schema}.{q_tgt}")
        assert cur.fetchone()[0] == 100

        # İlk satır kontrolü
        cur.execute(
            f"SELECT id, name FROM {q_schema}.{q_tgt} ORDER BY id LIMIT 1"
        )
        first = cur.fetchone()
        assert first[0] == 1
        assert first[1] == "test_record_001"
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {q_schema}.{q_tgt}")
        session.conn.commit()
        cur.close()
