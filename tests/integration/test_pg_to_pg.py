"""
Integration test: PostgreSQL → PostgreSQL pipeline (PG → PG).

DURUM: C05 (YAML Config) tamamlandığında aktif edilecek.

Ön koşullar:
  - docker/docker-compose.test.yml'deki postgres-test servisi çalışıyor olmalı
  - .env dosyasında PG_TEST_HOST, PG_TEST_PORT, PG_TEST_USER,
    PG_TEST_PASSWORD, PG_TEST_DB tanımlı olmalı
  - C05 ConfigLoader implement edilmiş olmalı

Çalıştırma:
  pytest tests/integration/test_pg_to_pg.py -v
"""

import os
import pytest

# C05 tamamlanana kadar bu testler skip edilir.
pytestmark = pytest.mark.skip(
    reason="C05 (YAML Config) tamamlandığında aktif edilecek."
)


# ------------------------------------------------------------------
# Fixtures (C05 sonrası gerçek bağlantılarla doldurulacak)
# ------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_conn_params():
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5432")),
        "user": os.getenv("PG_TEST_USER", "ffengine"),
        "password": os.getenv("PG_TEST_PASSWORD", "ffengine"),
        "database": os.getenv("PG_TEST_DB", "ffengine_test"),
    }


@pytest.fixture(scope="module")
def pg_dialect():
    from ffengine.dialects import PostgresDialect
    return PostgresDialect()


@pytest.fixture(scope="module")
def src_session(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession
    with DBSession(pg_conn_params, pg_dialect) as session:
        yield session


@pytest.fixture(scope="module")
def tgt_session(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession
    with DBSession(pg_conn_params, pg_dialect) as session:
        yield session


# ------------------------------------------------------------------
# Setup — test tablosunu hazırla
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def setup_tables(src_session, tgt_session, pg_dialect):
    """Kaynak tabloyu oluştur ve test verisi ekle, hedef tabloyu temizle."""
    src_cursor = src_session.cursor()
    src_cursor.execute(
        "CREATE TABLE IF NOT EXISTS pg_to_pg_source "
        "(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
    )
    src_cursor.execute("TRUNCATE TABLE pg_to_pg_source")
    src_cursor.executemany(
        "INSERT INTO pg_to_pg_source VALUES (%s, %s, %s)",
        [(1, "Alice", 100.50), (2, "Bob", 200.00), (3, "Carol", 300.75)],
    )
    src_session.conn.commit()
    src_cursor.close()

    tgt_cursor = tgt_session.cursor()
    tgt_cursor.execute("DROP TABLE IF EXISTS pg_to_pg_target")
    tgt_cursor.execute(
        "CREATE TABLE pg_to_pg_target "
        "(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
    )
    tgt_session.conn.commit()
    tgt_cursor.close()

    yield

    # Teardown
    src_session.cursor().execute("DROP TABLE IF EXISTS pg_to_pg_source")
    tgt_session.cursor().execute("DROP TABLE IF EXISTS pg_to_pg_target")
    src_session.conn.commit()
    tgt_session.conn.commit()


# ------------------------------------------------------------------
# Test senaryoları
# ------------------------------------------------------------------


def test_pg_to_pg_row_count(src_session, tgt_session, pg_dialect):
    from ffengine.core.etl_manager import ETLManager

    task_config = {
        "load_method": "append",
        "source_schema": "public",
        "source_table": "pg_to_pg_source",
        "source_columns": ["id", "name", "amount"],
        "target_schema": "public",
        "target_table": "pg_to_pg_target",
        "target_columns": ["id", "name", "amount"],
        "target_columns_meta": [],
        "batch_size": 1000,
    }

    manager = ETLManager()
    result = manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=task_config,
    )

    assert result.rows == 3
    assert result.errors == []

    # Hedef tabloda 3 satır olmalı
    cursor = tgt_session.cursor()
    cursor.execute("SELECT COUNT(*) FROM pg_to_pg_target")
    count = cursor.fetchone()[0]
    cursor.close()
    assert count == 3


def test_pg_to_pg_data_integrity(src_session, tgt_session, pg_dialect):
    """Kaynak ve hedef veriler birebir eşleşmeli."""
    from ffengine.core.etl_manager import ETLManager

    task_config = {
        "load_method": "append",
        "source_schema": "public",
        "source_table": "pg_to_pg_source",
        "source_columns": ["id", "name", "amount"],
        "target_schema": "public",
        "target_table": "pg_to_pg_target",
        "target_columns": ["id", "name", "amount"],
        "target_columns_meta": [],
        "batch_size": 1000,
    }

    manager = ETLManager()
    manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=task_config,
    )

    cursor = tgt_session.cursor()
    cursor.execute("SELECT id, name FROM pg_to_pg_target ORDER BY id")
    rows = cursor.fetchall()
    cursor.close()

    assert rows[0] == (1, "Alice")
    assert rows[1] == (2, "Bob")
    assert rows[2] == (3, "Carol")


def test_pg_to_pg_etl_result_fields(src_session, tgt_session, pg_dialect):
    from ffengine.core.etl_manager import ETLManager
    from ffengine.core.base_engine import ETLResult

    task_config = {
        "load_method": "append",
        "source_schema": "public",
        "source_table": "pg_to_pg_source",
        "source_columns": ["id", "name", "amount"],
        "target_schema": "public",
        "target_table": "pg_to_pg_target",
        "target_columns": ["id", "name", "amount"],
        "target_columns_meta": [],
    }

    manager = ETLManager()
    result = manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=task_config,
    )

    assert isinstance(result, ETLResult)
    assert result.partitions_completed == 1
    assert result.duration_seconds >= 0
    assert result.throughput >= 0
