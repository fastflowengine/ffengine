"""
Integration test: PostgreSQL → PostgreSQL pipeline (PG → PG).

Kaynak tablo: ff_test_data (kalıcı, 100 satır — docker-compose.test.yml ile oluşturulur)
Hedef tablo:  ff_test_target (her test için oluşturulur ve silinir)

Kolon yapısı:
  id           INT          — sayısal kimlik
  name         VARCHAR(100) — metin adı
  created_date DATE         — tarih

Çalıştırma:
  FFENGINE_ENABLE_PG_TESTS=1 pytest tests/integration/test_pg_to_pg.py -v
"""

import os
import pytest


def _should_skip():
    if os.getenv("FFENGINE_ENABLE_PG_TESTS", "0").strip() != "1":
        return True, "FFENGINE_ENABLE_PG_TESTS=1 olmadığı için skip."
    return False, ""


_SKIP, _SKIP_REASON = _should_skip()

pytestmark = [pytest.mark.integration]
if _SKIP:
    pytestmark.append(pytest.mark.skip(reason=_SKIP_REASON))


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


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
def src_session(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession
    with DBSession(pg_conn_params, pg_dialect) as session:
        yield session


@pytest.fixture(scope="module")
def tgt_session(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession
    with DBSession(pg_conn_params, pg_dialect) as session:
        yield session


@pytest.fixture(autouse=True)
def setup_target(tgt_session):
    """Hedef tabloyu oluştur; test sonunda sil. Kaynak ff_test_data kalıcıdır."""
    cur = tgt_session.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS public.ff_test_target")
        tgt_session.conn.commit()
    except Exception:
        tgt_session.conn.rollback()
    cur.execute(
        "CREATE TABLE public.ff_test_target "
        "(id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL)"
    )
    tgt_session.conn.commit()
    cur.close()

    yield

    cur = tgt_session.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS public.ff_test_target")
        tgt_session.conn.commit()
    except Exception:
        tgt_session.conn.rollback()
    cur.close()


# ------------------------------------------------------------------
# Ortak task_config
# ------------------------------------------------------------------

_TASK_CONFIG = {
    "load_method": "append",
    "source_schema": "public",
    "source_table": "ff_test_data",
    "source_columns": ["id", "name", "created_date"],
    "target_schema": "public",
    "target_table": "ff_test_target",
    "target_columns": ["id", "name", "created_date"],
    "target_columns_meta": [],
    "batch_size": 1000,
}


# ------------------------------------------------------------------
# Test senaryoları
# ------------------------------------------------------------------


def test_pg_to_pg_row_count(src_session, tgt_session, pg_dialect):
    """ff_test_data → ff_test_target: 100 satır aktarılmalı."""
    from ffengine.core.etl_manager import ETLManager

    manager = ETLManager()
    result = manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    assert result.rows == 100
    assert result.errors == []

    cursor = tgt_session.cursor()
    cursor.execute("SELECT COUNT(*) FROM public.ff_test_target")
    count = cursor.fetchone()[0]
    cursor.close()
    assert count == 100


def test_pg_to_pg_data_integrity(src_session, tgt_session, pg_dialect):
    """İlk 5 satır id/name/created_date doğru sırayla aktarılmalı."""
    from ffengine.core.etl_manager import ETLManager

    ETLManager().run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    cursor = tgt_session.cursor()
    cursor.execute(
        "SELECT id, name FROM public.ff_test_target ORDER BY id LIMIT 5"
    )
    rows = cursor.fetchall()
    cursor.close()

    assert rows[0] == (1, "test_record_001")
    assert rows[1] == (2, "test_record_002")
    assert rows[4] == (5, "test_record_005")


def test_pg_to_pg_etl_result_fields(src_session, tgt_session, pg_dialect):
    """ETLResult alanları geçerli değerler içermeli."""
    from ffengine.core.etl_manager import ETLManager
    from ffengine.core.base_engine import ETLResult

    result = ETLManager().run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    assert isinstance(result, ETLResult)
    assert result.rows == 100
    assert result.partitions_completed == 1
    assert result.duration_seconds >= 0
    assert result.throughput >= 0


def test_pg_to_pg_date_column_transferred(src_session, tgt_session, pg_dialect):
    """created_date kolonu DATE türünde doğru aktarılmalı."""
    from ffengine.core.etl_manager import ETLManager
    import datetime

    ETLManager().run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    cursor = tgt_session.cursor()
    cursor.execute(
        "SELECT created_date FROM public.ff_test_target ORDER BY id LIMIT 1"
    )
    row = cursor.fetchone()
    cursor.close()

    assert isinstance(row[0], datetime.date)
