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
    from ffengine.core.flow_manager import FlowManager

    manager = FlowManager()
    result = manager.run_flow_task(
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
    from ffengine.core.flow_manager import FlowManager

    FlowManager().run_flow_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    src_cursor = src_session.cursor()
    src_cursor.execute(
        "SELECT id, name FROM public.ff_test_data ORDER BY id LIMIT 5"
    )
    expected_rows = src_cursor.fetchall()
    src_cursor.close()

    tgt_cursor = tgt_session.cursor()
    tgt_cursor.execute(
        "SELECT id, name FROM public.ff_test_target ORDER BY id LIMIT 5"
    )
    actual_rows = tgt_cursor.fetchall()
    tgt_cursor.close()

    assert actual_rows == expected_rows


def test_pg_to_pg_etl_result_fields(src_session, tgt_session, pg_dialect):
    """FlowResult alanları geçerli değerler içermeli."""
    from ffengine.core.flow_manager import FlowManager
    from ffengine.core.base_engine import FlowResult

    result = FlowManager().run_flow_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_TASK_CONFIG,
    )

    assert isinstance(result, FlowResult)
    assert result.rows == 100
    assert result.partitions_completed == 1
    assert result.duration_seconds >= 0
    assert result.throughput >= 0


def test_pg_to_pg_date_column_transferred(src_session, tgt_session, pg_dialect):
    """created_date kolonu DATE türünde doğru aktarılmalı."""
    from ffengine.core.flow_manager import FlowManager
    import datetime

    FlowManager().run_flow_task(
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


def test_pg_incremental_datetime_precision_safe_no_loss(src_session, tgt_session, pg_dialect):
    """Fractional-second precision farklarinda filtrelenen kaynak sayisi ile transfer edilen satir sayisi ayni olmali."""
    from ffengine.config.binding_resolver import BindingResolver
    from ffengine.core.flow_manager import FlowManager

    cur = tgt_session.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_src")
        cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_tgt")
        cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_watermark")
        cur.execute(
            "CREATE TABLE public.ff_test_ts_src "
            "(id INT NOT NULL, event_ts TIMESTAMP(6) NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE public.ff_test_ts_tgt "
            "(id INT NOT NULL, event_ts TIMESTAMP(6) NOT NULL)"
        )
        cur.execute(
            "CREATE TABLE public.ff_test_ts_watermark "
            "(event_ts TIMESTAMP(6) NOT NULL)"
        )
        cur.execute(
            "INSERT INTO public.ff_test_ts_src (id, event_ts) VALUES "
            "(1, TIMESTAMP '2026-01-01 00:00:00.120000'),"
            "(2, TIMESTAMP '2026-01-01 00:00:00.120001'),"
            "(3, TIMESTAMP '2026-01-01 00:00:00.123450'),"
            "(4, TIMESTAMP '2026-01-01 00:00:00.123456')"
        )
        cur.execute(
            "INSERT INTO public.ff_test_ts_watermark (event_ts) VALUES "
            "(TIMESTAMP '2026-01-01 00:00:00.120000')"
        )
        tgt_session.conn.commit()

        cfg = {
            "load_method": "append",
            "source_schema": "public",
            "source_table": "ff_test_ts_src",
            "source_columns": ["id", "event_ts"],
            "target_schema": "public",
            "target_table": "ff_test_ts_tgt",
            "target_columns": ["id", "event_ts"],
            "target_columns_meta": [],
            "batch_size": 1000,
            "where": (
                '"event_ts" >= :wm_value '
                "AND \"event_ts\" < TIMESTAMP '2026-01-01 00:00:01.000000'"
            ),
            "bindings": [
                {
                    "variable_name": "wm_value",
                    "binding_source": "target",
                    "sql": (
                        "SELECT COALESCE(MAX(event_ts)::timestamp(6), "
                        "TIMESTAMP '1900-01-01 00:00:00.000000') "
                        "FROM public.ff_test_ts_watermark"
                    ),
                }
            ],
        }
        resolved = BindingResolver().resolve_sql_bindings(
            cfg,
            context={},
            source_session=src_session,
            target_session=tgt_session,
        )
        assert "TIMESTAMP '2026-01-01 00:00:00.120000'" in resolved["_resolved_where"]

        result = FlowManager().run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=pg_dialect,
            tgt_dialect=pg_dialect,
            task_config=resolved,
        )

        check_cur = tgt_session.cursor()
        check_cur.execute("SELECT COUNT(*) FROM public.ff_test_ts_tgt")
        transferred = check_cur.fetchone()[0]
        check_cur.execute(
            "SELECT COUNT(*) FROM public.ff_test_ts_src "
            f"WHERE {resolved['_resolved_where']}"
        )
        filtered_source = check_cur.fetchone()[0]
        check_cur.close()

        assert result.rows == filtered_source
        assert transferred == filtered_source
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_src")
            cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_tgt")
            cur.execute("DROP TABLE IF EXISTS public.ff_test_ts_watermark")
            tgt_session.conn.commit()
        except Exception:
            tgt_session.conn.rollback()
        cur.close()
