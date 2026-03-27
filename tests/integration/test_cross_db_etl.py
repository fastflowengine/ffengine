"""
Cross-DB Integration Testleri: 9 Dialect Kombinasyonu

DURUM: C05 (YAML Config) + docker-compose.test.yml tüm DB'leri hazır olduğunda aktif edilecek.

Her test için gereken environment variables (.env):
  PG_TEST_HOST / PORT / USER / PASSWORD / DB
  MSSQL_TEST_HOST / PORT / USER / PASSWORD / DB
  ORACLE_TEST_HOST / PORT / USER / PASSWORD / DB (ORACLE_TEST_SERVICE)

docker-compose.test.yml servisleri:
  postgres-test   — postgresql:15
  mssql-test      — mcr.microsoft.com/mssql/server:2022-latest
  oracle-test     — gvenzl/oracle-free:23-slim (veya Oracle XE)

Çalıştırma (tüm servisler hazırken):
  docker compose -f docker/docker-compose.test.yml up -d
  pytest tests/integration/test_cross_db_etl.py -v

Kombinasyon matrisi:
  ┌─────────┬────────┬────────┬────────┐
  │  src\tgt│  PG    │ MSSQL  │ Oracle │
  ├─────────┼────────┼────────┼────────┤
  │  PG     │   ✓    │   ✓    │   ✓    │
  │  MSSQL  │   ✓    │   ✓    │   ✓    │
  │  Oracle │   ✓    │   ✓    │   ✓    │
  └─────────┴────────┴────────┴────────┘
"""

import os
import pytest

_REQUIRED_ENV = [
    "PG_TEST_HOST",
    "PG_TEST_PORT",
    "PG_TEST_USER",
    "PG_TEST_PASSWORD",
    "PG_TEST_DB",
    "MSSQL_TEST_HOST",
    "MSSQL_TEST_PORT",
    "MSSQL_TEST_USER",
    "MSSQL_TEST_PASSWORD",
    "MSSQL_TEST_DB",
    "ORACLE_TEST_HOST",
    "ORACLE_TEST_PORT",
    "ORACLE_TEST_USER",
    "ORACLE_TEST_PASSWORD",
    "ORACLE_TEST_SERVICE",
]


def _cross_db_enable_state() -> tuple[bool, str]:
    """
    C11_T02: Cross-DB integration test aktivasyon stratejisi.
    Varsayılan davranış skip; açık aktivasyon için env flag gerekir.
    """
    if os.getenv("FFENGINE_ENABLE_CROSS_DB_TESTS", "0").strip() != "1":
        return False, "FFENGINE_ENABLE_CROSS_DB_TESTS=1 olmadığı için skip."
    missing = [k for k in _REQUIRED_ENV if not (os.getenv(k) or "").strip()]
    if missing:
        return False, f"Eksik environment değişkenleri: {', '.join(missing)}"
    return True, ""


_ENABLED, _SKIP_REASON = _cross_db_enable_state()
pytestmark = [pytest.mark.integration]
if not _ENABLED:
    pytestmark.append(pytest.mark.skip(reason=_SKIP_REASON))

# ------------------------------------------------------------------
# Connection helpers
# ------------------------------------------------------------------


def _pg_params():
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5432")),
        "user": os.getenv("PG_TEST_USER", "ffengine"),
        "password": os.getenv("PG_TEST_PASSWORD", "ffengine"),
        "database": os.getenv("PG_TEST_DB", "ffengine_test"),
    }


def _mssql_params():
    return {
        "host": os.getenv("MSSQL_TEST_HOST", "localhost"),
        "port": int(os.getenv("MSSQL_TEST_PORT", "1433")),
        "user": os.getenv("MSSQL_TEST_USER", "sa"),
        "password": os.getenv("MSSQL_TEST_PASSWORD", "FFengine_Pass1!"),
        "database": os.getenv("MSSQL_TEST_DB", "ffengine_test"),
        "driver": os.getenv("MSSQL_TEST_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    }


def _oracle_params():
    return {
        "host": os.getenv("ORACLE_TEST_HOST", "localhost"),
        "port": int(os.getenv("ORACLE_TEST_PORT", "1521")),
        "user": os.getenv("ORACLE_TEST_USER", "ffengine"),
        "password": os.getenv("ORACLE_TEST_PASSWORD", "ffengine"),
        "database": os.getenv("ORACLE_TEST_SERVICE", "FREEPDB1"),
    }


_DB_PARAMS = {
    "postgres": _pg_params,
    "mssql": _mssql_params,
    "oracle": _oracle_params,
}

_DB_SCHEMA = {
    "postgres": "public",
    "mssql": "dbo",
    "oracle": "FFENGINE",
}


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(scope="module")
def all_sessions():
    """Tüm 3 DB için session dict döner {dialect_name: session}."""
    from ffengine.dialects import PostgresDialect, MSSQLDialect, OracleDialect
    from ffengine.db.session import DBSession

    dialects = {
        "postgres": PostgresDialect(),
        "mssql": MSSQLDialect(),
        "oracle": OracleDialect(),
    }

    sessions = {}
    for name, dialect in dialects.items():
        session = DBSession(_DB_PARAMS[name](), dialect)
        try:
            session.__enter__()
        except Exception as exc:
            # C11_T02: Aktivasyon açık olsa da ortam bağımlılığı eksikse test suite
            # kırmızıya düşmek yerine anlamlı reason ile skip edilir.
            pytest.skip(f"{name} test bağlantısı kurulamadı: {type(exc).__name__}: {exc}")
        sessions[name] = (session, dialect)

    yield sessions

    for session, _ in sessions.values():
        session.__exit__(None, None, None)


@pytest.fixture(autouse=True)
def setup_source_tables(all_sessions):
    """Her DB'de kaynak tabloyu oluştur ve test verisi ekle."""
    test_rows = [(1, "Alice", 100.50), (2, "Bob", 200.00), (3, "Carol", 300.75)]

    for db_name, (session, dialect) in all_sessions.items():
        schema = _DB_SCHEMA[db_name]
        cur = session.cursor()
        # Kaynak tablo
        try:
            cur.execute(f"DROP TABLE IF EXISTS {dialect.quote_identifier(schema)}.{dialect.quote_identifier('cross_src')}")
            session.conn.commit()
        except Exception:
            session.conn.rollback()
        cur.execute(
            f"CREATE TABLE {dialect.quote_identifier(schema)}.{dialect.quote_identifier('cross_src')} "
            f"(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
        )
        session.conn.commit()
        sql = dialect.generate_bulk_insert_query(
            f"{dialect.quote_identifier(schema)}.{dialect.quote_identifier('cross_src')}",
            ["id", "name", "amount"],
        )
        cur.executemany(sql, test_rows)
        session.conn.commit()
        # Hedef tablo
        try:
            cur.execute(f"DROP TABLE IF EXISTS {dialect.quote_identifier(schema)}.{dialect.quote_identifier('cross_tgt')}")
            session.conn.commit()
        except Exception:
            session.conn.rollback()
        cur.execute(
            f"CREATE TABLE {dialect.quote_identifier(schema)}.{dialect.quote_identifier('cross_tgt')} "
            f"(id INT, name VARCHAR(100), amount NUMERIC(10,2))"
        )
        session.conn.commit()
        cur.close()

    yield

    # Teardown — test tablolarını sil
    for db_name, (session, dialect) in all_sessions.items():
        schema = _DB_SCHEMA[db_name]
        for tbl in ("cross_src", "cross_tgt"):
            try:
                cur = session.cursor()
                cur.execute(
                    f"DROP TABLE IF EXISTS {dialect.quote_identifier(schema)}.{dialect.quote_identifier(tbl)}"
                )
                session.conn.commit()
                cur.close()
            except Exception:
                session.conn.rollback()


# ------------------------------------------------------------------
# Ortak test fonksiyonu
# ------------------------------------------------------------------


def _run_cross_etl(src_name, tgt_name, all_sessions):
    from ffengine.core.etl_manager import ETLManager
    from ffengine.db.session import DBSession

    src_session, src_dialect = all_sessions[src_name]
    tgt_session, tgt_dialect = all_sessions[tgt_name]

    task_config = {
        "load_method": "append",
        "source_schema": _DB_SCHEMA[src_name],
        "source_table": "cross_src",
        "source_columns": ["id", "name", "amount"],
        "target_schema": _DB_SCHEMA[tgt_name],
        "target_table": "cross_tgt",
        "target_columns": ["id", "name", "amount"],
        "target_columns_meta": [],
        "batch_size": 1000,
    }

    manager = ETLManager()
    # Self-transfer senaryosunda (PG->PG gibi) kaynak ve hedef için ayrı connection
    # kullanılır; tek connection üzerinde server-side cursor + write çatışmasını önler.
    if src_name == tgt_name:
        tgt_session_alt = DBSession(_DB_PARAMS[tgt_name](), type(tgt_dialect)())
        tgt_session_alt.__enter__()
        try:
            return manager.run_etl_task(
                src_session=src_session,
                tgt_session=tgt_session_alt,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
                task_config=task_config,
            )
        finally:
            tgt_session_alt.__exit__(None, None, None)

    return manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=src_dialect,
        tgt_dialect=tgt_dialect,
        task_config=task_config,
    )


# ------------------------------------------------------------------
# PG → * (3 test)
# ------------------------------------------------------------------


def test_pg_to_pg(all_sessions):
    result = _run_cross_etl("postgres", "postgres", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_pg_to_mssql(all_sessions):
    result = _run_cross_etl("postgres", "mssql", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_pg_to_oracle(all_sessions):
    result = _run_cross_etl("postgres", "oracle", all_sessions)
    assert result.rows == 3
    assert result.errors == []


# ------------------------------------------------------------------
# MSSQL → * (3 test)
# ------------------------------------------------------------------


def test_mssql_to_pg(all_sessions):
    result = _run_cross_etl("mssql", "postgres", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_mssql_to_mssql(all_sessions):
    result = _run_cross_etl("mssql", "mssql", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_mssql_to_oracle(all_sessions):
    result = _run_cross_etl("mssql", "oracle", all_sessions)
    assert result.rows == 3
    assert result.errors == []


# ------------------------------------------------------------------
# Oracle → * (3 test)
# ------------------------------------------------------------------


def test_oracle_to_pg(all_sessions):
    result = _run_cross_etl("oracle", "postgres", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_oracle_to_mssql(all_sessions):
    result = _run_cross_etl("oracle", "mssql", all_sessions)
    assert result.rows == 3
    assert result.errors == []


def test_oracle_to_oracle(all_sessions):
    result = _run_cross_etl("oracle", "oracle", all_sessions)
    assert result.rows == 3
    assert result.errors == []
