"""
Cross-DB Integration Testleri: 9 Dialect Kombinasyonu

Kaynak tablo: ff_test_data (kalıcı, 100 satır — her DB'de önceden oluşturulmuş)
  Kolonlar: id (INT/NUMBER), name (VARCHAR/VARCHAR2), created_date (DATE)
Hedef tablo: ff_test_target (her test için oluşturulur ve silinir)

Kombinasyon matrisi:
  ┌─────────┬────────┬────────┬────────┐
  │  src\tgt│  PG    │ MSSQL  │ Oracle │
  ├─────────┼────────┼────────┼────────┤
  │  PG     │   ✓    │   ✓    │   ✓    │
  │  MSSQL  │   ✓    │   ✓    │   ✓    │
  │  Oracle │   ✓    │   ✓    │   ✓    │
  └─────────┴────────┴────────┴────────┘

Çalıştırma:
  FFENGINE_ENABLE_CROSS_DB_TESTS=1 pytest tests/integration/test_cross_db_etl.py -v
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
# Connection helpers — container varsayılan değerleri
# ------------------------------------------------------------------


def _pg_params():
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5435")),
        "user": os.getenv("PG_TEST_USER", "ffengine_test"),
        "password": os.getenv("PG_TEST_PASSWORD", "ffengine_pg_pass"),
        "database": os.getenv("PG_TEST_DB", "ffengine_test_db"),
    }


def _mssql_params():
    return {
        "host": os.getenv("MSSQL_TEST_HOST", "localhost"),
        "port": int(os.getenv("MSSQL_TEST_PORT", "1433")),
        "user": os.getenv("MSSQL_TEST_USER", "sa"),
        "password": os.getenv("MSSQL_TEST_PASSWORD", "Mssql_password123!"),
        "database": os.getenv("MSSQL_TEST_DB", "ffengine_test"),
        "driver": os.getenv("MSSQL_TEST_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    }


def _oracle_params():
    return {
        "host": os.getenv("ORACLE_TEST_HOST", "localhost"),
        "port": int(os.getenv("ORACLE_TEST_PORT", "1521")),
        "user": os.getenv("ORACLE_TEST_USER", "ffengine"),
        "password": os.getenv("ORACLE_TEST_PASSWORD", "Oracle_password123!"),
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

# Dialect-specific CREATE TABLE DDL for ff_test_target
_TARGET_DDL = {
    "postgres": "id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL",
    "mssql": "id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL",
    "oracle": "id NUMBER NOT NULL, name VARCHAR2(100) NOT NULL, created_date DATE NOT NULL",
}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _safe_drop(cur, session, db_name: str, fqn: str) -> None:
    """Tabloyu dialect uyumlu şekilde sil; hata olursa rollback yap."""
    if db_name == "oracle":
        # Oracle'da IF EXISTS desteklenmez; PL/SQL bloğu kullanılır
        sql = f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {fqn}'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    else:
        sql = f"DROP TABLE IF EXISTS {fqn}"
    try:
        cur.execute(sql)
        session.conn.commit()
    except Exception:
        session.conn.rollback()


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(scope="module")
def all_sessions():
    """Tüm 3 DB için session dict döner: {dialect_name: (session, dialect)}."""
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
            pytest.skip(f"{name} bağlantısı kurulamadı: {type(exc).__name__}: {exc}")
        sessions[name] = (session, dialect)

    yield sessions

    for session, _ in sessions.values():
        session.__exit__(None, None, None)


@pytest.fixture(autouse=True)
def setup_target_tables(all_sessions):
    """Her DB'de ff_test_target hedef tablosunu oluştur; sonunda sil.
    Kaynak ff_test_data kalıcıdır — oluşturulmaz/silinmez."""
    # Önceki test'ten kalan açık veya hatalı transaction'ları temizle
    for _, (session, _) in all_sessions.items():
        try:
            session.conn.rollback()
        except Exception:
            pass

    for db_name, (session, dialect) in all_sessions.items():
        schema = _DB_SCHEMA[db_name]
        q_schema = dialect.quote_identifier(schema)
        q_tgt = dialect.quote_identifier("ff_test_target")
        fqn = f"{q_schema}.{q_tgt}"

        cur = session.cursor()
        _safe_drop(cur, session, db_name, fqn)
        cur.execute(
            f"CREATE TABLE {fqn} ({_TARGET_DDL[db_name]})"
        )
        session.conn.commit()
        cur.close()

    yield

    for db_name, (session, dialect) in all_sessions.items():
        schema = _DB_SCHEMA[db_name]
        q_schema = dialect.quote_identifier(schema)
        q_tgt = dialect.quote_identifier("ff_test_target")
        fqn = f"{q_schema}.{q_tgt}"
        cur = session.cursor()
        _safe_drop(cur, session, db_name, fqn)
        cur.close()


# ------------------------------------------------------------------
# Ortak ETL çalıştırıcı
# ------------------------------------------------------------------


def _run_cross_etl(src_name: str, tgt_name: str, all_sessions):
    from ffengine.core.etl_manager import ETLManager
    from ffengine.db.session import DBSession

    src_session, src_dialect = all_sessions[src_name]
    tgt_session, tgt_dialect = all_sessions[tgt_name]

    task_config = {
        "load_method": "append",
        "source_schema": _DB_SCHEMA[src_name],
        "source_table": "ff_test_data",
        "source_columns": ["id", "name", "created_date"],
        "target_schema": _DB_SCHEMA[tgt_name],
        "target_table": "ff_test_target",
        "target_columns": ["id", "name", "created_date"],
        "target_columns_meta": [],
        "batch_size": 1000,
    }

    manager = ETLManager()
    # Aynı DB self-transfer: kaynak cursor ile hedef write çatışmaması için ayrı bağlantı
    if src_name == tgt_name:
        tgt_alt = DBSession(_DB_PARAMS[tgt_name](), type(tgt_dialect)())
        tgt_alt.__enter__()
        try:
            return manager.run_etl_task(
                src_session=src_session,
                tgt_session=tgt_alt,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
                task_config=task_config,
            )
        finally:
            tgt_alt.__exit__(None, None, None)

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
    assert result.rows == 100
    assert result.errors == []


def test_pg_to_mssql(all_sessions):
    result = _run_cross_etl("postgres", "mssql", all_sessions)
    assert result.rows == 100
    assert result.errors == []


def test_pg_to_oracle(all_sessions):
    result = _run_cross_etl("postgres", "oracle", all_sessions)
    assert result.rows == 100
    assert result.errors == []


# ------------------------------------------------------------------
# MSSQL → * (3 test)
# ------------------------------------------------------------------


def test_mssql_to_pg(all_sessions):
    result = _run_cross_etl("mssql", "postgres", all_sessions)
    assert result.rows == 100
    assert result.errors == []


def test_mssql_to_mssql(all_sessions):
    result = _run_cross_etl("mssql", "mssql", all_sessions)
    assert result.rows == 100
    assert result.errors == []


def test_mssql_to_oracle(all_sessions):
    result = _run_cross_etl("mssql", "oracle", all_sessions)
    assert result.rows == 100
    assert result.errors == []


# ------------------------------------------------------------------
# Oracle → * (3 test)
# ------------------------------------------------------------------


def test_oracle_to_pg(all_sessions):
    result = _run_cross_etl("oracle", "postgres", all_sessions)
    assert result.rows == 100
    assert result.errors == []


def test_oracle_to_mssql(all_sessions):
    result = _run_cross_etl("oracle", "mssql", all_sessions)
    assert result.rows == 100
    assert result.errors == []


def test_oracle_to_oracle(all_sessions):
    result = _run_cross_etl("oracle", "oracle", all_sessions)
    assert result.rows == 100
    assert result.errors == []
