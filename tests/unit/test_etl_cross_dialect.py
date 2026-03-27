"""
Cross-Dialect ETL Unit Testleri

9 kombinasyonun tümünü gerçek dialect nesneleri + mock session ile test eder:
  PG→PG  PG→MSSQL  PG→Oracle
  MSSQL→PG  MSSQL→MSSQL  MSSQL→Oracle
  Oracle→PG  Oracle→MSSQL  Oracle→Oracle

Her kombinasyon için doğrulananlar:
  1. ETLResult.rows doğru döner
  2. INSERT SQL'inde hedef dialect'ın quoting kuralı uygulanmış
  3. SELECT SQL'inde kaynak dialect'ın quoting kuralı uygulanmış
  4. ETLResult alanları geçerli (duration >= 0, throughput >= 0, errors == [])
  5. Aynı DB üzerine self-transfer (PG→PG, MSSQL→MSSQL, Oracle→Oracle)
  6. Rollback doğru dialect session'ına iletilir
"""

import pytest
from unittest.mock import MagicMock, call

from ffengine.core.etl_manager import ETLManager
from ffengine.core.base_engine import ETLResult
from ffengine.dialects import PostgresDialect, MSSQLDialect, OracleDialect
from ffengine.errors.exceptions import FFEngineError

# ------------------------------------------------------------------
# Test parametreleri
# ------------------------------------------------------------------

_DIALECT_CLASSES = {
    "postgres": PostgresDialect,
    "mssql": MSSQLDialect,
    "oracle": OracleDialect,
}

# Hedef dialect'a göre beklenen sütun quoting (sütun adı "id")
_EXPECTED_COL_QUOTE = {
    "postgres": '"id"',
    "mssql": "[id]",
    "oracle": '"ID"',
}

# Hedef dialect'a göre beklenen tablo quoting (schema="tgt", table="employees")
_EXPECTED_TBL_QUOTE = {
    "postgres": '"tgt"."employees"',
    "mssql": "[tgt].[employees]",
    "oracle": '"TGT"."EMPLOYEES"',
}

# Kaynak dialect'a göre beklenen SELECT'te tablo quoting
_EXPECTED_SRC_TBL_QUOTE = {
    "postgres": '"src"."employees"',
    "mssql": "[src].[employees]",
    "oracle": '"SRC"."EMPLOYEES"',
}

CROSS_PAIRS = [
    (src, tgt)
    for src in _DIALECT_CLASSES
    for tgt in _DIALECT_CLASSES
]

SELF_PAIRS = [("postgres", "postgres"), ("mssql", "mssql"), ("oracle", "oracle")]

CROSS_ONLY_PAIRS = [p for p in CROSS_PAIRS if p[0] != p[1]]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_src_session(rows=None):
    """Mock kaynak session: iki satır döndürür, sonra boş."""
    if rows is None:
        rows = [(1, "Alice"), (2, "Bob")]
    session = MagicMock()
    session.conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchmany.side_effect = [rows, []]
    session.cursor.return_value = cursor
    return session


def _make_tgt_session():
    """Mock hedef session: executemany başarılı."""
    session = MagicMock()
    session.conn = MagicMock()
    cursor = MagicMock()
    session.cursor.return_value = cursor
    return session


def _run(src_name, tgt_name, task_config=None, src_rows=None):
    """Verilen dialect çifti için ETLManager.run_etl_task() çalıştır."""
    src_dialect = _DIALECT_CLASSES[src_name]()
    tgt_dialect = _DIALECT_CLASSES[tgt_name]()
    src_session = _make_src_session(src_rows)
    tgt_session = _make_tgt_session()

    if task_config is None:
        task_config = {
            "load_method": "append",
            "source_schema": "src",
            "source_table": "employees",
            "source_columns": ["id", "name"],
            "target_schema": "tgt",
            "target_table": "employees",
            "target_columns": ["id", "name"],
            "target_columns_meta": [],
            "batch_size": 100,
        }

    manager = ETLManager()
    result = manager.run_etl_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=src_dialect,
        tgt_dialect=tgt_dialect,
        task_config=task_config,
    )
    return result, src_session, tgt_session


# ------------------------------------------------------------------
# 1. Row count — tüm 9 kombinasyon
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_row_count(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert result.rows == 2, f"{src_name}→{tgt_name}: beklenen 2 satır, gelen {result.rows}"


# ------------------------------------------------------------------
# 2. Hedef dialect quoting — INSERT SQL
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_target_col_quoting(src_name, tgt_name):
    """INSERT SQL hedef dialect'ın sütun quoting kuralını uygulamalı."""
    _, _, tgt_session = _run(src_name, tgt_name)
    cursor = tgt_session.cursor.return_value
    insert_sql = cursor.executemany.call_args[0][0]
    expected = _EXPECTED_COL_QUOTE[tgt_name]
    assert expected in insert_sql, (
        f"{src_name}→{tgt_name}: INSERT SQL'de '{expected}' bekleniyor, "
        f"gerçek SQL: {insert_sql!r}"
    )


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_target_table_quoting(src_name, tgt_name):
    """INSERT SQL hedef tablo adını hedef dialect ile quote etmeli."""
    _, _, tgt_session = _run(src_name, tgt_name)
    cursor = tgt_session.cursor.return_value
    insert_sql = cursor.executemany.call_args[0][0]
    expected = _EXPECTED_TBL_QUOTE[tgt_name]
    assert expected in insert_sql, (
        f"{src_name}→{tgt_name}: INSERT SQL'de '{expected}' bekleniyor, "
        f"gerçek SQL: {insert_sql!r}"
    )


# ------------------------------------------------------------------
# 3. Kaynak dialect quoting — SELECT SQL
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_source_table_quoting(src_name, tgt_name):
    """SELECT SQL kaynak tabloyu kaynak dialect ile quote etmeli."""
    _, src_session, _ = _run(src_name, tgt_name)
    cursor = src_session.cursor.return_value
    select_sql = cursor.execute.call_args[0][0]
    expected = _EXPECTED_SRC_TBL_QUOTE[src_name]
    assert expected in select_sql, (
        f"{src_name}→{tgt_name}: SELECT SQL'de '{expected}' bekleniyor, "
        f"gerçek SQL: {select_sql!r}"
    )


# ------------------------------------------------------------------
# 4. ETLResult alanları geçerli — tüm 9 kombinasyon
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_etl_result_type(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert isinstance(result, ETLResult)


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_etl_result_no_errors(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert result.errors == []


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_etl_result_duration_positive(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert result.duration_seconds >= 0


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_etl_result_throughput_non_negative(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert result.throughput >= 0


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_partitions_completed(src_name, tgt_name):
    result, _, _ = _run(src_name, tgt_name)
    assert result.partitions_completed == 1


# ------------------------------------------------------------------
# 5. Self-transfer — aynı DB üzerinde farklı tablo
# ------------------------------------------------------------------


@pytest.mark.parametrize("dialect_name", ["postgres", "mssql", "oracle"])
def test_self_transfer_row_count(dialect_name):
    """Aynı DB'nin farklı iki tablosu arasında transfer çalışmalı."""
    result, _, _ = _run(dialect_name, dialect_name)
    assert result.rows == 2


@pytest.mark.parametrize("dialect_name", ["postgres", "mssql", "oracle"])
def test_self_transfer_src_and_tgt_use_same_quoting(dialect_name):
    """Self-transfer'da hem SELECT hem INSERT aynı dialect kuralını kullanmalı."""
    _, src_session, tgt_session = _run(dialect_name, dialect_name)

    select_sql = src_session.cursor.return_value.execute.call_args[0][0]
    insert_sql = tgt_session.cursor.return_value.executemany.call_args[0][0]

    src_quote = _EXPECTED_SRC_TBL_QUOTE[dialect_name]
    tgt_quote = _EXPECTED_TBL_QUOTE[dialect_name]

    assert src_quote in select_sql
    assert tgt_quote in insert_sql


# ------------------------------------------------------------------
# 6. Rollback doğru session'a iletilir
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_rollback_on_write_error(src_name, tgt_name):
    """Hedef yazma hatası → hedef session'ın rollback'i çağrılmalı."""
    src_dialect = _DIALECT_CLASSES[src_name]()
    tgt_dialect = _DIALECT_CLASSES[tgt_name]()
    src_session = _make_src_session()
    tgt_session = _make_tgt_session()
    tgt_session.cursor.return_value.executemany.side_effect = RuntimeError("db error")

    task_config = {
        "load_method": "append",
        "source_schema": "src",
        "source_table": "employees",
        "source_columns": ["id", "name"],
        "target_schema": "tgt",
        "target_table": "employees",
        "target_columns": ["id", "name"],
        "target_columns_meta": [],
    }

    manager = ETLManager()
    with pytest.raises(FFEngineError):
        manager.run_etl_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=src_dialect,
            tgt_dialect=tgt_dialect,
            task_config=task_config,
        )

    tgt_session.conn.rollback.assert_called()


# ------------------------------------------------------------------
# 7. Boş kaynak — tüm kombinasyonlar
# ------------------------------------------------------------------


@pytest.mark.parametrize("src_name,tgt_name", CROSS_PAIRS)
def test_cross_dialect_empty_source(src_name, tgt_name):
    """Kaynak tablo boşsa ETLResult.rows == 0, INSERT çağrılmamalı."""
    result, _, tgt_session = _run(src_name, tgt_name, src_rows=[])
    assert result.rows == 0
    tgt_session.cursor.return_value.executemany.assert_not_called()
