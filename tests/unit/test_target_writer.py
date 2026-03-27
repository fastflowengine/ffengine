import pytest
from unittest.mock import MagicMock, call, patch
from ffengine.pipeline.target_writer import TargetWriter
from ffengine.dialects.base import ColumnInfo
from ffengine.errors import ConnectionError, ValidationError


@pytest.fixture
def dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    d.generate_ddl.return_value = "CREATE TABLE ..."
    d.generate_bulk_insert_query.return_value = "INSERT INTO ..."
    return d


@pytest.fixture
def session():
    s = MagicMock()
    s.conn = MagicMock()
    cursor = MagicMock()
    s.cursor.return_value = cursor
    return s


@pytest.fixture
def writer(session, dialect):
    return TargetWriter(session, dialect)


# ------------------------------------------------------------------
# prepare() — load_method kararları
# ------------------------------------------------------------------


def test_prepare_append_no_ddl(writer, session):
    writer.prepare({"load_method": "append", "target_schema": "s", "target_table": "t", "target_columns_meta": []})
    writer.dialect.generate_ddl.assert_not_called()


def test_prepare_create_if_not_exists_calls_ddl_and_truncate(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare({
        "load_method": "create_if_not_exists_or_truncate",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": cols,
    })
    dialect.generate_ddl.assert_called_once()
    # TRUNCATE çağrıldı mı?
    executed_sqls = [c[0][0] for c in session.cursor.return_value.execute.call_args_list]
    assert any("TRUNCATE" in sql for sql in executed_sqls)


def test_prepare_replace_calls_drop_and_ddl(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare({
        "load_method": "replace",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": cols,
    })
    dialect.generate_ddl.assert_called_once()
    executed_sqls = [c[0][0] for c in session.cursor.return_value.execute.call_args_list]
    assert any("DROP" in sql for sql in executed_sqls)


def test_prepare_drop_if_exists_and_create(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare({
        "load_method": "drop_if_exists_and_create",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": cols,
    })
    dialect.generate_ddl.assert_called_once()


def test_prepare_delete_from_table_with_where(writer, session):
    writer.prepare({
        "load_method": "delete_from_table",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": [],
        "delete_where": "region = 'EU'",
    })
    executed_sqls = [c[0][0] for c in session.cursor.return_value.execute.call_args_list]
    assert any("DELETE" in sql and "region = 'EU'" in sql for sql in executed_sqls)


def test_prepare_delete_from_table_no_where(writer, session):
    writer.prepare({
        "load_method": "delete_from_table",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": [],
    })
    executed_sqls = [c[0][0] for c in session.cursor.return_value.execute.call_args_list]
    assert any("DELETE FROM" in sql for sql in executed_sqls)


def test_prepare_script_executes_sql(writer, session):
    writer.prepare({
        "load_method": "script",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": [],
        "script_sql": "EXEC sp_cleanup",
    })
    executed_sqls = [c[0][0] for c in session.cursor.return_value.execute.call_args_list]
    assert any("EXEC sp_cleanup" in sql for sql in executed_sqls)


def test_prepare_unsupported_load_method_raises(writer):
    with pytest.raises(ValidationError, match="Desteklenmeyen load_method"):
        writer.prepare({"load_method": "magic_load"})


def test_prepare_upsert_no_ddl(writer, session, dialect):
    writer.prepare({
        "load_method": "upsert",
        "target_schema": "s",
        "target_table": "t",
        "target_columns_meta": [],
    })
    dialect.generate_ddl.assert_not_called()


# ------------------------------------------------------------------
# write_batch()
# ------------------------------------------------------------------


def test_write_batch_returns_row_count(writer):
    rows = [(1, "a"), (2, "b"), (3, "c")]
    count = writer.write_batch(rows, {"target_schema": "s", "target_table": "t", "target_columns": ["id", "name"]})
    assert count == 3


def test_write_batch_calls_executemany(writer, session):
    rows = [(1,), (2,)]
    writer.write_batch(rows, {"target_schema": "s", "target_table": "t", "target_columns": ["id"]})
    session.cursor.return_value.executemany.assert_called_once()


def test_write_batch_commits(writer, session):
    writer.write_batch([(1,)], {"target_schema": "s", "target_table": "t", "target_columns": ["id"]})
    session.conn.commit.assert_called()


def test_write_batch_empty_rows(writer, session):
    count = writer.write_batch([], {"target_schema": "s", "target_table": "t", "target_columns": []})
    assert count == 0
    session.cursor.return_value.executemany.assert_not_called()


def test_write_batch_closes_cursor(writer, session):
    writer.write_batch([(1,)], {"target_schema": "s", "target_table": "t", "target_columns": ["id"]})
    session.cursor.return_value.close.assert_called()


def test_write_batch_wraps_db_error_as_connection_error(writer, session):
    session.cursor.return_value.executemany.side_effect = RuntimeError("db write failed")
    with pytest.raises(ConnectionError, match="Hedefe batch yazimi basarisiz"):
        writer.write_batch([(1,)], {"target_schema": "s", "target_table": "t", "target_columns": ["id"]})
    session.conn.rollback.assert_called_once()


# ------------------------------------------------------------------
# rollback_batch()
# ------------------------------------------------------------------


def test_rollback_batch_calls_conn_rollback(writer, session):
    writer.rollback_batch()
    session.conn.rollback.assert_called_once()


def test_rollback_batch_with_exception(writer, session):
    exc = ValueError("test")
    writer.rollback_batch(exc)
    session.conn.rollback.assert_called_once()
