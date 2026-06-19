import sys
import types
import pytest
from unittest.mock import MagicMock, patch
from ffengine.pipeline.target_writer import TargetWriter
from ffengine.dialects.base import ColumnInfo
from ffengine.errors import ConnectionError, ValidationError


@pytest.fixture
def dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    d.generate_ddl.return_value = "CREATE TABLE ..."
    d.generate_bulk_insert_query.return_value = "INSERT INTO ..."
    d.generate_upsert_query.return_value = "UPSERT INTO ..."
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
    writer.prepare(
        {
            "load_method": "append",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": [],
        }
    )
    writer.dialect.generate_ddl.assert_not_called()


def test_prepare_create_if_not_exists_calls_ddl_and_truncate(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare(
        {
            "load_method": "create_if_not_exists_or_truncate",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": cols,
        }
    )
    dialect.generate_ddl.assert_called_once()
    # TRUNCATE çağrıldı mı?
    executed_sqls = [
        c[0][0] for c in session.cursor.return_value.execute.call_args_list
    ]
    assert any("TRUNCATE" in sql for sql in executed_sqls)


def test_prepare_create_if_not_exists_never_drops_existing_table(
    writer, session, dialect
):
    cols = [ColumnInfo("airport_name", "VARCHAR2", False, 140, None)]

    writer.prepare(
        {
            "load_method": "create_if_not_exists_or_truncate",
            "target_schema": "FFENGINE",
            "target_table": "FFE_AIRPORTS",
            "target_columns_meta": cols,
        }
    )

    executed_sqls = [
        c[0][0] for c in session.cursor.return_value.execute.call_args_list
    ]
    assert not any("DROP TABLE IF EXISTS" in sql for sql in executed_sqls)
    assert any("TRUNCATE" in sql for sql in executed_sqls)


def test_prepare_replace_uses_oracle_drop_block_when_needed(session):
    class OracleDialectStub:
        def quote_identifier(self, name):
            return f'"{name}"'

        def generate_ddl(self, *_args, **_kwargs):
            return "CREATE TABLE ..."

        def get_table_schema(self, *_args, **_kwargs):
            return [ColumnInfo("AIRPORT_NAME", "VARCHAR2", False, 4000, None)]

    writer = TargetWriter(session, OracleDialectStub())
    writer.prepare(
        {
            "load_method": "replace",
            "target_schema": "FFENGINE",
            "target_table": "FFE_AIRPORTS",
            "target_columns_meta": [
                ColumnInfo("airport_name", "VARCHAR2", False, 140, None)
            ],
        }
    )
    executed_sqls = [
        c[0][0] for c in session.cursor.return_value.execute.call_args_list
    ]
    assert any("EXECUTE IMMEDIATE 'DROP TABLE" in sql for sql in executed_sqls)
    assert any("SQLCODE != -942" in sql for sql in executed_sqls)


def test_prepare_replace_calls_drop_and_ddl(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare(
        {
            "load_method": "replace",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": cols,
        }
    )
    dialect.generate_ddl.assert_called_once()
    executed_sqls = [
        c[0][0] for c in session.cursor.return_value.execute.call_args_list
    ]
    assert any("DROP" in sql for sql in executed_sqls)


def test_prepare_drop_if_exists_and_create(writer, session, dialect):
    cols = [ColumnInfo("id", "INT", False)]
    writer.prepare(
        {
            "load_method": "drop_if_exists_and_create",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": cols,
        }
    )
    dialect.generate_ddl.assert_called_once()


@pytest.mark.parametrize(
    "load_method",
    ["create_if_not_exists_or_truncate", "replace", "drop_if_exists_and_create", "upsert"],
)
def test_prepare_rejects_empty_columns_meta_for_ddl_methods(
    writer, session, dialect, load_method
):
    with pytest.raises(ValidationError, match="target_columns_meta bos olamaz"):
        writer.prepare(
            {
                "load_method": load_method,
                "target_schema": "s",
                "target_table": "t",
                "target_columns_meta": [],
            }
        )
    dialect.generate_ddl.assert_not_called()
    session.cursor.return_value.execute.assert_not_called()


def test_prepare_replace_raises_when_drop_fails(writer, session, dialect):
    session.cursor.return_value.execute.side_effect = RuntimeError("drop failed")
    with pytest.raises(ConnectionError, match="Hedef tablo drop islemi basarisiz"):
        writer.prepare(
            {
                "load_method": "replace",
                "target_schema": "s",
                "target_table": "t",
                "target_columns_meta": [ColumnInfo("id", "INT", False)],
            }
        )
    session.conn.rollback.assert_called_once()
    dialect.generate_ddl.assert_not_called()


def test_prepare_script_executes_sql(writer, session):
    writer.prepare(
        {
            "load_method": "script",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": [],
            "script_sql": "EXEC sp_cleanup",
        }
    )
    executed_sqls = [
        c[0][0] for c in session.cursor.return_value.execute.call_args_list
    ]
    assert any("EXEC sp_cleanup" in sql for sql in executed_sqls)


def test_prepare_unsupported_load_method_raises(writer):
    with pytest.raises(ValidationError, match="Desteklenmeyen load_method"):
        writer.prepare({"load_method": "magic_load"})


def test_prepare_upsert_calls_ddl(writer, session, dialect):
    writer.prepare(
        {
            "load_method": "upsert",
            "target_schema": "s",
            "target_table": "t",
            "target_columns_meta": [ColumnInfo("id", "INT", False)],
        }
    )
    dialect.generate_ddl.assert_called_once()


# ------------------------------------------------------------------
# write_batch()
# ------------------------------------------------------------------


def test_write_batch_returns_row_count(writer):
    rows = [(1, "a"), (2, "b"), (3, "c")]
    count = writer.write_batch(
        rows,
        {"target_schema": "s", "target_table": "t", "target_columns": ["id", "name"]},
    )
    assert count == 3


def test_write_batch_calls_executemany(writer, session):
    rows = [(1,), (2,)]
    writer.write_batch(
        rows, {"target_schema": "s", "target_table": "t", "target_columns": ["id"]}
    )
    session.cursor.return_value.executemany.assert_called_once()


def test_write_batch_upsert_calls_generate_upsert_query(writer, session, dialect):
    rows = [(1, "x"), (2, "y")]
    writer.write_batch(
        rows,
        {
            "load_method": "upsert",
            "target_schema": "s",
            "target_table": "t",
            "target_columns": ["id", "name"],
            "upsert_match_columns": ["id"],
        },
    )
    dialect.generate_upsert_query.assert_called_once_with(
        '"s"."t"',
        ["id", "name"],
        ["id"],
        ["name"],
    )
    session.cursor.return_value.executemany.assert_called_once()


def test_write_batch_commits(writer, session):
    writer.write_batch(
        [(1,)], {"target_schema": "s", "target_table": "t", "target_columns": ["id"]}
    )
    session.conn.commit.assert_called()


def test_write_batch_empty_rows(writer, session):
    count = writer.write_batch(
        [], {"target_schema": "s", "target_table": "t", "target_columns": []}
    )
    assert count == 0
    session.cursor.return_value.executemany.assert_not_called()


def test_write_batch_rejects_empty_target_columns(writer, session):
    with pytest.raises(ValidationError, match="target_columns bos olamaz"):
        writer.write_batch(
            [(1,)],
            {"target_schema": "s", "target_table": "t", "target_columns": []},
        )
    session.cursor.return_value.executemany.assert_not_called()


def test_write_batch_closes_cursor(writer, session):
    writer.write_batch(
        [(1,)], {"target_schema": "s", "target_table": "t", "target_columns": ["id"]}
    )
    session.cursor.return_value.close.assert_called()


def test_write_batch_wraps_db_error_as_connection_error(writer, session):
    session.cursor.return_value.executemany.side_effect = RuntimeError(
        "db write failed"
    )
    with pytest.raises(ConnectionError, match="Hedefe batch yazimi basarisiz"):
        writer.write_batch(
            [(1,)],
            {"target_schema": "s", "target_table": "t", "target_columns": ["id"]},
        )
    session.conn.rollback.assert_called_once()


def test_write_batch_upsert_requires_match_columns(writer):
    with pytest.raises(ValidationError, match="upsert_match_columns"):
        writer.write_batch(
            [(1, "x")],
            {
                "load_method": "upsert",
                "target_schema": "s",
                "target_table": "t",
                "target_columns": ["id", "name"],
            },
        )


def test_write_batch_upsert_rejects_unknown_match_column(writer):
    with pytest.raises(ValidationError, match="target_columns alt kumesi"):
        writer.write_batch(
            [(1, "x")],
            {
                "load_method": "upsert",
                "target_schema": "s",
                "target_table": "t",
                "target_columns": ["id", "name"],
                "upsert_match_columns": ["missing_col"],
            },
        )


def test_write_batch_upsert_rejects_null_match_value(writer):
    with pytest.raises(ValidationError, match="NULL deger olamaz"):
        writer.write_batch(
            [(None, "x")],
            {
                "load_method": "upsert",
                "target_schema": "s",
                "target_table": "t",
                "target_columns": ["id", "name"],
                "upsert_match_columns": ["id"],
            },
        )


def test_write_batch_error_details_include_root_cause_and_sql_preview(
    writer, session, dialect
):
    dialect.generate_bulk_insert_query.return_value = (
        "INSERT INTO t (a) VALUES ('secret-token-value')"
    )
    session.cursor.return_value.executemany.side_effect = RuntimeError(
        "db write failed"
    )
    with pytest.raises(ConnectionError) as exc_info:
        writer.write_batch(
            [(1,)],
            {"target_schema": "s", "target_table": "t", "target_columns": ["id"]},
        )
    details = exc_info.value.details
    assert details["root_cause"] == "db write failed"
    assert details["sql_preview"] is not None
    assert "secret-token-value" not in details["sql_preview"]
    assert "'?'" in details["sql_preview"]


def test_write_batch_sql_preview_truncates_to_300_chars(writer, session, dialect):
    long_value = "x" * 500
    dialect.generate_bulk_insert_query.return_value = (
        f"INSERT INTO t (a) VALUES ({long_value})"
    )
    session.cursor.return_value.executemany.side_effect = RuntimeError(
        "db write failed"
    )
    with pytest.raises(ConnectionError) as exc_info:
        writer.write_batch(
            [(1,)],
            {"target_schema": "s", "target_table": "t", "target_columns": ["id"]},
        )
    preview = exc_info.value.details["sql_preview"]
    assert preview.endswith("...")
    assert len(preview) <= 303


def test_write_batch_adapts_postgres_dict_values_for_json(writer, session):
    class PostgresDialect:
        def quote_identifier(self, name):
            return f'"{name}"'

        def generate_bulk_insert_query(self, table, columns):
            return "INSERT INTO ..."

    class FakeJsonb:
        def __init__(self, value):
            self.value = value

    json_mod = types.ModuleType("psycopg.types.json")
    json_mod.Jsonb = FakeJsonb
    types_mod = types.ModuleType("psycopg.types")
    psycopg_mod = types.ModuleType("psycopg")
    with patch.dict(
        sys.modules,
        {
            "psycopg": psycopg_mod,
            "psycopg.types": types_mod,
            "psycopg.types.json": json_mod,
        },
    ):
        pg_writer = TargetWriter(session, PostgresDialect())
        pg_writer.write_batch(
            [({"k": "v"},)],
            {"target_schema": "s", "target_table": "t", "target_columns": ["payload"]},
        )

    call_rows = session.cursor.return_value.executemany.call_args[0][1]
    assert isinstance(call_rows[0][0], FakeJsonb)


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
