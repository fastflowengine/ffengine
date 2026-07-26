import pytest
from unittest.mock import MagicMock, patch
from ffengine.dialects.postgres import PostgresDialect
from ffengine.dialects.base import ColumnInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


# ------------------------------------------------------------------
# Connection & Cursor
# ------------------------------------------------------------------


@patch("psycopg.connect")
def test_connect(mock_connect, dialect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    params = {
        "host": "localhost",
        "port": 5432,
        "user": "admin",
        "password": "secret",
        "database": "testdb",
    }
    result = dialect.connect(params)

    mock_connect.assert_called_once_with(
        host="localhost",
        port=5432,
        user="admin",
        password="secret",
        dbname="testdb",
        autocommit=False,
    )
    assert result is mock_conn


@patch("psycopg.connect")
def test_connect_registers_infinity_datetime_loaders(mock_connect, dialect):
    from ffengine.dialects.postgres import (
        _PG_OID_DATE,
        _PG_OID_TIMESTAMP,
        _PG_OID_TIMESTAMPTZ,
    )

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    dialect.connect({"host": "h", "user": "u", "password": "p", "dbname": "d"})

    registered_oids = {
        call.args[0] for call in mock_conn.adapters.register_loader.call_args_list
    }
    assert registered_oids == {_PG_OID_DATE, _PG_OID_TIMESTAMP, _PG_OID_TIMESTAMPTZ}


def test_infinity_datetime_loaders_preserve_sentinels():
    # ±infinity sentinels are returned as raw text (exact copy); finite values
    # are left to psycopg's default loader (not exercised here).
    from ffengine.dialects.postgres import (
        _PG_OID_DATE,
        _PG_OID_TIMESTAMP,
        _PG_OID_TIMESTAMPTZ,
        _infinity_datetime_loaders,
    )

    loaders = dict(_infinity_datetime_loaders())
    assert set(loaders) == {_PG_OID_DATE, _PG_OID_TIMESTAMP, _PG_OID_TIMESTAMPTZ}
    for oid, loader_cls in loaders.items():
        loader = loader_cls(oid, None)
        assert loader.load(b"-infinity") == "-infinity"
        assert loader.load(b"infinity") == "infinity"
        assert loader.load(memoryview(b"-infinity")) == "-infinity"


def test_create_cursor_standard(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = dialect.create_cursor(mock_conn, server_side=False)
    mock_conn.cursor.assert_called_once_with()
    assert result is mock_cursor


def test_create_cursor_server_side(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = dialect.create_cursor(mock_conn, server_side=True)
    mock_conn.cursor.assert_called_once_with(name="ff_sse_cursor")
    assert result is mock_cursor


# ------------------------------------------------------------------
# Schema Discovery
# ------------------------------------------------------------------


def test_get_table_schema(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        ("id", "integer", "NO", None, None, None),
        ("amount", "numeric", "YES", 38, 10, None),
        ("name", "character varying", "YES", None, None, 120),
    ]

    columns = dialect.get_table_schema(mock_conn, "public", "orders")

    assert len(columns) == 3
    assert columns[0] == ColumnInfo("id", "INTEGER", False, None, None)
    assert columns[1] == ColumnInfo("amount", "NUMERIC", True, 38, 10)
    assert columns[2] == ColumnInfo("name", "CHARACTER VARYING", True, 120, None)
    mock_cursor.close.assert_called_once()


def test_get_table_schema_falls_back_to_pg_catalog_for_materialized_view(dialect):
    mock_conn = MagicMock()
    info_cursor = MagicMock()
    catalog_cursor = MagicMock()
    mock_conn.cursor.side_effect = [info_cursor, catalog_cursor]
    info_cursor.fetchall.return_value = []
    catalog_cursor.fetchall.return_value = [
        ("guid", "numeric(16,0)", True),
        ("sce_id", "character varying(8)", True),
        ("description", "text", False),
    ]

    columns = dialect.get_table_schema(mock_conn, "ocn_cat", "cat_scenario_def")

    assert columns == [
        ColumnInfo("guid", "NUMERIC", False, 16, 0),
        ColumnInfo("sce_id", "CHARACTER VARYING", False, 8, None),
        ColumnInfo("description", "TEXT", True, None, None),
    ]
    info_cursor.close.assert_called_once()
    catalog_cursor.close.assert_called_once()


def test_list_schemas(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("public",), ("sales",)]

    schemas = dialect.list_schemas(mock_conn)

    assert schemas == ["public", "sales"]
    mock_cursor.close.assert_called_once()


def test_list_tables(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("orders",), ("products",)]

    tables = dialect.list_tables(mock_conn, "public")

    assert tables == ["orders", "products"]
    mock_cursor.close.assert_called_once()


# ------------------------------------------------------------------
# SQL Generation
# ------------------------------------------------------------------


def test_generate_ddl(dialect):
    columns = [
        ColumnInfo("id", "INTEGER", False),
        ColumnInfo("amount", "NUMERIC", True, 18, 2),
        ColumnInfo("name", "VARCHAR", True, 100),
    ]
    ddl = dialect.generate_ddl("public.orders", columns)

    assert "CREATE TABLE IF NOT EXISTS public.orders" in ddl
    assert '"id" INTEGER NOT NULL' in ddl
    assert '"amount" NUMERIC(18,2)' in ddl
    assert '"name" VARCHAR(100)' in ddl


def test_generate_ddl_deterministic(dialect):
    """Same input must always produce the exact same DDL output."""
    columns = [
        ColumnInfo("a", "INTEGER", False),
        ColumnInfo("b", "TEXT", True),
    ]
    ddl1 = dialect.generate_ddl("t", columns)
    ddl2 = dialect.generate_ddl("t", columns)
    assert ddl1 == ddl2


def test_generate_bulk_insert_query(dialect):
    query = dialect.generate_bulk_insert_query("orders", ["id", "amount", "name"])
    assert query == ('INSERT INTO orders ("id", "amount", "name") VALUES (%s, %s, %s)')


def test_generate_upsert_query_with_update_columns(dialect):
    query = dialect.generate_upsert_query(
        "orders",
        ["id", "name", "amount"],
        ["id"],
        ["name", "amount"],
    )
    assert 'ON CONFLICT ("id") DO UPDATE SET' in query
    assert '"name" = EXCLUDED."name"' in query
    assert '"amount" = EXCLUDED."amount"' in query


def test_generate_upsert_query_without_update_columns(dialect):
    query = dialect.generate_upsert_query(
        "orders",
        ["id", "name"],
        ["id"],
        [],
    )
    assert 'ON CONFLICT ("id") DO NOTHING' in query


def test_get_pagination_query(dialect):
    result = dialect.get_pagination_query("SELECT * FROM t", 100, 200)
    assert result == "SELECT * FROM t LIMIT 100 OFFSET 200"


# ------------------------------------------------------------------
# Quoting & Type Map
# ------------------------------------------------------------------


def test_quote_identifier(dialect):
    assert dialect.quote_identifier("my_col") == '"my_col"'


def test_quote_identifier_with_quotes(dialect):
    assert dialect.quote_identifier('has"quote') == '"has""quote"'


def test_get_data_type_map(dialect):
    type_map = dialect.get_data_type_map()
    assert "INTEGER" in type_map
    assert "NUMERIC" in type_map
    assert "TEXT" in type_map
    assert "TIMESTAMP" in type_map


# ------------------------------------------------------------------
# Health Check
# ------------------------------------------------------------------


def test_health_check_success(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)

    assert dialect.health_check(mock_conn) is True
    mock_cursor.close.assert_called_once()


def test_health_check_failure(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    assert dialect.health_check(mock_conn) is False
