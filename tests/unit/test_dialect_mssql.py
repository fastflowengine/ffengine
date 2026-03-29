import pytest
from unittest.mock import MagicMock, patch
from ffengine.dialects.mssql import MSSQLDialect
from ffengine.dialects.base import ColumnInfo


@pytest.fixture
def dialect():
    return MSSQLDialect()


# ------------------------------------------------------------------
# Connection & Cursor
# ------------------------------------------------------------------


@patch("pyodbc.connect")
def test_connect(mock_connect, dialect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    params = {
        "host": "mssql-server",
        "port": 1433,
        "user": "sa",
        "password": "P@ssword",
        "database": "master",
        "extra": {"Encrypt": "no", "TrustServerCertificate": "yes"},
    }
    result = dialect.connect(params)

    call_args = mock_connect.call_args
    conn_str = call_args[0][0]
    assert "SERVER=mssql-server,1433" in conn_str
    assert "DATABASE=master" in conn_str
    assert "UID=sa" in conn_str
    assert "Encrypt=no" in conn_str
    assert result is mock_conn


@patch("pyodbc.connect")
def test_connect_default_extra(mock_connect, dialect):
    """Without extra params, defaults Encrypt=yes, TrustServerCertificate=yes."""
    mock_connect.return_value = MagicMock()

    params = {"host": "localhost", "user": "sa", "password": "pwd"}
    dialect.connect(params)

    conn_str = mock_connect.call_args[0][0]
    assert "Encrypt=yes" in conn_str
    assert "TrustServerCertificate=yes" in conn_str


def test_create_cursor(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = dialect.create_cursor(mock_conn)
    assert result is mock_cursor


def test_create_cursor_server_side_ignored(dialect):
    """MSSQL does not support server-side; should return standard cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = dialect.create_cursor(mock_conn, server_side=True)
    mock_conn.cursor.assert_called_once_with()
    assert result is mock_cursor


# ------------------------------------------------------------------
# Schema Discovery
# ------------------------------------------------------------------


def test_get_table_schema(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        ("OrderId", "int", "NO", 10, 0),
        ("Total", "decimal", "YES", 18, 2),
    ]

    columns = dialect.get_table_schema(mock_conn, "dbo", "Orders")

    assert len(columns) == 2
    assert columns[0] == ColumnInfo("OrderId", "INT", False, 10, 0)
    assert columns[1] == ColumnInfo("Total", "DECIMAL", True, 18, 2)
    mock_cursor.close.assert_called_once()


def test_list_schemas(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("dbo",), ("sales",)]

    schemas = dialect.list_schemas(mock_conn)
    assert schemas == ["dbo", "sales"]


def test_list_tables(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("Orders",), ("Products",)]

    tables = dialect.list_tables(mock_conn, "dbo")
    assert tables == ["Orders", "Products"]


# ------------------------------------------------------------------
# SQL Generation
# ------------------------------------------------------------------


def test_generate_ddl(dialect):
    columns = [
        ColumnInfo("id", "INT", False, 10, 0),
        ColumnInfo("price", "DECIMAL", True, 18, 4),
    ]
    ddl = dialect.generate_ddl("dbo.Orders", columns)

    assert "IF OBJECT_ID" in ddl
    assert "CREATE TABLE dbo.Orders" in ddl
    assert "[id] INT NOT NULL" in ddl
    assert "[price] DECIMAL(18,4)" in ddl


def test_generate_ddl_nvarchar_fallback_length(dialect):
    ddl = dialect.generate_ddl(
        "dbo.T",
        [ColumnInfo("name", "NVARCHAR", True)],
    )
    assert "[name] NVARCHAR(4000)" in ddl


def test_generate_ddl_respects_explicit_params(dialect):
    ddl = dialect.generate_ddl(
        "dbo.T",
        [ColumnInfo("name", "NVARCHAR(MAX)", True, 200, None)],
    )
    assert "[name] NVARCHAR(MAX)" in ddl


def test_generate_ddl_deterministic(dialect):
    columns = [
        ColumnInfo("a", "INT", False),
        ColumnInfo("b", "NVARCHAR", True),
    ]
    assert dialect.generate_ddl("t", columns) == dialect.generate_ddl("t", columns)


def test_generate_bulk_insert_query(dialect):
    query = dialect.generate_bulk_insert_query("Orders", ["id", "name"])
    assert query == "INSERT INTO Orders ([id], [name]) VALUES (?, ?)"


def test_get_pagination_query(dialect):
    result = dialect.get_pagination_query("SELECT * FROM t", 50, 100)
    assert result == "SELECT * FROM t OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY"


# ------------------------------------------------------------------
# Quoting & Type Map
# ------------------------------------------------------------------


def test_quote_identifier(dialect):
    assert dialect.quote_identifier("my_col") == "[my_col]"


def test_quote_identifier_with_brackets(dialect):
    assert dialect.quote_identifier("has]bracket") == "[has]]bracket]"


def test_get_data_type_map(dialect):
    type_map = dialect.get_data_type_map()
    assert "INT" in type_map
    assert "DECIMAL" in type_map
    assert "NVARCHAR" in type_map
    assert "DATETIME2" in type_map


# ------------------------------------------------------------------
# Health Check
# ------------------------------------------------------------------


def test_health_check_success(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)

    assert dialect.health_check(mock_conn) is True


def test_health_check_failure(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    assert dialect.health_check(mock_conn) is False
