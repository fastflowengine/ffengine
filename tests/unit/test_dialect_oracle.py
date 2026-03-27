import pytest
from unittest.mock import MagicMock, patch
from ffengine.dialects.oracle import OracleDialect
from ffengine.dialects.base import ColumnInfo


@pytest.fixture
def dialect():
    return OracleDialect()


# ------------------------------------------------------------------
# Connection & Cursor
# ------------------------------------------------------------------


@patch("oracledb.init_oracle_client")
@patch("oracledb.connect")
def test_connect(mock_connect, mock_init, dialect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    params = {
        "host": "oracle-db",
        "port": 1521,
        "user": "system",
        "password": "oracle_pwd",
        "database": "FREEPDB1",
    }
    result = dialect.connect(params)

    mock_connect.assert_called_once_with(
        user="system", password="oracle_pwd", dsn="oracle-db:1521/FREEPDB1"
    )
    mock_init.assert_not_called()
    assert result is mock_conn


@patch("oracledb.init_oracle_client")
@patch("oracledb.connect")
def test_connect_thick_mode(mock_connect, mock_init, dialect):
    mock_connect.return_value = MagicMock()

    params = {
        "host": "oracle-db",
        "user": "sys",
        "password": "pwd",
        "extra": {"thick_mode": True},
    }
    dialect.connect(params)

    mock_init.assert_called_once()


def test_create_cursor(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    result = dialect.create_cursor(mock_conn)
    assert result is mock_cursor


def test_create_cursor_server_side_ignored(dialect):
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
        ("ID", "NUMBER", "N", 10, 0),
        ("AMOUNT", "NUMBER", "Y", 38, 10),
        ("DESCRIPTION", "CLOB", "Y", None, None),
    ]

    columns = dialect.get_table_schema(mock_conn, "HR", "EMPLOYEES")

    assert len(columns) == 3
    assert columns[0] == ColumnInfo("ID", "NUMBER", False, 10, 0)
    assert columns[1] == ColumnInfo("AMOUNT", "NUMBER", True, 38, 10)
    assert columns[2] == ColumnInfo("DESCRIPTION", "CLOB", True, None, None)

    # Oracle should upper-case schema and table in query
    call_args = mock_cursor.execute.call_args[0]
    assert call_args[1] == ("HR", "EMPLOYEES")
    mock_cursor.close.assert_called_once()


def test_list_schemas(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("HR",), ("SYS",)]

    schemas = dialect.list_schemas(mock_conn)
    assert schemas == ["HR", "SYS"]


def test_list_tables(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("EMPLOYEES",), ("DEPARTMENTS",)]

    tables = dialect.list_tables(mock_conn, "HR")
    assert tables == ["EMPLOYEES", "DEPARTMENTS"]
    # Must upper-case schema param
    assert mock_cursor.execute.call_args[0][1] == ("HR",)


# ------------------------------------------------------------------
# SQL Generation
# ------------------------------------------------------------------


def test_generate_ddl(dialect):
    columns = [
        ColumnInfo("ID", "NUMBER", False, 10, 0),
        ColumnInfo("SALARY", "NUMBER", True, 18, 2),
    ]
    ddl = dialect.generate_ddl("HR.EMPLOYEES", columns)

    assert "BEGIN" in ddl
    assert "EXECUTE IMMEDIATE" in ddl
    assert "CREATE TABLE HR.EMPLOYEES" in ddl
    assert '"ID" NUMBER(10,0) NOT NULL' in ddl
    assert '"SALARY" NUMBER(18,2)' in ddl
    assert "SQLCODE != -955" in ddl  # IF NOT EXISTS equivalent


def test_generate_ddl_deterministic(dialect):
    columns = [
        ColumnInfo("A", "NUMBER", False),
        ColumnInfo("B", "CLOB", True),
    ]
    assert dialect.generate_ddl("T", columns) == dialect.generate_ddl("T", columns)


def test_generate_bulk_insert_query(dialect):
    query = dialect.generate_bulk_insert_query(
        "EMPLOYEES", ["ID", "NAME", "SALARY"]
    )
    assert query == (
        'INSERT INTO EMPLOYEES ("ID", "NAME", "SALARY") VALUES (:1, :2, :3)'
    )


def test_get_pagination_query(dialect):
    result = dialect.get_pagination_query("SELECT * FROM T", 50, 100)
    assert result == "SELECT * FROM T OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY"


# ------------------------------------------------------------------
# Quoting & Type Map
# ------------------------------------------------------------------


def test_quote_identifier(dialect):
    assert dialect.quote_identifier("my_col") == '"MY_COL"'


def test_quote_identifier_uppercase(dialect):
    """Oracle identifiers should be upper-cased."""
    assert dialect.quote_identifier("lower_name") == '"LOWER_NAME"'


def test_get_data_type_map(dialect):
    type_map = dialect.get_data_type_map()
    assert "NUMBER" in type_map
    assert "VARCHAR2" in type_map
    assert "CLOB" in type_map
    assert "BLOB" in type_map


# ------------------------------------------------------------------
# Health Check (Oracle DUAL)
# ------------------------------------------------------------------


def test_health_check_success(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)

    assert dialect.health_check(mock_conn) is True
    mock_cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")
    mock_cursor.close.assert_called_once()


def test_health_check_failure(dialect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    assert dialect.health_check(mock_conn) is False
