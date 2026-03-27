import pytest
from unittest.mock import MagicMock, patch
from ffengine.db.session import DBSession
from ffengine.db.airflow_adapter import AirflowConnectionAdapter

def test_db_session_commit():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn

    # Successful block should trigger commit
    with DBSession({"host": "localhost"}, mock_dialect) as session:
        assert session.conn is mock_conn
        
    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()
    mock_conn.close.assert_called_once()

def test_db_session_rollback():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn

    # Exception block should trigger rollback
    try:
        with DBSession({"host": "localhost"}, mock_dialect) as session:
            raise ValueError("Test error")
    except ValueError:
        pass
        
    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()
    mock_conn.close.assert_called_once()

def test_db_session_cursor():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_dialect.connect.return_value = mock_conn
    mock_dialect.create_cursor.return_value = mock_cursor

    with DBSession({"host": "localhost"}, mock_dialect) as session:
        cursor = session.cursor(server_side=True)
        assert cursor is mock_cursor
        mock_dialect.create_cursor.assert_called_once_with(mock_conn, True)

def test_db_session_cursor_no_conn():
    mock_dialect = MagicMock()
    # Explicitly creating session without 'with' to keep conn empty
    session = DBSession({"host": "localhost"}, mock_dialect)
    with pytest.raises(RuntimeError, match="Database connection is not open."):
        session.cursor()

def test_db_session_connect_exception():
    mock_dialect = MagicMock()
    mock_dialect.connect.side_effect = ConnectionError("DB Down")
    
    with pytest.raises(ConnectionError, match="DB Down"):
        with DBSession({"host": "localhost"}, mock_dialect):
            pass

def test_db_session_commit_exception():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn
    mock_conn.commit.side_effect = Exception("Commit Failed")
    
    with pytest.raises(Exception, match="Commit Failed"):
        with DBSession({"host": "localhost"}, mock_dialect):
            pass
    # The essential guarantee: close MUST be called even if commit fails
    mock_conn.close.assert_called_once()

def test_db_session_create_cursor_exception():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn
    mock_dialect.create_cursor.side_effect = RuntimeError("Cursor Fail")
    
    with DBSession({"host": "localhost"}, mock_dialect) as session:
        with pytest.raises(RuntimeError, match="Cursor Fail"):
            session.cursor()

@patch("airflow.hooks.base.BaseHook.get_connection")
def test_airflow_adapter_resolution_postgres(mock_get_connection):
    mock_conn = MagicMock()
    mock_conn.host = "postgres1"
    mock_conn.port = 5432
    mock_conn.login = "admin"
    mock_conn.password = "secret"
    mock_conn.schema = "dwh_db"
    mock_conn.conn_type = "postgres"
    mock_conn.extra_dejson = {"options": "-c search_path=public"}
    
    mock_get_connection.return_value = mock_conn
    
    params = AirflowConnectionAdapter.get_connection_params("my_pg_conn")
    
    assert params["host"] == "postgres1"
    assert params["port"] == 5432
    assert params["user"] == "admin"
    assert params["password"] == "secret"
    assert params["database"] == "dwh_db"
    assert params["conn_type"] == "postgres"
    assert params["extra"] == {"options": "-c search_path=public"}

@patch("airflow.hooks.base.BaseHook.get_connection")
def test_airflow_adapter_resolution_mssql(mock_get_connection):
    mock_conn = MagicMock()
    mock_conn.host = "mssql-server"
    mock_conn.port = 1433
    mock_conn.login = "sa"
    mock_conn.password = "P@ssword"
    mock_conn.schema = "master"
    mock_conn.conn_type = "mssql"
    mock_conn.extra_dejson = {"Encrypt": "yes"}
    
    mock_get_connection.return_value = mock_conn
    params = AirflowConnectionAdapter.get_connection_params("my_mssql_conn")
    
    assert params["host"] == "mssql-server"
    assert params["conn_type"] == "mssql"
    assert params["extra"] == {"Encrypt": "yes"}

@patch("airflow.hooks.base.BaseHook.get_connection")
def test_airflow_adapter_resolution_oracle(mock_get_connection):
    mock_conn = MagicMock()
    mock_conn.host = "oracle-db"
    mock_conn.port = 1521
    mock_conn.login = "system"
    mock_conn.password = "oracle_pwd"
    mock_conn.schema = "FREEPDB1"
    mock_conn.conn_type = "oracle"
    mock_conn.extra_dejson = {"thick_mode": True}
    
    mock_get_connection.return_value = mock_conn
    params = AirflowConnectionAdapter.get_connection_params("my_ora_conn")
    
    assert params["conn_type"] == "oracle"
    assert params["database"] == "FREEPDB1"
    assert params["extra"] == {"thick_mode": True}

def test_db_session_health_check_success():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn
    mock_dialect.health_check.return_value = True

    with DBSession({"host": "localhost"}, mock_dialect) as session:
        assert session.health_check() is True
        mock_dialect.health_check.assert_called_once_with(mock_conn)

def test_db_session_health_check_failure():
    mock_dialect = MagicMock()
    mock_conn = MagicMock()
    mock_dialect.connect.return_value = mock_conn
    mock_dialect.health_check.side_effect = Exception("DB Down")

    with DBSession({"host": "localhost"}, mock_dialect) as session:
        assert session.health_check() is False

def test_db_session_health_check_no_conn():
    mock_dialect = MagicMock()
    session = DBSession({"host": "localhost"}, mock_dialect)
    assert session.health_check() is False
