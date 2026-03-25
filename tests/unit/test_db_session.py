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

@patch("ffengine.db.airflow_adapter.BaseHook.get_connection")
def test_airflow_adapter_resolution(mock_get_connection):
    # Mocking Airflow's BaseHook
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
