import pytest
from unittest.mock import MagicMock, call, patch
from ffengine.pipeline.source_reader import SourceReader


@pytest.fixture
def dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    return d


@pytest.fixture
def session():
    s = MagicMock()
    return s


# ------------------------------------------------------------------
# _build_query
# ------------------------------------------------------------------


def test_build_query_select_star(dialect, session):
    config = {"source_schema": "hr", "source_table": "employees"}
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert q == 'SELECT * FROM "hr"."employees"'


def test_build_query_with_columns(dialect, session):
    config = {
        "source_schema": "hr",
        "source_table": "employees",
        "source_columns": ["id", "name"],
    }
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert '"id"' in q
    assert '"name"' in q
    assert "SELECT" in q


def test_build_query_with_where_clause(dialect, session):
    config = {
        "source_schema": "hr",
        "source_table": "employees",
        "where_clause": "dept_id = 10",
    }
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert "WHERE dept_id = 10" in q


def test_build_query_with_resolved_where(dialect, session):
    config = {
        "source_schema": "dbo",
        "source_table": "orders",
        "_resolved_where": "order_date > '2024-01-01'",
    }
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert "WHERE order_date > '2024-01-01'" in q


def test_build_query_no_schema(dialect, session):
    config = {"source_table": "orders"}
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert q == 'SELECT * FROM "orders"'


def test_build_query_sql_inline_sql(dialect, session):
    config = {
        "source_type": "sql",
        "inline_sql": "SELECT id FROM orders",
    }
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert q == "SELECT id FROM orders"


def test_build_query_sql_inline_sql_with_where(dialect, session):
    config = {
        "source_type": "sql",
        "inline_sql": "SELECT id FROM orders",
        "where_clause": "id > 100",
    }
    reader = SourceReader(session, config, dialect)
    q = reader._build_query()
    assert q == "SELECT * FROM (SELECT id FROM orders) AS ffengine_inline_sql WHERE id > 100"


# ------------------------------------------------------------------
# batch_size default
# ------------------------------------------------------------------


def test_default_batch_size(dialect, session):
    reader = SourceReader(session, {}, dialect)
    assert reader.batch_size == 10_000


def test_custom_batch_size(dialect, session):
    reader = SourceReader(session, {"batch_size": 500}, dialect)
    assert reader.batch_size == 500


# ------------------------------------------------------------------
# read() — chunk iteration
# ------------------------------------------------------------------


def test_read_yields_chunks(dialect, session):
    mock_cursor = MagicMock()
    session.cursor.return_value = mock_cursor

    batch1 = [(1, "a"), (2, "b")]
    batch2 = [(3, "c")]
    mock_cursor.fetchmany.side_effect = [batch1, batch2, []]

    config = {"source_schema": "s", "source_table": "t"}
    reader = SourceReader(session, config, dialect)

    chunks = list(reader.read())

    assert chunks == [batch1, batch2]
    session.cursor.assert_called_once_with(server_side=True)


def test_read_empty_table(dialect, session):
    mock_cursor = MagicMock()
    session.cursor.return_value = mock_cursor
    mock_cursor.fetchmany.return_value = []

    config = {"source_schema": "s", "source_table": "t"}
    reader = SourceReader(session, config, dialect)

    chunks = list(reader.read())
    assert chunks == []


def test_read_cursor_closed_on_completion(dialect, session):
    mock_cursor = MagicMock()
    session.cursor.return_value = mock_cursor
    mock_cursor.fetchmany.return_value = []

    config = {"source_schema": "s", "source_table": "t"}
    reader = SourceReader(session, config, dialect)
    list(reader.read())

    mock_cursor.close.assert_called_once()


def test_read_cursor_closed_on_error(dialect, session):
    mock_cursor = MagicMock()
    session.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = RuntimeError("DB error")

    config = {"source_schema": "s", "source_table": "t"}
    reader = SourceReader(session, config, dialect)

    with pytest.raises(RuntimeError, match="DB error"):
        list(reader.read())

    mock_cursor.close.assert_called_once()


def test_read_uses_batch_size(dialect, session):
    mock_cursor = MagicMock()
    session.cursor.return_value = mock_cursor
    mock_cursor.fetchmany.side_effect = [[(1,)], []]

    config = {"source_schema": "s", "source_table": "t", "batch_size": 250}
    reader = SourceReader(session, config, dialect)
    list(reader.read())

    mock_cursor.fetchmany.assert_called_with(250)
