from __future__ import annotations

from unittest.mock import patch

import pytest

from ffengine.ui import studio_service as ss


class _FakeDialect:
    def list_schemas(self, _conn):
        return ["public", "analytics", "pub_admin"]

    def list_tables(self, _conn, schema):
        if schema == "public":
            return ["orders", "order_items", "customers"]
        if schema == "analytics":
            return ["daily_sales"]
        if schema == "pub_admin":
            return ["users"]
        return []


class _FakeDBSession:
    def __init__(self, *_args, **_kwargs):
        self.conn = object()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


@patch.object(ss.AirflowConnectionAdapter, "get_connection_params", return_value={"conn_type": "postgres"})
@patch.object(ss, "resolve_dialect", return_value=_FakeDialect())
@patch.object(ss, "DBSession", _FakeDBSession)
def test_discover_schemas_search_case_insensitive(_mock_dialect, _mock_conn):
    items = ss.discover_schemas("src", search="PUB", limit=50)
    assert "public" in items
    assert "pub_admin" in items


@patch.object(ss.AirflowConnectionAdapter, "get_connection_params", return_value={"conn_type": "postgres"})
@patch.object(ss, "resolve_dialect", return_value=_FakeDialect())
@patch.object(ss, "DBSession", _FakeDBSession)
def test_discover_tables_schema_case_insensitive_exact(_mock_dialect, _mock_conn):
    data = ss.discover_tables("src", schema="PUBLIC", search="ord", limit=50, offset=0)
    assert data["schema"] == "public"
    assert "orders" in data["items"]


@patch.object(ss.AirflowConnectionAdapter, "get_connection_params", return_value={"conn_type": "postgres"})
@patch.object(ss, "resolve_dialect", return_value=_FakeDialect())
@patch.object(ss, "DBSession", _FakeDBSession)
def test_discover_tables_schema_single_partial_match(_mock_dialect, _mock_conn):
    data = ss.discover_tables("src", schema="analy", search=None, limit=50, offset=0)
    assert data["schema"] == "analytics"
    assert data["items"] == ["daily_sales"]


@patch.object(ss.AirflowConnectionAdapter, "get_connection_params", return_value={"conn_type": "postgres"})
@patch.object(ss, "resolve_dialect", return_value=_FakeDialect())
@patch.object(ss, "DBSession", _FakeDBSession)
def test_discover_tables_schema_partial_ambiguous_raises(_mock_dialect, _mock_conn):
    with pytest.raises(ValueError, match="birden fazla eslesme"):
        ss.discover_tables("src", schema="pub", search=None, limit=50, offset=0)
