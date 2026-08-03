"""
F2.1 — BaseDialect additive contract (INV-2). Maps to T-F2.1-1: the bulk seam is
optional (non-abstract), so existing dialects are not broken.
"""

from ffengine.dialects.base import BaseDialect
from ffengine.dialects.mssql import MSSQLDialect
from ffengine.dialects.oracle import OracleDialect
from ffengine.dialects.postgres import PostgresDialect


def test_configure_write_cursor_is_not_abstract():
    assert "configure_write_cursor" not in BaseDialect.__abstractmethods__


def test_insert_query_rename_did_not_add_abstract_surface():
    # generate_insert_query replaced generate_bulk_insert_query (rename, not add);
    # the old name survives as a *concrete* deprecating alias.
    assert "generate_insert_query" in BaseDialect.__abstractmethods__
    assert "generate_bulk_insert_query" not in BaseDialect.__abstractmethods__


def test_existing_dialects_still_instantiate():
    for cls in (PostgresDialect, MSSQLDialect, OracleDialect):
        assert cls() is not None


def test_base_configure_write_cursor_default_is_noop():
    # PostgresDialect does not override it -> inherits the base no-op; a write
    # cursor is left untouched (only MSSQL opts into fast_executemany).
    class _Cursor:
        pass

    cur = _Cursor()
    assert PostgresDialect().configure_write_cursor(cur) is None
    assert not hasattr(cur, "fast_executemany")
