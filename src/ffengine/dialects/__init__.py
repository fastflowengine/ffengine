"""
FFEngine Dialect Layer
Provides database-agnostic dialect abstraction for PostgreSQL, MSSQL, and Oracle.
"""

from ffengine.dialects.base import BaseDialect, ColumnInfo
from ffengine.dialects.mssql import MSSQLDialect
from ffengine.dialects.oracle import OracleDialect
from ffengine.dialects.postgres import PostgresDialect
from ffengine.dialects.type_mapper import TypeMapper

__all__ = [
    "BaseDialect",
    "ColumnInfo",
    "MSSQLDialect",
    "OracleDialect",
    "PostgresDialect",
    "TypeMapper",
]
