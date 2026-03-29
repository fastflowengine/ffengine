"""
MSSQLDialect — pyodbc implementation of BaseDialect.
"""

from typing import Any

from ffengine.dialects.base import BaseDialect, ColumnInfo


class MSSQLDialect(BaseDialect):
    """Microsoft SQL Server dialect using the pyodbc driver."""

    # ------------------------------------------------------------------
    # Connection & Cursor
    # ------------------------------------------------------------------

    def connect(self, params: dict) -> Any:
        import pyodbc

        driver = params.get("driver", "{ODBC Driver 18 for SQL Server}")
        host = params.get("host", "localhost")
        port = params.get("port", 1433)
        user = params.get("user", "")
        password = params.get("password", "")
        database = params.get("database", "master")
        extra = params.get("extra", {})

        encrypt = extra.get("Encrypt", "yes")
        trust_cert = extra.get("TrustServerCertificate", "yes")

        conn_str = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_cert}"
        )
        return pyodbc.connect(conn_str, autocommit=False)

    def create_cursor(self, conn: Any, server_side: bool = False) -> Any:
        # pyodbc does not support named / server-side cursors
        return conn.cursor()

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------

    def get_table_schema(
        self, conn: Any, schema: str, table: str
    ) -> list[ColumnInfo]:
        query = """
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   NUMERIC_PRECISION,
                   NUMERIC_SCALE
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = ?
              AND  TABLE_NAME   = ?
            ORDER  BY ORDINAL_POSITION
        """
        cur = conn.cursor()
        cur.execute(query, (schema, table))
        columns = []
        for row in cur.fetchall():
            columns.append(
                ColumnInfo(
                    name=row[0],
                    data_type=row[1].upper(),
                    nullable=row[2] == "YES",
                    precision=row[3],
                    scale=row[4],
                )
            )
        cur.close()
        return columns

    def list_schemas(self, conn: Any) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys') "
            "ORDER BY SCHEMA_NAME"
        )
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        return schemas

    def list_tables(self, conn: Any, schema: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME",
            (schema,),
        )
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        return tables

    # ------------------------------------------------------------------
    # SQL Generation
    # ------------------------------------------------------------------

    def generate_ddl(self, table_name: str, columns: list[ColumnInfo]) -> str:
        col_defs = []
        for col in columns:
            type_str = self._column_type_sql(col)
            null_str = "" if col.nullable else " NOT NULL"
            col_defs.append(
                f"    {self.quote_identifier(col.name)} {type_str}{null_str}"
            )
        cols_sql = ",\n".join(col_defs)
        return (
            f"IF OBJECT_ID(N'{table_name}', N'U') IS NULL\n"
            f"CREATE TABLE {table_name} (\n{cols_sql}\n);"
        )

    def generate_bulk_insert_query(
        self, table: str, columns: list[str]
    ) -> str:
        quoted = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        return f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"

    def get_pagination_query(
        self, query: str, limit: int, offset: int
    ) -> str:
        return (
            f"{query} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        )

    # ------------------------------------------------------------------
    # Quoting & Type Map
    # ------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace("]", "]]")
        return f"[{escaped}]"

    def get_data_type_map(self) -> dict[str, str]:
        return {
            "INT": "INT",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "DECIMAL": "DECIMAL",
            "NUMERIC": "NUMERIC",
            "FLOAT": "FLOAT",
            "REAL": "REAL",
            "BIT": "BIT",
            "NVARCHAR": "NVARCHAR",
            "VARCHAR": "VARCHAR",
            "NCHAR": "NCHAR",
            "CHAR": "CHAR",
            "NTEXT": "NTEXT",
            "TEXT": "TEXT",
            "VARBINARY": "VARBINARY",
            "DATE": "DATE",
            "DATETIME2": "DATETIME2",
            "DATETIMEOFFSET": "DATETIMEOFFSET",
            "TIME": "TIME",
            "UNIQUEIDENTIFIER": "UNIQUEIDENTIFIER",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _column_type_sql(col: ColumnInfo) -> str:
        base = (col.data_type or "").strip().upper()
        if not base:
            return "NVARCHAR(4000)"

        # Respect explicit type parameters from mapper/config (e.g. DECIMAL(18,4), NVARCHAR(MAX)).
        if "(" in base and base.endswith(")"):
            return base

        precision = col.precision
        scale = col.scale

        # Precision/scale only applies to true numeric precision types.
        if base in {"DECIMAL", "NUMERIC"} and precision is not None:
            if scale is not None:
                return f"{base}({precision},{scale})"
            return f"{base}({precision})"

        # Length-bearing text/binary types need explicit defaults when length is unknown.
        if base in {"NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "VARBINARY", "BINARY"}:
            if precision is not None:
                return f"{base}({precision})"
            if base == "NVARCHAR":
                return "NVARCHAR(4000)"
            if base == "VARCHAR":
                return "VARCHAR(8000)"
            if base == "NCHAR":
                return "NCHAR(1)"
            if base == "CHAR":
                return "CHAR(1)"
            return "VARBINARY(MAX)"

        # Integer/date/time/uuid families are intentionally parameterless in MSSQL DDL.
        return base
