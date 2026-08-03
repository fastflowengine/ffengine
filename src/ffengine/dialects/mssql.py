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

    def configure_write_cursor(self, cursor: Any) -> None:
        # pyodbc: collapse the executemany round-trips into one batched TDS
        # call. Applied only to write cursors — create_cursor() is shared with
        # read/validation paths where fast_executemany must not be set.
        cursor.fast_executemany = True

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------

    def get_table_schema(self, conn: Any, schema: str, table: str) -> list[ColumnInfo]:
        query = """
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   NUMERIC_PRECISION,
                   NUMERIC_SCALE,
                   CHARACTER_MAXIMUM_LENGTH
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = ?
              AND  TABLE_NAME   = ?
            ORDER  BY ORDINAL_POSITION
        """
        cur = conn.cursor()
        cur.execute(query, (schema, table))
        columns = []
        for row in cur.fetchall():
            data_type = row[1].upper()
            numeric_precision = row[3]
            numeric_scale = row[4]
            char_length = row[5]
            precision = None
            scale = None

            if data_type in {
                "DECIMAL",
                "NUMERIC",
                "MONEY",
                "SMALLMONEY",
                "INT",
                "BIGINT",
                "SMALLINT",
                "TINYINT",
                "FLOAT",
                "REAL",
                "BIT",
            }:
                precision = numeric_precision
                scale = numeric_scale
            elif data_type in {"NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "VARBINARY", "BINARY"}:
                precision = self._normalize_length(char_length)

            columns.append(
                ColumnInfo(
                    name=row[0],
                    data_type=data_type,
                    nullable=row[2] == "YES",
                    precision=precision,
                    scale=scale,
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

    def generate_insert_query(self, table: str, columns: list[str]) -> str:
        quoted = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        return f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"

    def generate_upsert_query(
        self,
        table: str,
        columns: list[str],
        match_columns: list[str],
        update_columns: list[str],
    ) -> str:
        quoted_columns = [self.quote_identifier(c) for c in columns]
        quoted_match_columns = [self.quote_identifier(c) for c in match_columns]
        quoted_update_columns = [self.quote_identifier(c) for c in update_columns]
        source_select = ", ".join(
            f"? AS {quoted_col}" for quoted_col in quoted_columns
        )
        on_clause = " AND ".join(
            f"target.{quoted_col} = source.{quoted_col}"
            for quoted_col in quoted_match_columns
        )
        insert_columns = ", ".join(quoted_columns)
        insert_values = ", ".join(f"source.{quoted_col}" for quoted_col in quoted_columns)
        clauses: list[str] = []
        if quoted_update_columns:
            updates = ", ".join(
                f"target.{quoted_col} = source.{quoted_col}"
                for quoted_col in quoted_update_columns
            )
            clauses.append(f"WHEN MATCHED THEN UPDATE SET {updates}")
        clauses.append(
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )
        return (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_select}) AS source "
            f"ON {on_clause} "
            f"{' '.join(clauses)};"
        )

    # ------------------------------------------------------------------
    # F1.2 - Push-down enrichment
    # ------------------------------------------------------------------

    def _bind_placeholder(self, index: int) -> str:
        return "?"

    def _expr_ops(self, mode: str, *, alloc=None, quote=None):
        from ffengine.mapping import expression as _expr

        return _expr.MSSQLExprOps(mode=mode, alloc=alloc, quote=quote)

    def generate_enriched_upsert_query(
        self,
        table: str,
        target_columns: list[str],
        value_exprs: dict,
        plain_source_by_target: dict[str, str],
        match_columns: list[str],
        update_columns: list[str],
    ) -> tuple[str, list[str]]:
        """F1.2 - MERGE upsert with target-side derived expressions."""
        value_sql, bind_plan = self._compile_row_values(
            target_columns, value_exprs, plain_source_by_target
        )
        q = self.quote_identifier
        source_select = ", ".join(
            f"{v} AS {q(c)}" for v, c in zip(value_sql, target_columns)
        )
        on_clause = " AND ".join(
            f"target.{q(c)} = source.{q(c)}" for c in match_columns
        )
        clauses: list[str] = []
        if update_columns:
            updates = ", ".join(
                f"target.{q(c)} = source.{q(c)}" for c in update_columns
            )
            clauses.append(f"WHEN MATCHED THEN UPDATE SET {updates}")
        insert_cols = ", ".join(q(c) for c in target_columns)
        insert_vals = ", ".join(f"source.{q(c)}" for c in target_columns)
        clauses.append(
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        sql = (
            f"MERGE INTO {table} AS target "
            f"USING (SELECT {source_select}) AS source "
            f"ON {on_clause} "
            f"{' '.join(clauses)};"
        )
        return sql, bind_plan

    def get_pagination_query(self, query: str, limit: int, offset: int) -> str:
        return f"{query} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

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
        if base in {"DECIMAL", "NUMERIC"} and precision is not None and precision > 0:
            if scale is not None:
                return f"{base}({precision},{scale})"
            return f"{base}({precision})"

        # Length-bearing text/binary types need explicit defaults when length is unknown.
        if base in {"NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "VARBINARY", "BINARY"}:
            if precision is not None and precision > 0:
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

    @staticmethod
    def _normalize_length(length: Any) -> int | None:
        """
        Normalize MSSQL CHARACTER_MAXIMUM_LENGTH.

        - `-1` (MAX) and non-positive values are treated as unbounded.
        - Unbounded values are returned as None so downstream DDL falls back to
          bounded defaults per policy.
        """
        try:
            value = int(length)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        return value
