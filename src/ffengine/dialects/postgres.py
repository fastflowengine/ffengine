"""
PostgresDialect — psycopg (v3) implementation of BaseDialect.
"""

from typing import Any

from ffengine.dialects.base import BaseDialect, ColumnInfo


class PostgresDialect(BaseDialect):
    """PostgreSQL dialect using the psycopg3 driver."""

    # ------------------------------------------------------------------
    # Connection & Cursor
    # ------------------------------------------------------------------

    def connect(self, params: dict) -> Any:
        import psycopg

        conn_params = {
            k: v
            for k, v in params.items()
            if k in ("host", "port", "user", "password", "dbname", "database")
        }
        # psycopg3 uses 'dbname', but callers may pass 'database'
        if "database" in conn_params and "dbname" not in conn_params:
            conn_params["dbname"] = conn_params.pop("database")
        elif "database" in conn_params:
            del conn_params["database"]

        return psycopg.connect(**conn_params, autocommit=False)

    def create_cursor(self, conn: Any, server_side: bool = False) -> Any:
        if server_side:
            return conn.cursor(name="ff_sse_cursor")
        return conn.cursor()

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------

    def get_table_schema(
        self, conn: Any, schema: str, table: str
    ) -> list[ColumnInfo]:
        query = """
            SELECT column_name,
                   data_type,
                   is_nullable,
                   numeric_precision,
                   numeric_scale
            FROM   information_schema.columns
            WHERE  table_schema = %s
              AND  table_name   = %s
            ORDER  BY ordinal_position
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
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY schema_name"
        )
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        return schemas

    def list_tables(self, conn: Any, schema: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
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
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols_sql}\n);"

    def generate_bulk_insert_query(
        self, table: str, columns: list[str]
    ) -> str:
        quoted = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"

    def get_pagination_query(
        self, query: str, limit: int, offset: int
    ) -> str:
        return f"{query} LIMIT {limit} OFFSET {offset}"

    # ------------------------------------------------------------------
    # Quoting & Type Map
    # ------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def get_data_type_map(self) -> dict[str, str]:
        return {
            "INTEGER": "INTEGER",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "NUMERIC": "NUMERIC",
            "REAL": "REAL",
            "DOUBLE PRECISION": "DOUBLE PRECISION",
            "BOOLEAN": "BOOLEAN",
            "TEXT": "TEXT",
            "VARCHAR": "VARCHAR",
            "CHAR": "CHAR",
            "BYTEA": "BYTEA",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
            "TIME": "TIME",
            "INTERVAL": "INTERVAL",
            "JSONB": "JSONB",
            "UUID": "UUID",
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _column_type_sql(col: ColumnInfo) -> str:
        """Build the SQL type expression including precision/scale."""
        base = (col.data_type or "").strip().upper()
        if not base:
            return "TEXT"

        # Respect explicit type parameters from mapper/config.
        if "(" in base and base.endswith(")"):
            return base

        if base in {"NUMERIC", "DECIMAL"} and col.precision is not None:
            if col.scale is not None:
                return f"{base}({col.precision},{col.scale})"
            return f"{base}({col.precision})"

        if base in {"VARCHAR", "CHAR", "CHARACTER VARYING", "CHARACTER"} and col.precision is not None:
            return f"{base}({col.precision})"

        return base
