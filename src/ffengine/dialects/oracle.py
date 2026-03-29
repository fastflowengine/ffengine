"""
OracleDialect — python-oracledb implementation of BaseDialect.
"""

from typing import Any

from ffengine.dialects.base import BaseDialect, ColumnInfo


class OracleDialect(BaseDialect):
    """Oracle Database dialect using the oracledb driver."""

    # ------------------------------------------------------------------
    # Connection & Cursor
    # ------------------------------------------------------------------

    def connect(self, params: dict) -> Any:
        import oracledb

        host = params.get("host", "localhost")
        port = params.get("port", 1521)
        database = params.get("database", "FREEPDB1")
        user = params.get("user", "")
        password = params.get("password", "")
        extra = params.get("extra", {})

        dsn = f"{host}:{port}/{database}"

        thick_mode = extra.get("thick_mode", False)
        if thick_mode:
            oracledb.init_oracle_client()

        return oracledb.connect(user=user, password=password, dsn=dsn)

    def create_cursor(self, conn: Any, server_side: bool = False) -> Any:
        # oracledb does not support named / server-side cursors
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
                   NULLABLE,
                   DATA_PRECISION,
                   DATA_SCALE
            FROM   ALL_TAB_COLUMNS
            WHERE  OWNER       = :1
              AND  TABLE_NAME  = :2
            ORDER  BY COLUMN_ID
        """
        cur = conn.cursor()
        cur.execute(query, (schema.upper(), table.upper()))
        columns = []
        for row in cur.fetchall():
            columns.append(
                ColumnInfo(
                    name=row[0],
                    data_type=row[1].upper(),
                    nullable=row[2] == "Y",
                    precision=row[3],
                    scale=row[4],
                )
            )
        cur.close()
        return columns

    def list_schemas(self, conn: Any) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME"
        )
        schemas = [row[0] for row in cur.fetchall()]
        cur.close()
        return schemas

    def list_tables(self, conn: Any, schema: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABLE_NAME FROM ALL_TABLES "
            "WHERE OWNER = :1 ORDER BY TABLE_NAME",
            (schema.upper(),),
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
            f"BEGIN\n"
            f"  EXECUTE IMMEDIATE 'CREATE TABLE {table_name} (\n{cols_sql}\n)';\n"
            f"EXCEPTION WHEN OTHERS THEN\n"
            f"  IF SQLCODE != -955 THEN RAISE; END IF;\n"
            f"END;"
        )

    def generate_bulk_insert_query(
        self, table: str, columns: list[str]
    ) -> str:
        quoted = ", ".join(self.quote_identifier(c) for c in columns)
        placeholders = ", ".join(
            [f":{i + 1}" for i in range(len(columns))]
        )
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
        escaped = name.upper().replace('"', '""')
        return f'"{escaped}"'

    def get_data_type_map(self) -> dict[str, str]:
        return {
            "NUMBER": "NUMBER",
            "INTEGER": "INTEGER",
            "FLOAT": "FLOAT",
            "BINARY_FLOAT": "BINARY_FLOAT",
            "BINARY_DOUBLE": "BINARY_DOUBLE",
            "VARCHAR2": "VARCHAR2",
            "NVARCHAR2": "NVARCHAR2",
            "CHAR": "CHAR",
            "NCHAR": "NCHAR",
            "CLOB": "CLOB",
            "NCLOB": "NCLOB",
            "BLOB": "BLOB",
            "RAW": "RAW",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
        }

    # ------------------------------------------------------------------
    # Health Check (Oracle-specific: DUAL)
    # ------------------------------------------------------------------

    def health_check(self, conn: Any) -> bool:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        cursor.close()
        return result is not None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _column_type_sql(col: ColumnInfo) -> str:
        base = (col.data_type or "").strip().upper()
        if not base:
            return "VARCHAR2(4000)"

        # Respect explicit type parameters from mapper/config.
        if "(" in base and base.endswith(")"):
            return base

        if base in {"NUMBER", "NUMERIC", "DECIMAL"} and col.precision is not None:
            if col.scale is not None:
                return f"{base}({col.precision},{col.scale})"
            return f"{base}({col.precision})"

        if base in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"}:
            if col.precision is not None:
                return f"{base}({col.precision})"
            if base == "VARCHAR2":
                return "VARCHAR2(4000)"
            if base == "NVARCHAR2":
                return "NVARCHAR2(2000)"
            if base == "RAW":
                return "RAW(2000)"
            return f"{base}(1)"

        return base
