from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ColumnInfo:
    """Represents a single column's metadata from a database table."""

    name: str
    data_type: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None


class BaseDialect(ABC):
    """
    Abstract contract for all FFEngine database dialects.
    Every dialect must implement these methods to participate
    in the Flow pipeline.
    """

    # ------------------------------------------------------------------
    # Connection & Cursor
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self, params: dict) -> Any:
        """Create and return a native database connection."""
        ...

    @abstractmethod
    def create_cursor(self, conn: Any, server_side: bool = False) -> Any:
        """Create a cursor; if server_side=True, use a streaming cursor."""
        ...

    # ------------------------------------------------------------------
    # Schema Discovery
    # ------------------------------------------------------------------

    @abstractmethod
    def get_table_schema(self, conn: Any, schema: str, table: str) -> list[ColumnInfo]:
        """Return column metadata for the given table."""
        ...

    @abstractmethod
    def list_schemas(self, conn: Any) -> list[str]:
        """List all available schemas in the database."""
        ...

    @abstractmethod
    def list_tables(self, conn: Any, schema: str) -> list[str]:
        """List all tables in the given schema."""
        ...

    # ------------------------------------------------------------------
    # SQL Generation
    # ------------------------------------------------------------------

    @abstractmethod
    def generate_ddl(self, table_name: str, columns: list[ColumnInfo]) -> str:
        """Generate a deterministic CREATE TABLE statement."""
        ...

    @abstractmethod
    def generate_bulk_insert_query(self, table: str, columns: list[str]) -> str:
        """Generate a parameterized INSERT statement for bulk loading."""
        ...

    @abstractmethod
    def generate_upsert_query(
        self,
        table: str,
        columns: list[str],
        match_columns: list[str],
        update_columns: list[str],
    ) -> str:
        """Generate a parameterized UPSERT statement for bulk loading."""
        ...

    @abstractmethod
    def get_pagination_query(self, query: str, limit: int, offset: int) -> str:
        """Wrap a query with dialect-specific pagination."""
        ...

    # ------------------------------------------------------------------
    # Identifier Quoting
    # ------------------------------------------------------------------

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Quote an identifier (table/column name) per dialect rules."""
        ...

    # ------------------------------------------------------------------
    # Data Type Map
    # ------------------------------------------------------------------

    @abstractmethod
    def get_data_type_map(self) -> dict[str, str]:
        """
        Return a mapping of canonical type names to native SQL types.
        Used by TypeMapper for cross-dialect translation.
        """
        ...

    # ------------------------------------------------------------------
    # Health Check   (concrete default)
    # ------------------------------------------------------------------

    def health_check(self, conn: Any) -> bool:
        """Verify the connection is alive by executing SELECT 1."""
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        return result is not None

    # ------------------------------------------------------------------
    # F1.2 - Push-down enrichment (v1.1 derived columns)
    # ------------------------------------------------------------------

    def _bind_placeholder(self, index: int) -> str:
        """Positional bind placeholder for the given 1-based index (PG default)."""
        return "%s"

    def _expr_ops(self, mode: str, *, alloc=None, quote=None):
        """Dialect expression-render ops (PostgreSQL-style default)."""
        from ffengine.mapping import expression as _expr

        return _expr.ExprOps(mode=mode, alloc=alloc, quote=quote)

    def _compile_row_values(
        self,
        target_columns: list[str],
        value_exprs: dict,
        plain_source_by_target: dict[str, str],
    ) -> tuple[list[str], list[str]]:
        """
        Compile per-target-column VALUES SQL + the ordered bind plan.

        Plain columns emit a placeholder bound to their source value; derived
        columns emit CAST(<rendered expression> AS <target_type>) whose column
        refs bind their source values. `bind_plan` is the ordered source-column
        names filling the placeholders left-to-right (dialect-agnostic order).
        """
        from ffengine.mapping import expression as _expr

        bind_plan: list[str] = []

        def alloc(name: str) -> str:
            bind_plan.append(name)
            return self._bind_placeholder(len(bind_plan))

        ops = self._expr_ops("bind", alloc=alloc)
        value_sql: list[str] = []
        for col in target_columns:
            derived = value_exprs.get(col)
            if derived is not None:
                inner = _expr.render(derived.ast, ops)
                value_sql.append(ops.cast(inner, derived.target_type))
            else:
                value_sql.append(alloc(plain_source_by_target[col]))
        return value_sql, bind_plan

    def generate_enriched_insert_query(
        self,
        table: str,
        target_columns: list[str],
        value_exprs: dict,
        plain_source_by_target: dict[str, str],
    ) -> tuple[str, list[str]]:
        """Single-step INSERT with target-side derived expressions (executemany)."""
        value_sql, bind_plan = self._compile_row_values(
            target_columns, value_exprs, plain_source_by_target
        )
        cols = ", ".join(self.quote_identifier(c) for c in target_columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({', '.join(value_sql)})"
        return sql, bind_plan

    def generate_promotion_select(
        self,
        staging_table: str,
        final_table: str,
        target_columns: list[str],
        value_exprs: dict,
        plain_source_by_target: dict[str, str],
    ) -> str:
        """
        Two-step staging->final promotion SQL (column-ref render mode). The same
        derived expressions render against staging column names; used to prove
        F1.2 value-equality with the single-step path (F2.1 reuses this).
        """
        from ffengine.mapping import expression as _expr

        ops = self._expr_ops("colref", quote=self.quote_identifier)
        select_sql: list[str] = []
        for col in target_columns:
            derived = value_exprs.get(col)
            if derived is not None:
                inner = _expr.render(derived.ast, ops)
                select_sql.append(ops.cast(inner, derived.target_type))
            else:
                select_sql.append(
                    self.quote_identifier(plain_source_by_target[col])
                )
        cols = ", ".join(self.quote_identifier(c) for c in target_columns)
        return (
            f"INSERT INTO {final_table} ({cols}) "
            f"SELECT {', '.join(select_sql)} FROM {staging_table}"
        )
