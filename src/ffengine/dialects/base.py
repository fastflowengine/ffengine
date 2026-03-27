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
    in the ETL pipeline.
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
    def get_table_schema(
        self, conn: Any, schema: str, table: str
    ) -> list[ColumnInfo]:
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
    def generate_bulk_insert_query(
        self, table: str, columns: list[str]
    ) -> str:
        """Generate a parameterized INSERT statement for bulk loading."""
        ...

    @abstractmethod
    def get_pagination_query(
        self, query: str, limit: int, offset: int
    ) -> str:
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
