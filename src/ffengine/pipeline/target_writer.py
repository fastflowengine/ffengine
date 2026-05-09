"""
TargetWriter - executemany-based target writer.

Supported load methods (LOAD_METHODS.md):
  create_if_not_exists_or_truncate  - default staging
  append                            - incremental
  replace                           - full refresh (DROP + CREATE + INSERT)
  upsert                            - PK-based INSERT/UPDATE
  delete_from_table                 - delete by WHERE + INSERT
  drop_if_exists_and_create         - recreate from scratch
  script                            - run SQL script in target DB
"""

from ffengine.errors import ConnectionError, DialectError, ValidationError
from ffengine.errors.handler import sanitize_sql_preview

_SUPPORTED_LOAD_METHODS = frozenset(
    {
        "create_if_not_exists_or_truncate",
        "append",
        "replace",
        "upsert",
        "delete_from_table",
        "drop_if_exists_and_create",
        "script",
    }
)


class TargetWriter:
    def __init__(self, session, dialect):
        """
        Parameters
        ----------
        session : Open DBSession object.
        dialect : BaseDialect implementation.
        """
        self.session = session
        self.dialect = dialect

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prepare(self, task_config: dict) -> None:
        """
        Prepare target table before load.

        Executes DDL operations depending on load_method.
        """
        load_method = task_config.get("load_method", "append")
        if load_method not in _SUPPORTED_LOAD_METHODS:
            raise ValidationError(f"Desteklenmeyen load_method: {load_method!r}")

        schema = task_config.get("target_schema", "")
        table = task_config.get("target_table", "")
        columns = task_config.get("target_columns_meta", [])

        qualified = self._qualify(schema, table)

        if load_method == "create_if_not_exists_or_truncate":
            self._ddl(self.dialect.generate_ddl(qualified, columns))
            self._exec(f"TRUNCATE TABLE {qualified}")

        elif load_method == "append":
            pass

        elif load_method in ("replace", "drop_if_exists_and_create"):
            self._drop_if_exists(qualified)
            self._ddl(self.dialect.generate_ddl(qualified, columns))

        elif load_method == "upsert":
            pass

        elif load_method == "delete_from_table":
            where = task_config.get("delete_where", "")
            if where:
                self._exec(f"DELETE FROM {qualified} WHERE {where}")
            else:
                self._exec(f"DELETE FROM {qualified}")

        elif load_method == "script":
            script_sql = task_config.get("script_sql", "")
            if script_sql:
                self._exec(script_sql)

    def write_batch(self, rows: list[tuple], task_config: dict) -> int:
        """
        Write one chunk to target.

        Returns
        -------
        Number of written rows.
        """
        if not rows:
            return 0

        schema = task_config.get("target_schema", "")
        table = task_config.get("target_table", "")
        columns = task_config.get("target_columns", [])
        qualified = self._qualify(schema, table)

        try:
            sql = self.dialect.generate_bulk_insert_query(qualified, columns)
        except Exception as exc:
            raise DialectError.wrap(
                exc,
                f"Bulk insert SQL uretilemedi: {qualified}",
                details={"target": qualified},
            ) from exc

        cursor = self.session.cursor(server_side=False)
        adapted_rows = self._adapt_rows_for_insert(rows)
        try:
            cursor.executemany(sql, adapted_rows)
            self.session.conn.commit()
        except Exception as exc:
            self.session.conn.rollback()
            raise ConnectionError.wrap(
                exc,
                f"Hedefe batch yazimi basarisiz: {qualified}. Root cause: {exc}",
                details={
                    "target": qualified,
                    "rows": len(rows),
                    "root_cause": str(exc),
                    "sql_preview": sanitize_sql_preview(sql),
                },
            ) from exc
        finally:
            cursor.close()
        return len(rows)

    def rollback_batch(self, exc: Exception | None = None) -> None:
        """Rollback active transaction."""
        try:
            self.session.conn.rollback()
        except Exception as rollback_exc:
            raise ConnectionError.wrap(
                rollback_exc,
                "Batch rollback basarisiz.",
            ) from rollback_exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _qualify(self, schema: str, table: str) -> str:
        if schema:
            return (
                f"{self.dialect.quote_identifier(schema)}"
                f".{self.dialect.quote_identifier(table)}"
            )
        return self.dialect.quote_identifier(table)

    def _exec(self, sql: str) -> None:
        cursor = self.session.cursor(server_side=False)
        try:
            cursor.execute(sql)
            self.session.conn.commit()
        except Exception as exc:
            self.session.conn.rollback()
            raise ConnectionError.wrap(
                exc,
                "SQL yurutme basarisiz.",
                details={"sql_preview": sanitize_sql_preview(sql)},
            ) from exc
        finally:
            cursor.close()

    def _ddl(self, ddl: str) -> None:
        """Execute DDL statement (some dialects imply commit)."""
        cursor = self.session.cursor(server_side=False)
        try:
            cursor.execute(ddl)
        except Exception as exc:
            raise DialectError.wrap(exc, "DDL yurutme basarisiz.", details={"ddl": ddl}) from exc
        finally:
            cursor.close()

    def _drop_if_exists(self, qualified: str) -> None:
        cursor = self.session.cursor(server_side=False)
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
            self.session.conn.commit()
        except Exception:
            self.session.conn.rollback()
        finally:
            cursor.close()

    def _adapt_rows_for_insert(self, rows: list[tuple]) -> list[tuple]:
        """
        Normalize Python values before executemany().

        psycopg3 does not auto-adapt dict/list values for JSONB inserts when
        passed as plain %s params. For PostgreSQL dialects, wrap JSON-like
        payloads with Jsonb to avoid `cannot adapt type 'dict'` errors.
        """
        dialect_name = type(self.dialect).__name__.lower()
        if "postgres" not in dialect_name:
            return rows

        try:
            from psycopg.types.json import Jsonb
        except Exception:
            return rows

        out: list[tuple] = []
        changed = False
        for row in rows:
            row_out = []
            row_changed = False
            for value in row:
                if isinstance(value, (dict, list)):
                    row_out.append(Jsonb(value))
                    row_changed = True
                else:
                    row_out.append(value)
            if row_changed:
                changed = True
                out.append(tuple(row_out))
            else:
                out.append(row)
        return out if changed else rows
