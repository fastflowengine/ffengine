"""
TargetWriter - executemany-based target writer.

Supported load methods (LOAD_METHODS.md):
  create_if_not_exists_or_truncate  - default staging
  append                            - incremental
  replace                           - full refresh (DROP + CREATE + INSERT)
  upsert                            - PK-based INSERT/UPDATE
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
            self._require_columns_meta(columns, qualified)
            self._ddl(self.dialect.generate_ddl(qualified, columns))
            self._exec(f"TRUNCATE TABLE {qualified}")

        elif load_method == "append":
            pass

        elif load_method in ("replace", "drop_if_exists_and_create"):
            self._require_columns_meta(columns, qualified)
            self._drop_if_exists(qualified)
            self._ddl(self.dialect.generate_ddl(qualified, columns))

        elif load_method == "upsert":
            self._require_columns_meta(columns, qualified)
            self._ddl(self.dialect.generate_ddl(qualified, columns))

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
        load_method = task_config.get("load_method", "append")

        # F1.2: v1.1 derived columns -> target-side push-down enrichment.
        value_exprs = task_config.get("target_value_exprs") or {}
        if value_exprs:
            return self._write_batch_enriched(
                rows, task_config, qualified, load_method, value_exprs
            )

        if not columns:
            raise ValidationError(
                f"target_columns bos olamaz: {qualified}. "
                "Column mapping/source metadata en az bir kolon uretmeli."
            )

        if load_method == "upsert":
            match_columns, update_columns = self._resolve_upsert_columns(
                task_config=task_config,
                target_columns=columns,
            )
            self._validate_upsert_rows_for_null_match(
                rows=rows,
                target_columns=columns,
                match_columns=match_columns,
            )
        try:
            if load_method == "upsert":
                sql = self.dialect.generate_upsert_query(
                    qualified,
                    columns,
                    match_columns,
                    update_columns,
                )
            else:
                sql = self.dialect.generate_bulk_insert_query(qualified, columns)
        except Exception as exc:
            raise DialectError.wrap(
                exc,
                f"Yazma SQL uretilemedi: {qualified}",
                details={"target": qualified, "load_method": load_method},
            ) from exc

        cursor = self.session.cursor(server_side=False)
        column_types = self._target_column_types(task_config, columns)
        adapted_rows = self._adapt_rows_for_insert(rows, column_types)
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
                    "load_method": load_method,
                    "root_cause": str(exc),
                    "sql_preview": sanitize_sql_preview(sql),
                },
            ) from exc
        finally:
            cursor.close()
        return len(rows)

    # ------------------------------------------------------------------
    # F1.2 - Push-down enrichment write path
    # ------------------------------------------------------------------

    def _write_batch_enriched(
        self,
        rows: list[tuple],
        task_config: dict,
        qualified: str,
        load_method: str,
        value_exprs: dict,
    ) -> int:
        target_columns = task_config.get("target_columns", [])
        source_columns = task_config.get("source_columns", [])
        plain_source_by_target = task_config.get("plain_source_by_target", {})
        if not target_columns:
            raise ValidationError(
                f"target_columns bos olamaz: {qualified}."
            )
        src_index = {name: i for i, name in enumerate(source_columns)}

        try:
            if load_method == "upsert":
                match_columns, update_columns = self._resolve_upsert_columns(
                    task_config=task_config, target_columns=target_columns
                )
                derived_match = [c for c in match_columns if c in value_exprs]
                if derived_match:
                    raise ValidationError(
                        "upsert_match_columns bir turetilmis (expression) kolon "
                        f"olamaz: {derived_match}"
                    )
                self._validate_enriched_match_not_null(
                    rows, src_index, match_columns, plain_source_by_target
                )
                sql, bind_plan = self.dialect.generate_enriched_upsert_query(
                    qualified, target_columns, value_exprs,
                    plain_source_by_target, match_columns, update_columns,
                )
            else:
                sql, bind_plan = self.dialect.generate_enriched_insert_query(
                    qualified, target_columns, value_exprs, plain_source_by_target
                )
        except ValidationError:
            raise
        except Exception as exc:
            raise DialectError.wrap(
                exc,
                f"Enrichment yazma SQL uretilemedi: {qualified}",
                details={"target": qualified, "load_method": load_method},
            ) from exc

        bind_rows = [
            tuple(row[src_index[name]] for name in bind_plan) for row in rows
        ]
        adapted_rows = self._adapt_rows_for_insert(bind_rows)
        cursor = self.session.cursor(server_side=False)
        try:
            cursor.executemany(sql, adapted_rows)
            self.session.conn.commit()
        except Exception as exc:
            self.session.conn.rollback()
            raise ConnectionError.wrap(
                exc,
                f"Hedefe enriched batch yazimi basarisiz: {qualified}. "
                f"Root cause: {exc}",
                details={
                    "target": qualified,
                    "rows": len(rows),
                    "load_method": load_method,
                    "root_cause": str(exc),
                    "sql_preview": sanitize_sql_preview(sql),
                },
            ) from exc
        finally:
            cursor.close()
        return len(rows)

    def _validate_enriched_match_not_null(
        self,
        rows: list[tuple],
        src_index: dict[str, int],
        match_columns: list[str],
        plain_source_by_target: dict[str, str],
    ) -> None:
        for row_idx, row in enumerate(rows):
            for col in match_columns:
                src = plain_source_by_target[col]
                if row[src_index[src]] is None:
                    raise ValidationError(
                        "upsert_match_columns kolonlarinda NULL deger olamaz. "
                        f"Satir={row_idx}, kolon={col}"
                    )

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

    def _require_columns_meta(self, columns: list, qualified: str) -> None:
        if not columns:
            raise ValidationError(
                f"target_columns_meta bos olamaz: {qualified}. "
                "Prepare phase icin column mapping/source metadata en az bir kolon uretmeli."
            )

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
            raise DialectError.wrap(
                exc, "DDL yurutme basarisiz.", details={"ddl": ddl}
            ) from exc
        finally:
            cursor.close()

    def _drop_if_exists(self, qualified: str) -> None:
        cursor = self.session.cursor(server_side=False)
        try:
            dialect_name = type(self.dialect).__name__.lower()
            if "oracle" in dialect_name:
                cursor.execute(
                    f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {qualified}'; "
                    f"EXCEPTION WHEN OTHERS THEN "
                    f"IF SQLCODE != -942 THEN RAISE; END IF; "
                    f"END;"
                )
            else:
                cursor.execute(f"DROP TABLE IF EXISTS {qualified}")
            self.session.conn.commit()
        except Exception as exc:
            self.session.conn.rollback()
            raise ConnectionError.wrap(
                exc,
                f"Hedef tablo drop islemi basarisiz: {qualified}",
                details={"target": qualified, "operation": "drop_if_exists"},
            ) from exc
        finally:
            cursor.close()

    @staticmethod
    def _is_json_column_type(data_type: str) -> bool:
        base = str(data_type or "").strip().upper().split("(")[0].strip()
        return base in ("JSON", "JSONB")

    def _target_column_types(
        self, task_config: dict, columns: list[str]
    ) -> list[str]:
        """Target data_type per output column (aligned to `columns`).

        Read from target_columns_meta (ColumnInfo or dict), keyed by name so it
        is robust to ordering. Empty string when a column has no metadata.
        """
        type_by_name: dict[str, str] = {}
        for meta in task_config.get("target_columns_meta") or []:
            if isinstance(meta, dict):
                name = str(meta.get("name") or "")
                data_type = str(meta.get("data_type") or "")
            else:
                name = str(getattr(meta, "name", "") or "")
                data_type = str(getattr(meta, "data_type", "") or "")
            if name:
                type_by_name[name] = data_type
        return [type_by_name.get(name, "") for name in columns]

    def _adapt_rows_for_insert(
        self, rows: list[tuple], column_types: list[str] | None = None
    ) -> list[tuple]:
        """
        Normalize Python values before executemany().

        psycopg3 does not auto-adapt dict values for JSONB inserts when passed
        as plain %s params, so JSON payloads are wrapped with Jsonb. A Python
        ``list``, however, is ambiguous: it can come from a JSON/JSONB column
        (wrap as Jsonb) OR from a Postgres array column such as ``text[]``
        (leave as-is so psycopg adapts it to a native array). We disambiguate
        by the target column's declared type; without type info we keep the
        legacy behavior (list -> Jsonb).
        """
        dialect_name = type(self.dialect).__name__.lower()
        if "postgres" not in dialect_name:
            return rows

        try:
            from psycopg.types.json import Jsonb
        except Exception:
            return rows

        def _list_is_json(idx: int) -> bool:
            if not column_types or idx >= len(column_types):
                return True  # no type info -> legacy behavior
            return self._is_json_column_type(column_types[idx])

        out: list[tuple] = []
        changed = False
        for row in rows:
            row_out = []
            row_changed = False
            for idx, value in enumerate(row):
                if isinstance(value, dict):
                    row_out.append(Jsonb(value))
                    row_changed = True
                elif isinstance(value, list):
                    if _list_is_json(idx):
                        row_out.append(Jsonb(value))
                        row_changed = True
                    else:
                        # Postgres array column (text[], int[], ...) -> leave the
                        # Python list; psycopg adapts it to a native array.
                        row_out.append(value)
                else:
                    row_out.append(value)
            if row_changed:
                changed = True
                out.append(tuple(row_out))
            else:
                out.append(row)
        return out if changed else rows

    def _resolve_upsert_columns(
        self,
        *,
        task_config: dict,
        target_columns: list[str],
    ) -> tuple[list[str], list[str]]:
        match_columns_raw = task_config.get("upsert_match_columns") or []
        if not isinstance(match_columns_raw, list):
            raise ValidationError(
                "upsert_match_columns listesi zorunludur (load_method='upsert')."
            )

        normalized_match: list[str] = []
        seen: set[str] = set()
        for raw in match_columns_raw:
            col = str(raw or "").strip()
            if not col or col in seen:
                continue
            seen.add(col)
            normalized_match.append(col)

        if not normalized_match:
            raise ValidationError(
                "upsert_match_columns bos olamaz (load_method='upsert')."
            )

        target_set = set(target_columns)
        invalid = [col for col in normalized_match if col not in target_set]
        if invalid:
            raise ValidationError(
                "upsert_match_columns target_columns alt kumesi olmali. "
                f"Gecersiz kolon(lar): {invalid}"
            )

        update_columns = [col for col in target_columns if col not in set(normalized_match)]
        return normalized_match, update_columns

    def _validate_upsert_rows_for_null_match(
        self,
        *,
        rows: list[tuple],
        target_columns: list[str],
        match_columns: list[str],
    ) -> None:
        index_by_column = {name: idx for idx, name in enumerate(target_columns)}
        match_indexes = [index_by_column[name] for name in match_columns]
        for row_idx, row in enumerate(rows):
            for col_name, col_idx in zip(match_columns, match_indexes):
                if col_idx >= len(row):
                    raise ValidationError(
                        "Upsert satirinda kolon sayisi target_columns ile uyusmuyor."
                    )
                if row[col_idx] is None:
                    raise ValidationError(
                        "upsert_match_columns kolonlarinda NULL deger olamaz. "
                        f"Satir={row_idx}, kolon={col_name}"
                    )
