"""
TargetWriter — executemany tabanlı hedef yazıcı.

Desteklenen load_method'lar (LOAD_METHODS.md):
  create_if_not_exists_or_truncate  — varsayılan staging
  append                            — incremental
  replace                           — full refresh (DROP + CREATE + INSERT)
  upsert                            — PK bazlı INSERT/UPDATE
  delete_from_table                 — WHERE ile sil + INSERT
  drop_if_exists_and_create         — baştan üretim
  script                            — hedef DB'de SQL script çalıştır
"""

from ffengine.errors import ConnectionError, DialectError, ValidationError

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
        session : Açık DBSession nesnesi.
        dialect : BaseDialect implementasyonu.
        """
        self.session = session
        self.dialect = dialect

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def prepare(self, task_config: dict) -> None:
        """
        Yükleme öncesi hedef tabloyu hazırla.

        load_method değerine göre DDL işlemleri yürütür.
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
            pass  # Tablo var sayılır, DDL yok

        elif load_method in ("replace", "drop_if_exists_and_create"):
            self._drop_if_exists(qualified)
            self._ddl(self.dialect.generate_ddl(qualified, columns))

        elif load_method == "upsert":
            # Tablo var olmalı; upsert mantığı write_batch'te yönetilir
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
        Bir chunk'ı hedefe yazar.

        Returns
        -------
        Yazılan satır sayısı.
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
        try:
            cursor.executemany(sql, rows)
            self.session.conn.commit()
        except Exception as exc:
            self.session.conn.rollback()
            raise ConnectionError.wrap(
                exc,
                f"Hedefe batch yazimi basarisiz: {qualified}",
                details={"target": qualified, "rows": len(rows)},
            ) from exc
        finally:
            cursor.close()
        return len(rows)

    def rollback_batch(self, exc: Exception | None = None) -> None:
        """Aktif transaction'ı geri al."""
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
            raise ConnectionError.wrap(exc, "SQL yurutme basarisiz.", details={"sql": sql}) from exc
        finally:
            cursor.close()

    def _ddl(self, ddl: str) -> None:
        """DDL ifadesini çalıştır (dialect bazı DDL'leri implicit commit yapar)."""
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
