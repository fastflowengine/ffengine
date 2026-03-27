"""
SourceReader — fetchmany tabanlı chunk okuyucu.

DBSession + BaseDialect üzerine inşa edilir; server-side cursor ile
bellek baskısı olmadan büyük tabloları stream eder.
"""

from typing import Generator


class SourceReader:
    def __init__(self, session, config: dict, dialect):
        """
        Parameters
        ----------
        session:  Açık DBSession nesnesi (conn mevcut olmalı).
        config:   Task config dict.
                  Beklenen anahtarlar:
                    source_schema  : str
                    source_table   : str
                    source_columns : list[str] | None  (None → SELECT *)
                    where_clause   : str | None
                    batch_size     : int  (varsayılan 10 000)
        dialect:  BaseDialect implementasyonu.
        """
        self.session = session
        self.config = config
        self.dialect = dialect
        self.batch_size = config.get("batch_size", 10_000)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(self) -> Generator[list[tuple], None, None]:
        """
        Kaynak tabloyu batch_size büyüklüğünde chunk'lar halinde yield eder.

        Yields
        ------
        list[tuple]  — her eleman bir satır.
        """
        query = self._build_query()
        cursor = self.session.cursor(server_side=True)
        try:
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break
                yield list(rows)
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_query(self) -> str:
        schema = self.config.get("source_schema", "")
        table = self.config.get("source_table", "")
        columns = self.config.get("source_columns")
        where = self.config.get("where_clause") or self.config.get(
            "_resolved_where"
        )

        if schema:
            qualified = f"{self.dialect.quote_identifier(schema)}.{self.dialect.quote_identifier(table)}"
        else:
            qualified = self.dialect.quote_identifier(table)

        if columns:
            col_list = ", ".join(
                self.dialect.quote_identifier(c) for c in columns
            )
            select = f"SELECT {col_list} FROM {qualified}"
        else:
            select = f"SELECT * FROM {qualified}"

        if where:
            return f"{select} WHERE {where}"
        return select
