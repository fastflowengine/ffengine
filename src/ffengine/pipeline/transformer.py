"""
Transformer — Community satır dönüşüm bileşeni.

Community scope'unda rules=None → identity passthrough.
Gelecekte C09 (Mapping Tools) cast/rename kurallarını buraya bağlayacak.
"""


class Transformer:
    def apply(
        self,
        rows: list[tuple],
        columns: list[dict],
        rules: dict | None = None,
    ) -> list[tuple]:
        """
        Satır listesine dönüşüm uygula.

        Parameters
        ----------
        rows:    İşlenecek satır listesi.
        columns: Kolon metadata'sı (ColumnInfo veya dict).
        rules:   Dönüşüm kuralları haritası; None ise passthrough.

        Returns
        -------
        Dönüştürülmüş satır listesi.
        """
        if not rules:
            return rows

        result = []
        for row in rows:
            result.append(self._apply_row(row, columns, rules))
        return result

    def _apply_row(
        self,
        row: tuple,
        columns: list[dict],
        rules: dict,
    ) -> tuple:
        """Tek bir satıra kural setini uygula."""
        values = list(row)
        for i, col in enumerate(columns):
            col_name = col if isinstance(col, str) else col.get("name", "")
            rule = rules.get(col_name)
            if rule is None:
                continue
            cast = rule.get("cast")
            if cast is not None:
                try:
                    values[i] = cast(values[i])
                except (TypeError, ValueError):
                    pass
        return tuple(values)
