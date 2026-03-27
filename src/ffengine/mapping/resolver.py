"""
C09 — Column mapping çözümleyici.

MappingResolver.resolve(task_config, src_conn, src_dialect, tgt_dialect)
    → MappingResult(source_columns, target_columns, target_columns_meta)

İki mod:
  source       — kaynak tablo introspect edilir, TypeMapper ile çevrilir.
  mapping_file — YAML eşleştirme dosyasından okunur, TypeMapper çağrılmaz.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import yaml

from ffengine.dialects.base import ColumnInfo
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.errors.exceptions import MappingError

VALID_MAPPING_VERSIONS: frozenset[str] = frozenset({"v1"})


# ---------------------------------------------------------------------------
# Çıktı tipi
# ---------------------------------------------------------------------------


@dataclass
class MappingResult:
    """MappingResolver çıktısı — ETLManager'a hazır kolon listeleri."""

    source_columns: list[str]
    target_columns: list[str]
    target_columns_meta: list[ColumnInfo]


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------


def _dialect_name(dialect) -> str:
    """
    Dialect sınıf adından TypeMapper'ın beklediği kısa adı türetir.

    PostgresDialect  → "postgres"
    MSSQLDialect     → "mssql"
    OracleDialect    → "oracle"
    """
    raw = type(dialect).__name__.lower()
    return re.sub(r"dialect$", "", raw) or raw


# ---------------------------------------------------------------------------
# Ana sınıf
# ---------------------------------------------------------------------------


class MappingResolver:
    """
    Kolon eşleştirmesini çözümler.

    Kullanım::

        result = MappingResolver().resolve(task_config, src_conn, src_dialect, tgt_dialect)
        effective_config["source_columns"]      = result.source_columns
        effective_config["target_columns"]      = result.target_columns
        effective_config["target_columns_meta"] = result.target_columns_meta
    """

    def resolve(
        self,
        task_config: dict,
        src_conn,
        src_dialect,
        tgt_dialect,
    ) -> MappingResult:
        """
        Parameters
        ----------
        task_config  : normalize edilmiş task config dict.
        src_conn     : ham kaynak DB bağlantısı (DBSession.conn).
        src_dialect  : kaynak BaseDialect implementasyonu.
        tgt_dialect  : hedef BaseDialect implementasyonu.

        Raises
        ------
        MappingError : schema okuma hatası, YAML parse hatası,
                       UnsupportedTypeError, eksik kolon vb.
        """
        mode = task_config.get("column_mapping_mode", "source")

        if mode == "source":
            return self._resolve_source_mode(
                task_config, src_conn, src_dialect, tgt_dialect
            )
        elif mode == "mapping_file":
            return self._resolve_mapping_file_mode(task_config)
        else:
            raise MappingError(
                f"Bilinmeyen column_mapping_mode: {mode!r}"
            )

    # ------------------------------------------------------------------
    # source modu
    # ------------------------------------------------------------------

    def _resolve_source_mode(
        self,
        task_config: dict,
        src_conn,
        src_dialect,
        tgt_dialect,
    ) -> MappingResult:
        schema = task_config.get("source_schema", "")
        table = task_config.get("source_table", "")

        try:
            all_cols: list[ColumnInfo] = src_dialect.get_table_schema(
                src_conn, schema, table
            )
        except Exception as exc:
            raise MappingError(
                f"Tablo şeması alınamadı: {schema}.{table}: {exc}"
            ) from exc

        passthrough_full = task_config.get("passthrough_full", True)
        if passthrough_full:
            selected = list(all_cols)
        else:
            requested = task_config.get("source_columns") or []
            col_map = {c.name: c for c in all_cols}
            selected = []
            for name in requested:
                if name not in col_map:
                    raise MappingError(
                        f"source_columns'da istenen '{name}' kolonu "
                        f"{schema}.{table} tablosunda bulunamadı."
                    )
                selected.append(col_map[name])

        src_name = _dialect_name(src_dialect)
        tgt_name = _dialect_name(tgt_dialect)

        translated: list[ColumnInfo] = []
        for col in selected:
            try:
                tgt_type = TypeMapper.map_type(col.data_type, src_name, tgt_name)
            except UnsupportedTypeError as exc:
                raise MappingError(
                    f"'{col.name}' kolonu için tür çevirisi başarısız: {exc}"
                ) from exc
            translated.append(
                ColumnInfo(
                    name=col.name,
                    data_type=tgt_type,
                    nullable=col.nullable,
                    precision=col.precision,
                    scale=col.scale,
                )
            )

        cols = [c.name for c in selected]
        return MappingResult(
            source_columns=cols,
            target_columns=cols,
            target_columns_meta=translated,
        )

    # ------------------------------------------------------------------
    # mapping_file modu
    # ------------------------------------------------------------------

    def _load_mapping_file(self, path: str) -> dict:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        except FileNotFoundError as exc:
            raise MappingError(
                f"Mapping dosyası bulunamadı: '{path}'"
            ) from exc
        except yaml.YAMLError as exc:
            raise MappingError(
                f"Mapping dosyası YAML parse hatası '{path}': {exc}"
            ) from exc

        version = data.get("version") if isinstance(data, dict) else None
        if version not in VALID_MAPPING_VERSIONS:
            raise MappingError(
                f"Desteklenmeyen mapping dosyası versiyonu: {version!r}. "
                f"Geçerli: {sorted(VALID_MAPPING_VERSIONS)}"
            )
        return data

    def _resolve_mapping_file_mode(self, task_config: dict) -> MappingResult:
        path = task_config.get("mapping_file")
        mapping = self._load_mapping_file(path)

        entries = mapping.get("columns") or []
        source_columns = []
        target_columns = []
        target_columns_meta = []

        for entry in entries:
            src_col = entry["source_name"]
            tgt_col = entry["target_name"]
            tgt_type = entry["target_type"]
            nullable = entry.get("nullable", True)

            source_columns.append(src_col)
            target_columns.append(tgt_col)
            target_columns_meta.append(
                ColumnInfo(name=tgt_col, data_type=tgt_type, nullable=nullable)
            )

        return MappingResult(
            source_columns=source_columns,
            target_columns=target_columns,
            target_columns_meta=target_columns_meta,
        )
