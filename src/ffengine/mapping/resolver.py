"""
C09 - Column mapping resolver.

MappingResolver.resolve(task_config, src_conn, src_dialect, tgt_dialect)
    -> MappingResult(source_columns, target_columns, target_columns_meta)

Modes:
  source       - source table introspection + TypeMapper translation
  mapping_file - read YAML mapping file, no TypeMapper call
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import yaml

from ffengine.dialects.base import ColumnInfo
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.errors.exceptions import MappingError

VALID_MAPPING_VERSIONS: frozenset[str] = frozenset({"v1"})
_TYPE_PATTERN = re.compile(r"^([A-Z][A-Z0-9_ ]*?)(?:\((.+)\))?$")


@dataclass
class MappingResult:
    """MappingResolver output - column lists ready for ETLManager."""

    source_columns: list[str]
    target_columns: list[str]
    target_columns_meta: list[ColumnInfo]


def _dialect_name(dialect) -> str:
    """
    Derive TypeMapper short dialect name from class name.

    PostgresDialect  -> "postgres"
    MSSQLDialect     -> "mssql"
    OracleDialect    -> "oracle"
    """
    raw = type(dialect).__name__.lower()
    return re.sub(r"dialect$", "", raw) or raw


class MappingResolver:
    """Resolves column mapping in source or mapping_file mode."""

    def resolve(
        self,
        task_config: dict,
        src_conn,
        src_dialect,
        tgt_dialect,
    ) -> MappingResult:
        mode = task_config.get("column_mapping_mode", "source")

        if mode == "source":
            return self._resolve_source_mode(
                task_config, src_conn, src_dialect, tgt_dialect
            )
        if mode == "mapping_file":
            return self._resolve_mapping_file_mode(task_config, tgt_dialect)

        raise MappingError(f"Bilinmeyen column_mapping_mode: {mode!r}")

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

            precision, scale = self._normalize_precision_scale(
                target_type=tgt_type,
                source_precision=col.precision,
                source_scale=col.scale,
                target_dialect=tgt_name,
                column_name=col.name,
            )

            translated.append(
                ColumnInfo(
                    name=col.name,
                    data_type=tgt_type,
                    nullable=col.nullable,
                    precision=precision,
                    scale=scale,
                )
            )

        cols = [c.name for c in selected]
        return MappingResult(
            source_columns=cols,
            target_columns=cols,
            target_columns_meta=translated,
        )

    def _load_mapping_file(self, path: str) -> dict:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        except FileNotFoundError as exc:
            raise MappingError(f"Mapping dosyası bulunamadı: '{path}'") from exc
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

    def _resolve_mapping_file_mode(self, task_config: dict, tgt_dialect) -> MappingResult:
        path = task_config.get("mapping_file")
        mapping = self._load_mapping_file(path)
        tgt_name = _dialect_name(tgt_dialect)

        entries = mapping.get("columns") or []
        source_columns: list[str] = []
        target_columns: list[str] = []
        target_columns_meta: list[ColumnInfo] = []

        for entry in entries:
            src_col = entry["source_name"]
            tgt_col = entry["target_name"]
            tgt_type = entry["target_type"]
            nullable = entry.get("nullable", True)

            precision, scale = self._normalize_precision_scale(
                target_type=tgt_type,
                source_precision=None,
                source_scale=None,
                target_dialect=tgt_name,
                column_name=tgt_col,
            )

            source_columns.append(src_col)
            target_columns.append(tgt_col)
            target_columns_meta.append(
                ColumnInfo(
                    name=tgt_col,
                    data_type=tgt_type,
                    nullable=nullable,
                    precision=precision,
                    scale=scale,
                )
            )

        return MappingResult(
            source_columns=source_columns,
            target_columns=target_columns,
            target_columns_meta=target_columns_meta,
        )

    @staticmethod
    def _parse_type(raw: str) -> tuple[str, str | None]:
        normalized = str(raw or "").strip().upper()
        m = _TYPE_PATTERN.match(normalized)
        if not m:
            return normalized, None
        return m.group(1).strip(), m.group(2)

    def _normalize_precision_scale(
        self,
        *,
        target_type: str,
        source_precision: int | None,
        source_scale: int | None,
        target_dialect: str,
        column_name: str,
    ) -> tuple[int | None, int | None]:
        """
        Attach precision/scale only for compatible target type families.
        If target_type already has explicit params, do not duplicate metadata params.
        """
        base, params = self._parse_type(target_type)
        has_explicit_params = params is not None

        if target_dialect == "mssql":
            self._validate_mssql_numeric_limits(
                base=base,
                params=params,
                source_precision=source_precision,
                source_scale=source_scale,
                column_name=column_name,
            )

        if has_explicit_params:
            return None, None

        if base in {"DECIMAL", "NUMERIC", "NUMBER"}:
            return source_precision, source_scale

        if base in {
            "VARCHAR",
            "NVARCHAR",
            "CHAR",
            "NCHAR",
            "VARCHAR2",
            "NVARCHAR2",
            "RAW",
            "VARBINARY",
            "BINARY",
        }:
            return source_precision, None

        # Integer/date/time/uuid families stay parameterless.
        return None, None

    @staticmethod
    def _validate_mssql_numeric_limits(
        *,
        base: str,
        params: str | None,
        source_precision: int | None,
        source_scale: int | None,
        column_name: str,
    ) -> None:
        if base not in {"DECIMAL", "NUMERIC"}:
            return

        precision = source_precision
        scale = source_scale

        if params:
            parts = [p.strip() for p in params.split(",")]
            try:
                if parts and parts[0]:
                    precision = int(parts[0])
                if len(parts) > 1 and parts[1]:
                    scale = int(parts[1])
            except ValueError:
                return

        if precision is not None and precision > 38:
            raise MappingError(
                f"'{column_name}' kolonu için MSSQL DECIMAL precision limiti aşıldı: {precision} > 38"
            )
        if precision is not None and scale is not None and scale > precision:
            raise MappingError(
                f"'{column_name}' kolonu için scale precision'dan büyük olamaz: scale={scale}, precision={precision}"
            )
