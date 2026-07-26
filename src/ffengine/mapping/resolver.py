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
from dataclasses import dataclass, field
from typing import Any

import yaml

from ffengine.dialects.base import ColumnInfo
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.errors.exceptions import MappingError
from ffengine.mapping import expression as _expr
from ffengine.mapping.type_contract import (
    LENGTH_BEARING_TYPES,
    NUMERIC_PARAM_TYPES,
    parse_type,
    validate_mapping_object_strict,
)

VALID_MAPPING_VERSIONS: frozenset[str] = frozenset({"v1", "v1.1"})


@dataclass(frozen=True)
class DerivedExpr:
    """A v1.1 push-down derived target column (compiled once per task/run)."""

    ast: Any
    refs: tuple[str, ...]      # ordered source-column refs = bind order
    target_type: str


@dataclass
class MappingResult:
    """MappingResolver output - column lists ready for FlowManager."""

    source_columns: list[str]
    target_columns: list[str]
    target_columns_meta: list[ColumnInfo]
    # v1.1 push-down enrichment (empty for v1 / source mode):
    target_value_exprs: dict[str, DerivedExpr] = field(default_factory=dict)
    plain_source_by_target: dict[str, str] = field(default_factory=dict)


def _dedup(names: list[str]) -> list[str]:
    """Order-stable de-duplication (first appearance wins)."""
    return list(dict.fromkeys(names))


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
            return self._resolve_mapping_file_mode(
                task_config, src_conn, src_dialect, tgt_dialect
            )

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
            raise MappingError(f"Tablo semasi alinamadi: {schema}.{table}: {exc}") from exc

        if not all_cols:
            raise MappingError(
                f"Tablo semasi bos dondu: {schema}.{table}. "
                "Source metadata discovery returned no columns."
            )

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
                        f"{schema}.{table} tablosunda bulunamadi."
                    )
                selected.append(col_map[name])

        if not selected:
            raise MappingError(
                f"Kolon mapping bos: {schema}.{table}. "
                "source_columns veya source metadata en az bir kolon icermeli."
            )

        src_name = _dialect_name(src_dialect)
        tgt_name = _dialect_name(tgt_dialect)

        translated: list[ColumnInfo] = []
        for col in selected:
            try:
                tgt_type = TypeMapper.map_type(col.data_type, src_name, tgt_name)
            except UnsupportedTypeError as exc:
                raise MappingError(
                    f"'{col.name}' kolonu icin tur cevirisi basarisiz: {exc}"
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
            raise MappingError(f"Mapping dosyasi bulunamadi: '{path}'") from exc
        except yaml.YAMLError as exc:
            raise MappingError(f"Mapping dosyasi YAML parse hatasi '{path}': {exc}") from exc

        return validate_mapping_object_strict(
            data,
            valid_versions=VALID_MAPPING_VERSIONS,
            error_cls=MappingError,
            context=path,
        )

    def _resolve_mapping_file_mode(
        self, task_config: dict, src_conn, src_dialect, tgt_dialect
    ) -> MappingResult:
        path = task_config.get("mapping_file")
        mapping = self._load_mapping_file(path)
        tgt_name = _dialect_name(tgt_dialect)
        entries = mapping.get("columns") or []

        direct_names = [
            str(e["source_name"]) for e in entries if e.get("source_name")
        ]
        # F1.2 drift check: one metadata query at run start (mapping_file mode).
        live_cols = self._live_source_columns(task_config, src_conn, src_dialect)
        # Expressions may reference any physical source column; fall back to the
        # declared direct columns when live metadata is unavailable (mocks/empty).
        allowed = live_cols if live_cols else set(direct_names)

        target_columns: list[str] = []
        target_columns_meta: list[ColumnInfo] = []
        target_value_exprs: dict[str, DerivedExpr] = {}
        plain_source_by_target: dict[str, str] = {}
        extra_refs: list[str] = []

        for entry in entries:
            tgt_col = str(entry["target_name"])
            tgt_type = str(entry["target_type"])
            nullable = entry.get("nullable", True)
            precision, scale = self._normalize_precision_scale(
                target_type=tgt_type,
                source_precision=None,
                source_scale=None,
                target_dialect=tgt_name,
                column_name=tgt_col,
            )
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
            expr_text = entry.get("expression")
            if expr_text:
                ast = _expr.compile_expression(str(expr_text), allowed)
                refs = _expr.column_refs(ast)
                target_value_exprs[tgt_col] = DerivedExpr(
                    ast=ast, refs=tuple(refs), target_type=tgt_type
                )
                extra_refs.extend(refs)
            else:
                plain_source_by_target[tgt_col] = str(entry["source_name"])

        self._check_source_drift(direct_names, extra_refs, live_cols)
        source_columns = _dedup(direct_names + extra_refs)

        return MappingResult(
            source_columns=source_columns,
            target_columns=target_columns,
            target_columns_meta=target_columns_meta,
            target_value_exprs=target_value_exprs,
            plain_source_by_target=plain_source_by_target,
        )

    @staticmethod
    def _live_source_columns(task_config: dict, src_conn, src_dialect) -> set[str]:
        """One metadata query at task start; empty set when unavailable."""
        if src_conn is None or src_dialect is None:
            return set()
        schema = task_config.get("source_schema", "")
        table = task_config.get("source_table", "")
        try:
            live = src_dialect.get_table_schema(src_conn, schema, table)
        except Exception as exc:
            raise MappingError(
                f"mapping_file drift check: kaynak sema alinamadi "
                f"{schema}.{table}: {exc}"
            ) from exc
        return {c.name for c in live}

    @staticmethod
    def _check_source_drift(
        direct_names: list[str], extra_refs: list[str], live_cols: set[str]
    ) -> None:
        """Fail loud if a referenced physical column is missing from source."""
        if not live_cols:
            return
        referenced = _dedup(list(direct_names) + list(extra_refs))
        missing = [c for c in referenced if c not in live_cols]
        if missing:
            raise MappingError(
                "mapping_file drift: referenced source column(s) not found in "
                f"live source schema: {missing}. Regenerate/repair the mapping."
            )

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
        base, params = parse_type(target_type)
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

        if base in NUMERIC_PARAM_TYPES:
            return source_precision, source_scale

        if base in LENGTH_BEARING_TYPES:
            return self._normalize_length_precision(source_precision), None

        # Integer/date/time/uuid families stay parameterless.
        return None, None

    @staticmethod
    def _normalize_length_precision(value: int | None) -> int | None:
        """
        Normalize length-like precision metadata for text/binary target types.

        Non-positive or invalid values are treated as unknown length so dialect
        DDL builders can apply safe bounded defaults.
        """
        if value is None:
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        if parsed <= 0:
            return None
        return parsed

    @staticmethod
    def _validate_mssql_numeric_limits(
        *,
        base: str,
        params: str | None,
        source_precision: int | None,
        source_scale: int | None,
        column_name: str,
    ) -> None:
        if base not in {"DECIMAL", "NUMERIC", "NUMBER"}:
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
                f"'{column_name}' kolonu icin MSSQL DECIMAL precision limiti asildi: {precision} > 38"
            )
        if precision is not None and scale is not None and scale > precision:
            raise MappingError(
                f"'{column_name}' kolonu icin scale precision'dan buyuk olamaz: "
                f"scale={scale}, precision={precision}"
            )
