"""
C09 - Mapping dosyasi ureticisi (design-time arac).
"""

from __future__ import annotations

import os

import yaml

from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.errors.exceptions import MappingError
from ffengine.mapping.resolver import VALID_MAPPING_VERSIONS, _dialect_name
from ffengine.mapping.type_contract import (
    render_type_literal_from_column,
    validate_mapping_object_strict,
)


class MappingGenerator:
    """Kaynak tablodan mapping YAML objesi uretir."""

    def generate(
        self,
        src_conn,
        src_dialect,
        tgt_dialect,
        schema: str,
        table: str,
        version: str = "v1",
    ) -> dict:
        if version not in VALID_MAPPING_VERSIONS:
            raise MappingError(
                f"Gecersiz mapping versiyonu: {version!r}. "
                f"Gecerli: {sorted(VALID_MAPPING_VERSIONS)}"
            )

        try:
            src_cols = src_dialect.get_table_schema(src_conn, schema, table)
        except Exception as exc:
            raise MappingError(f"Tablo semasi alinamadi: {schema}.{table}: {exc}") from exc

        src_name = _dialect_name(src_dialect)
        tgt_name = _dialect_name(tgt_dialect)

        columns = []
        for col in src_cols:
            source_type = render_type_literal_from_column(col)
            try:
                tgt_type = TypeMapper.map_type(source_type, src_name, tgt_name)
            except UnsupportedTypeError as exc:
                raise MappingError(
                    f"'{col.name}' kolonu icin tur cevirisi basarisiz: {exc}"
                ) from exc
            columns.append(
                {
                    "source_name": col.name,
                    "target_name": col.name,
                    "source_type": source_type,
                    "target_type": tgt_type,
                    "nullable": col.nullable,
                }
            )

        mapping_obj = {
            "version": version,
            "source_dialect": src_name,
            "target_dialect": tgt_name,
            "columns": columns,
        }
        try:
            return validate_mapping_object_strict(
                mapping_obj,
                valid_versions=VALID_MAPPING_VERSIONS,
                error_cls=MappingError,
                context=f"{schema}.{table}",
            )
        except MappingError:
            raise
        except Exception as exc:
            raise MappingError(
                f"Generated mapping failed strict validation: {exc}"
            ) from exc

    def save(self, mapping: dict, path: str) -> None:
        parent = os.path.dirname(os.path.abspath(path))
        if not os.path.isdir(parent):
            raise MappingError(f"Hedef dizin mevcut degil: '{parent}'")
        with open(path, "w", encoding="utf-8") as fh:
            yaml.dump(
                mapping,
                fh,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
