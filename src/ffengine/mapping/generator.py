"""
C09 — Mapping dosyası üreticisi (design-time araç).

MappingGenerator.generate() kaynak tabloyu introspect eder, TypeMapper ile
çevirir ve mapping YAML dict döndürür.
MappingGenerator.save()    dict'i YAML dosyasına yazar.
"""

from __future__ import annotations

import os

import yaml

from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.errors.exceptions import MappingError
from ffengine.mapping.resolver import VALID_MAPPING_VERSIONS, _dialect_name


class MappingGenerator:
    """
    Kaynak tablodan mapping YAML üretir.

    Kullanım::

        gen = MappingGenerator()
        mapping = gen.generate(
            src_conn, src_dialect, tgt_dialect,
            schema="public", table="orders"
        )
        gen.save(mapping, "mappings/orders.yaml")
    """

    def generate(
        self,
        src_conn,
        src_dialect,
        tgt_dialect,
        schema: str,
        table: str,
        version: str = "v1",
    ) -> dict:
        """
        Parameters
        ----------
        src_conn    : ham kaynak DB bağlantısı.
        src_dialect : kaynak BaseDialect.
        tgt_dialect : hedef BaseDialect.
        schema      : kaynak şema adı.
        table       : kaynak tablo adı.
        version     : mapping dosyası versiyonu (varsayılan "v1").

        Returns
        -------
        mapping dict — save() ile YAML'a yazılabilir.

        Raises
        ------
        MappingError : geçersiz versiyon, tablo schema okuma hatası,
                       UnsupportedTypeError vb.
        """
        if version not in VALID_MAPPING_VERSIONS:
            raise MappingError(
                f"Geçersiz mapping versiyonu: {version!r}. "
                f"Geçerli: {sorted(VALID_MAPPING_VERSIONS)}"
            )

        try:
            src_cols = src_dialect.get_table_schema(src_conn, schema, table)
        except Exception as exc:
            raise MappingError(
                f"Tablo şeması alınamadı: {schema}.{table}: {exc}"
            ) from exc

        src_name = _dialect_name(src_dialect)
        tgt_name = _dialect_name(tgt_dialect)

        columns = []
        for col in src_cols:
            try:
                tgt_type = TypeMapper.map_type(col.data_type, src_name, tgt_name)
            except UnsupportedTypeError as exc:
                raise MappingError(
                    f"'{col.name}' kolonu için tür çevirisi başarısız: {exc}"
                ) from exc
            columns.append({
                "source_name": col.name,
                "target_name": col.name,
                "source_type": col.data_type,
                "target_type": tgt_type,
                "nullable": col.nullable,
            })

        return {
            "version": version,
            "source_dialect": src_name,
            "target_dialect": tgt_name,
            "columns": columns,
        }

    def save(self, mapping: dict, path: str) -> None:
        """
        mapping dict'ini YAML dosyasına yazar.

        Parameters
        ----------
        mapping : generate() çıktısı.
        path    : yazılacak dosya yolu (dizin mevcut olmalı).

        Raises
        ------
        MappingError : dizin mevcut değilse.
        """
        parent = os.path.dirname(os.path.abspath(path))
        if not os.path.isdir(parent):
            raise MappingError(
                f"Hedef dizin mevcut değil: '{parent}'"
            )
        with open(path, "w", encoding="utf-8") as fh:
            yaml.dump(
                mapping,
                fh,
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
            )
