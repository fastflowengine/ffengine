"""Dosya uc tipleri icin deployment kapisi (kullanici karari 2026-08-19).

`csv` ve `json` uclari her kurulumda acikti ve kapatilamiyordu. Bu modul
tiplerin AYRI AYRI acilip kapatilmasini saglar; ayar deployment-owned bir env
degiskenidir (`FFENGINE_FILE_TYPES`), boylece yetkilendirme Airflow/konteyner
sahibine ait olur -- Community'de rol kavrami yoktur (bkz. ``core/edition.py``).

Neden Airflow Variable degil: whitelist kontrolu DAG-parse yolunda calisir ve
orada ag/DB erisimi YASAKTIR (``config/schema.py`` ustundeki sozlesme notu).
Variable okumak metadata DB'ye gitmek demektir; env okumasi saftir.

Kapi YALNIZCA yeni akis kurmayi engeller (``ui/api_app.py`` payload katmani,
422). ``config/validator.py``'ye KONMAZ: bir tip sonradan kapatilirsa halihazirda
calisan config'lerin DAG'lari parse edilemez hale gelirdi.

Varsayilan: her iki tip de ACIK -- geriye uyum.
"""

from __future__ import annotations

import logging
import os

FILE_TYPES_ENV = "FFENGINE_FILE_TYPES"

# Kapinin tanidigi tipler. Bu kume `config.schema.VALID_SOURCE_FILE_FORMATS` ile ayni
# olmalidir; ayri tutulur cunku schema import'u dairesel bagimlilik yaratir.
KNOWN_FILE_TYPES: frozenset[str] = frozenset({"csv", "json"})

_log = logging.getLogger(__name__)


def enabled_file_types() -> frozenset[str]:
    """Aktif dosya uc tipleri (varsayilan: hepsi).

    Bilinmeyen ad yok sayilir + uyari loglanir: env deployment-owned'dir ve bu
    deger DAG-parse yolundan da okunabilir; bir yazim hatasi yuzunden fail-loud
    atmak tum DAG'lari parse edilemez hale getirirdi. INV-1 sessiz VERI
    sapmasini yasaklar; burada sessizlik degil, gorunur bir uyari vardir.
    """
    raw = os.environ.get(FILE_TYPES_ENV)
    if raw is None:
        return KNOWN_FILE_TYPES
    names = {part.strip().lower() for part in str(raw).split(",")}
    names.discard("")
    unknown = names - KNOWN_FILE_TYPES
    if unknown:
        _log.warning(
            "%s: bilinmeyen dosya tipi yok sayildi: %s (taninanlar: %s)",
            FILE_TYPES_ENV,
            sorted(unknown),
            sorted(KNOWN_FILE_TYPES),
        )
    return frozenset(names & KNOWN_FILE_TYPES)


def is_file_type_enabled(name: str) -> bool:
    """True iff ``name`` dosya ucu bu kurulumda kullanilabilir."""
    return str(name or "").strip().lower() in enabled_file_types()
