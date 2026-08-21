"""Dosya uc tipi kapisi (FFENGINE_FILE_TYPES) — kullanici karari 2026-08-19."""

import logging

from ffengine.core.file_types import (
    FILE_TYPES_ENV,
    KNOWN_FILE_TYPES,
    enabled_file_types,
    is_file_type_enabled,
)


def test_default_enables_both_types(monkeypatch):
    # Geriye uyum: env verilmediginde mevcut davranis (ikisi de acik) korunur.
    monkeypatch.delenv(FILE_TYPES_ENV, raising=False)
    assert enabled_file_types() == KNOWN_FILE_TYPES
    assert is_file_type_enabled("csv")
    assert is_file_type_enabled("json")


def test_types_can_be_toggled_independently(monkeypatch):
    monkeypatch.setenv(FILE_TYPES_ENV, "csv")
    assert enabled_file_types() == frozenset({"csv"})
    assert is_file_type_enabled("csv")
    assert not is_file_type_enabled("json")

    monkeypatch.setenv(FILE_TYPES_ENV, "json")
    assert not is_file_type_enabled("csv")
    assert is_file_type_enabled("json")


def test_value_is_normalized(monkeypatch):
    # Bosluk, buyuk harf ve bos parcalar tolere edilir.
    monkeypatch.setenv(FILE_TYPES_ENV, "  CSV , , JSON ")
    assert enabled_file_types() == KNOWN_FILE_TYPES
    assert is_file_type_enabled(" Csv ")


def test_empty_value_disables_all(monkeypatch):
    monkeypatch.setenv(FILE_TYPES_ENV, "")
    assert enabled_file_types() == frozenset()
    assert not is_file_type_enabled("csv")


def test_unknown_name_is_ignored_with_visible_warning(monkeypatch, caplog):
    # Fail-loud DEGIL: bu deger DAG-parse yolundan da okunabilir; yazim hatasi
    # tum DAG'lari parse edilemez hale getirmemeli. Ama sessiz de degil.
    monkeypatch.setenv(FILE_TYPES_ENV, "csv,parquet")
    with caplog.at_level(logging.WARNING, logger="ffengine.core.file_types"):
        assert enabled_file_types() == frozenset({"csv"})
    assert "parquet" in caplog.text
    assert FILE_TYPES_ENV in caplog.text


def test_known_types_match_schema_contract():
    # Kapi kumesi ile config sozlesmesi ayni kalmali (dairesel import olmasin
    # diye ayri tanimli — bu test kaymayi yakalar).
    from ffengine.config.schema import (
        VALID_SOURCE_FILE_FORMATS,
        VALID_TARGET_FILE_FORMATS,
    )

    assert KNOWN_FILE_TYPES == VALID_SOURCE_FILE_FORMATS
    assert KNOWN_FILE_TYPES == VALID_TARGET_FILE_FORMATS
