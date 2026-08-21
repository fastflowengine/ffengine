"""F7.1 — retry blogu sozlesmesi (normalize_retry + build_retry_kwargs).

Retry motoru YAZILMAZ: Airflow'un kendi `default_args.retries` /
`retry_delay` mekanizmasi kullanilir (ARCH-09). Buradaki sozlesme yalnizca
"kullanici ne yazabilir" ve "DAG'a ne emit edilir" sorularini muhurler.

Byte-stability (ARCH-11): `retry` yazmayan legacy config'ler icin normalize
None doner ve DAG'a `default_args` HIC emit edilmez -- mevcut davranis aynen
korunur.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from ffengine.airflow.retry import build_retry_kwargs
from ffengine.ui.studio_service import normalize_retry


# --- T-F7.1-01: omit (byte-stable) ------------------------------------------

def test_missing_block_returns_none():
    assert normalize_retry(None) is None


def test_empty_dict_returns_none():
    assert normalize_retry({}) is None


def test_retries_zero_is_omitted_not_persisted():
    """retries=0 "kapali" demektir; blok config'e HIC yazilmaz.

    Sifiri persist etmek legacy config'lerin YAML'ini degistirirdi.
    """
    assert normalize_retry({"retries": 0}) is None


def test_negative_retries_is_omitted():
    assert normalize_retry({"retries": -3}) is None


# --- T-F7.1-02: normalize -----------------------------------------------------

def test_delay_default_is_written_explicitly():
    """delay verilmezse 60 -- ama normalized ciktida ACIKCA yazilir.

    Gizli default yok: config'i okuyan kisi degeri gorebilmeli.
    """
    assert normalize_retry({"retries": 2}) == {"retries": 2, "delay_seconds": 60}


def test_explicit_delay_is_preserved():
    assert normalize_retry({"retries": 3, "delay_seconds": 120}) == {
        "retries": 3,
        "delay_seconds": 120,
    }


def test_boundary_values_are_accepted():
    assert normalize_retry({"retries": 1, "delay_seconds": 1}) == {
        "retries": 1,
        "delay_seconds": 1,
    }
    assert normalize_retry({"retries": 10, "delay_seconds": 86400}) == {
        "retries": 10,
        "delay_seconds": 86400,
    }


def test_numeric_strings_are_accepted():
    """UI number input'u string gonderebilir; sayisal ise kabul edilir."""
    assert normalize_retry({"retries": "2", "delay_seconds": "90"}) == {
        "retries": 2,
        "delay_seconds": 90,
    }


# --- T-F7.1-03: fail-loud -----------------------------------------------------

def test_non_dict_fails_loud():
    with pytest.raises(ValueError, match="retry"):
        normalize_retry("2")


def test_retries_above_max_fails_loud():
    with pytest.raises(ValueError, match="1..10|retries"):
        normalize_retry({"retries": 11})


def test_non_integer_retries_fails_loud():
    with pytest.raises(ValueError, match="retries"):
        normalize_retry({"retries": "iki"})


def test_float_retries_fails_loud():
    """2.5 deneme diye bir sey yok -- sessizce kirpma yapilmaz."""
    with pytest.raises(ValueError, match="retries"):
        normalize_retry({"retries": 2.5})


def test_delay_below_min_fails_loud():
    with pytest.raises(ValueError, match="delay_seconds"):
        normalize_retry({"retries": 2, "delay_seconds": 0})


def test_delay_above_max_fails_loud():
    with pytest.raises(ValueError, match="delay_seconds"):
        normalize_retry({"retries": 2, "delay_seconds": 86401})


def test_non_integer_delay_fails_loud():
    with pytest.raises(ValueError, match="delay_seconds"):
        normalize_retry({"retries": 2, "delay_seconds": "bir dakika"})


def test_unknown_key_fails_loud():
    """Yazim hatasi sessizce yutulmaz (ornegin `delay` vs `delay_seconds`)."""
    with pytest.raises(ValueError, match="delay|bilinmeyen|unknown"):
        normalize_retry({"retries": 2, "delay": 60})


# --- T-F7.1-04: DAG kwargs ----------------------------------------------------

def test_kwargs_empty_when_retry_absent():
    """Zero-diff: retry yoksa DAG'a default_args EMIT EDILMEZ."""
    assert build_retry_kwargs(None) == {}
    assert build_retry_kwargs({}) == {}


def test_kwargs_map_to_airflow_default_args():
    assert build_retry_kwargs({"retries": 3, "delay_seconds": 120}) == {
        "default_args": {"retries": 3, "retry_delay": timedelta(seconds=120)}
    }


def test_kwargs_apply_delay_default():
    assert build_retry_kwargs({"retries": 1}) == {
        "default_args": {"retries": 1, "retry_delay": timedelta(seconds=60)}
    }


def test_kwargs_reject_invalid_block():
    """Factory yolu da fail-loud: gecersiz blok sessizce yok sayilmaz."""
    with pytest.raises(ValueError):
        build_retry_kwargs({"retries": 99})
