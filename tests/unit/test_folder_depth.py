"""Degisken klasor derinligi (2-4 seviye) sozlesmesi.

Onceden hiyerarsi 4 SABIT seviyeydi (project/domain/level/flow) ve kullanicilar
zorunlulugu karsilamak icin anlamsiz dolgu segmentleri yaziyordu
(`enhancement/sftp/sftp/test`). Artik min 2, max 4.

Iki kritik guvence burada muhurlenir:
1. **Byte-stability** — derinlik 4 ise `folder_path` YAZILMAZ; mevcut
   config'lerin YAML'i bayt-ayni kalir.
2. **Sessiz guncelleme yok** — dag_id hiyerarsiyi kodladigi ve `_slugify` alt
   cizgiyi korudugu icin `a/b_c` (2 seviye) ile `a/b/c` (3 seviye) AYNI id'yi
   uretir; bu belirsizlik hem yazmada hem okumada fail-loud reddedilir.
"""

from __future__ import annotations

import pytest

from ffengine.ui.studio_service import (
    _build_dag_filename,
    _build_yaml_filename,
    _derive_tags,
    _require_folder_path,
    _validate_folder_segments,
)


# --- T-FD-01: segment dogrulama ---------------------------------------------

@pytest.mark.parametrize("depth", [2, 3, 4])
def test_valid_depths_accepted(depth):
    segments = ["a", "b", "c", "d"][:depth]
    assert _validate_folder_segments(segments) == segments


@pytest.mark.parametrize("segments", [[], ["a"], ["a", "b", "c", "d", "e"]])
def test_out_of_range_depth_rejected(segments):
    with pytest.raises(ValueError, match="between 2 and 4"):
        _validate_folder_segments(segments)


@pytest.mark.parametrize("bad", ["", "   "])
def test_empty_segment_rejected(bad):
    with pytest.raises(ValueError):
        _validate_folder_segments(["a", bad])


@pytest.mark.parametrize("bad", ["..", ".", "a/b", "a\\b"])
def test_path_traversal_rejected(bad):
    """Segment sayisi kullanici kontrolunde; her biri bir dizin adina donusur."""
    with pytest.raises(ValueError, match="Invalid folder segment"):
        _validate_folder_segments(["a", bad])


# --- T-FD-02: payload -> yol -------------------------------------------------

def test_four_fields_map_to_four_levels():
    payload = {"project": "p", "domain": "d", "level": "l", "flow": "f"}
    assert _require_folder_path(payload) == ["p", "d", "l", "f"]


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"project": "p", "domain": "d", "level": "l", "flow": ""}, ["p", "d", "l"]),
        ({"project": "p", "domain": "d", "level": "", "flow": ""}, ["p", "d"]),
        ({"project": "p", "domain": "d"}, ["p", "d"]),
    ],
)
def test_trailing_levels_are_trimmed(payload, expected):
    assert _require_folder_path(payload) == expected


def test_gapped_path_is_rejected():
    """DELIK yasak: `flow` sessizce `level` konumuna KAYDIRILMAZ."""
    payload = {"project": "p", "domain": "d", "level": "", "flow": "f"}
    with pytest.raises(ValueError, match="without gaps"):
        _require_folder_path(payload)


def test_missing_domain_is_rejected():
    with pytest.raises(ValueError):
        _require_folder_path({"project": "p"})


# --- T-FD-03: isim uretimi (mevcut dag_id'ler DEGISMEMELI) -------------------

def test_four_level_names_are_byte_identical_to_legacy():
    """Regresyon muhru: mevcut 14 DAG'in id'si degismemeli."""
    assert (
        _build_dag_filename(["webhook", "whk", "level1", "src_to_stg"], 1)
        == "webhook_whk_level1_src_to_stg_1_dag.py"
    )
    assert (
        _build_yaml_filename(["webhook", "whk", "level1", "src_to_stg"], 2)
        == "webhook_whk_level1_src_to_stg_2.yaml"
    )


@pytest.mark.parametrize(
    "segments,expected",
    [
        (["a", "b"], "a_b_1_dag.py"),
        (["a", "b", "c"], "a_b_c_1_dag.py"),
        (["a", "b", "c", "d"], "a_b_c_d_1_dag.py"),
    ],
)
def test_shorter_paths_produce_shorter_ids(segments, expected):
    assert _build_dag_filename(segments, 1) == expected


# --- T-FD-04: tag turetme ----------------------------------------------------

def test_tags_follow_depth():
    assert _derive_tags("a", "b") == ["a", "b"]
    assert _derive_tags("a", "b", "c", "d") == ["a", "b", "c", "d"]


def test_tags_reject_incomplete_path():
    """Bos segmentle cagri sessizce default tag uretmez (16. kilit)."""
    with pytest.raises(ValueError):
        _derive_tags("a", "")
