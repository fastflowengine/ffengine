"""
F1.5 — FileTargetWriter unit tests (delimited output, atomic promote, M=1).

Covers: temp→rename atomicity — the final path appears only after finalize()
(T-F1.5-1); interrupt/abort drops the temp and never creates the final; M=1
single-writer lock (T-F1.5-2); header + delimiter options. Local `fs` transport.
"""

import json
from datetime import date, datetime
from decimal import Decimal

import pytest

from ffengine.errors.exceptions import FileTargetError
from ffengine.pipeline.file_target_writer import FileTargetWriter
from ffengine.pipeline.file_transport import FileTargetContext


def _ctx(path, **options):
    return FileTargetContext(
        conn_id="fs_default", conn_type="fs", file_path=str(path), options=options
    )


def _temps(tmp_path, name):
    return list(tmp_path.glob(name + ".fftmp-*"))


def test_writes_and_promotes_atomically(tmp_path):
    final = tmp_path / "out.csv"
    w = FileTargetWriter(_ctx(final))
    cfg = {"target_columns": ["id", "name"]}
    w.prepare(cfg)
    # Before finalize: only a temp exists, never the final path.
    assert not final.exists()
    assert len(_temps(tmp_path, "out.csv")) == 1
    w.write_batch([(1, "a"), (2, "b")], cfg)
    assert not final.exists()
    w.finalize()
    assert final.exists()
    assert _temps(tmp_path, "out.csv") == []
    assert final.read_text("utf-8").splitlines() == ["id,name", "1,a", "2,b"]


def test_abort_drops_temp_and_never_creates_final(tmp_path):
    final = tmp_path / "ab.csv"
    w = FileTargetWriter(_ctx(final))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    w.write_batch([(1,)], cfg)
    w.abort()
    assert not final.exists()
    assert _temps(tmp_path, "ab.csv") == []


def test_rollback_batch_aborts(tmp_path):
    final = tmp_path / "rb.csv"
    w = FileTargetWriter(_ctx(final))
    w.prepare({"target_columns": ["id"]})
    w.rollback_batch()
    assert not final.exists()


def test_m1_double_prepare_fails_loud(tmp_path):
    final = tmp_path / "m1.csv"
    w = FileTargetWriter(_ctx(final))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    with pytest.raises(FileTargetError, match="M=1"):
        w.prepare(cfg)
    w.abort()


def test_empty_columns_fails_loud(tmp_path):
    w = FileTargetWriter(_ctx(tmp_path / "e.csv"))
    with pytest.raises(FileTargetError, match="target_columns"):
        w.prepare({"target_columns": []})


def test_no_header_when_disabled(tmp_path):
    final = tmp_path / "nh.csv"
    w = FileTargetWriter(_ctx(final, header=False))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    w.write_batch([(1,)], cfg)
    w.finalize()
    assert final.read_text("utf-8").splitlines() == ["1"]


def test_custom_delimiter(tmp_path):
    final = tmp_path / "d.csv"
    w = FileTargetWriter(_ctx(final, delimiter=";"))
    cfg = {"target_columns": ["id", "name"]}
    w.prepare(cfg)
    w.write_batch([(1, "a")], cfg)
    w.finalize()
    assert final.read_text("utf-8").splitlines() == ["id;name", "1;a"]


def test_finalize_without_prepare_is_noop(tmp_path):
    w = FileTargetWriter(_ctx(tmp_path / "np.csv"))
    w.finalize()  # no handle → no error
    assert not (tmp_path / "np.csv").exists()


# ---------------------------------------------------------------------------
# JSON (JSONL) hedef formati — kullanici karari 2026-08-19
# ---------------------------------------------------------------------------


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def test_json_format_writes_jsonl_objects(tmp_path):
    final = tmp_path / "out.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["id", "name"]}
    w.prepare(cfg)
    w.write_batch([(1, "a"), (2, "b")], cfg)
    w.finalize()
    assert _read_jsonl(final) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_json_format_writes_no_header_row(tmp_path):
    # JSONL'de baslik satiri yoktur: kolon adlari her satirin anahtarlaridir.
    final = tmp_path / "nohdr.jsonl"
    w = FileTargetWriter(_ctx(final, format="json", header=True))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    w.write_batch([(7,)], cfg)
    w.finalize()
    assert _read_jsonl(final) == [{"id": 7}]


def test_json_format_multi_batch_has_no_array_syntax(tmp_path):
    # JSONL muhru: array'e sessiz kayis olursa bu test kirilir.
    final = tmp_path / "multi.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    w.write_batch([(1,), (2,)], cfg)
    w.write_batch([(3,)], cfg)
    w.finalize()
    text = final.read_text("utf-8")
    assert text.count("\n") == 3
    assert not text.lstrip().startswith("[")
    assert "]," not in text and text.rstrip()[-1] != "]"


def test_json_format_preserves_decimal_precision(tmp_path):
    # Decimal -> str: float'a cevirmek sessiz precision kaybi olurdu (INV-1).
    final = tmp_path / "dec.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["amount"]}
    w.prepare(cfg)
    w.write_batch([(Decimal("0.10"),)], cfg)
    w.finalize()
    assert _read_jsonl(final) == [{"amount": "0.10"}]


def test_json_format_serializes_dates_and_null(tmp_path):
    final = tmp_path / "dt.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["d", "ts", "missing"]}
    w.prepare(cfg)
    w.write_batch([(date(2026, 8, 19), datetime(2026, 8, 19, 10, 30), None)], cfg)
    w.finalize()
    assert _read_jsonl(final) == [
        {"d": "2026-08-19", "ts": "2026-08-19T10:30:00", "missing": None}
    ]


def test_json_format_keeps_utf8_characters_raw(tmp_path):
    # ensure_ascii=False muhru: target_encoding=utf-8 anlamli kalmali.
    final = tmp_path / "tr.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["city"]}
    w.prepare(cfg)
    w.write_batch([("Şişli",)], cfg)
    w.finalize()
    assert "Şişli" in final.read_text("utf-8")


def test_json_format_rejects_unserializable_value_fail_loud(tmp_path):
    final = tmp_path / "bin.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["blob"]}
    w.prepare(cfg)
    with pytest.raises(FileTargetError) as exc:
        w.write_batch([(b"\x00\x01",)], cfg)
    assert "bytes" in str(exc.value)
    w.abort()


def test_json_format_atomic_promote_and_abort(tmp_path):
    final = tmp_path / "atomic.jsonl"
    w = FileTargetWriter(_ctx(final, format="json"))
    cfg = {"target_columns": ["id"]}
    w.prepare(cfg)
    w.write_batch([(1,)], cfg)
    assert not final.exists()
    assert len(_temps(tmp_path, "atomic.jsonl")) == 1
    w.finalize()
    assert final.exists() and _temps(tmp_path, "atomic.jsonl") == []

    other = tmp_path / "dropped.jsonl"
    w2 = FileTargetWriter(_ctx(other, format="json"))
    w2.prepare({"target_columns": ["id"]})
    w2.write_batch([(1,)], {"target_columns": ["id"]})
    w2.abort()
    assert not other.exists() and _temps(tmp_path, "dropped.jsonl") == []


def test_missing_format_defaults_to_csv(tmp_path):
    # Geriye uyum muhru: target_file_format tasimayan configler CSV yazar.
    final = tmp_path / "legacy.csv"
    w = FileTargetWriter(_ctx(final))
    cfg = {"target_columns": ["id", "name"]}
    w.prepare(cfg)
    w.write_batch([(1, "a")], cfg)
    w.finalize()
    assert final.read_text("utf-8").splitlines() == ["id,name", "1,a"]


def test_invalid_format_fails_loud(tmp_path):
    w = FileTargetWriter(_ctx(tmp_path / "x.txt", format="xml"))
    with pytest.raises(FileTargetError) as exc:
        w.prepare({"target_columns": ["id"]})
    assert "target_file_format" in str(exc.value)
