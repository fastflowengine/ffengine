"""
F1.5 — FileTargetWriter unit tests (delimited output, atomic promote, M=1).

Covers: temp→rename atomicity — the final path appears only after finalize()
(T-F1.5-1); interrupt/abort drops the temp and never creates the final; M=1
single-writer lock (T-F1.5-2); header + delimiter options. Local `fs` transport.
"""

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
