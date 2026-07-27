"""
F1.4/F1.5 — FlowManager integration over file endpoints (no DB, no operator).

Exercises the real dispatch (build_source_reader/build_target_writer) + Streamer
+ finalize/abort: a file→file transfer, and the fail-loud path where a corrupt
source row aborts the file target so no partial/residual output survives
(T-F1.4-2 + T-F1.5-1/3).
"""

import pytest

from ffengine.core.flow_manager import FlowManager
from ffengine.errors.exceptions import FFEngineError
from ffengine.pipeline.file_transport import FileSourceContext, FileTargetContext


def _cfg(**overrides):
    cfg = {
        "task_group_id": "t",
        "source_type": "csv",
        "target_type": "file",
        "source_columns": ["id", "name"],
        "target_columns": ["id", "name"],
        "batch_size": 10,
    }
    cfg.update(overrides)
    return cfg


def _run(src, out, cfg):
    src_ctx = FileSourceContext(
        conn_id="fs", conn_type="fs", file_path=str(src),
        source_type=cfg["source_type"], options={},
    )
    tgt_ctx = FileTargetContext(
        conn_id="fs", conn_type="fs", file_path=str(out), options={}
    )
    return FlowManager().run_flow_task(
        src_session=src_ctx,
        tgt_session=tgt_ctx,
        src_dialect=None,
        tgt_dialect=None,
        task_config=cfg,
        skip_prepare=False,
    )


def test_file_to_file_end_to_end(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text("id,name\n1,alice\n2,bob\n", encoding="utf-8")
    out = tmp_path / "out.csv"
    result = _run(src, out, _cfg())
    assert result.rows == 2
    assert out.read_text("utf-8").splitlines() == ["id,name", "1,alice", "2,bob"]
    assert not list(tmp_path.glob("out.csv.fftmp-*"))  # temp promoted away


def test_corrupt_row_fails_loud_and_leaves_no_residue(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text("id,name\n1,alice\n2,bob,EXTRA\n", encoding="utf-8")
    out = tmp_path / "out.csv"
    with pytest.raises(FFEngineError, match="satir 3"):
        _run(src, out, _cfg())
    # Atomicity + no residue: the final file never appears and the temp is gone.
    assert not out.exists()
    assert not list(tmp_path.glob("out.csv.fftmp-*"))


def test_json_flat_to_file(tmp_path):
    src = tmp_path / "in.jsonl"
    src.write_text('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n', encoding="utf-8")
    out = tmp_path / "out.csv"
    result = _run(src, out, _cfg(source_type="json"))
    assert result.rows == 2
    assert out.read_text("utf-8").splitlines() == ["id,name", "1,a", "2,b"]
