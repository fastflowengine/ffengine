import pytest
from unittest.mock import MagicMock, call
from ffengine.pipeline.streamer import Streamer


@pytest.fixture
def writer():
    w = MagicMock()
    w.write_batch.side_effect = lambda rows, cfg: len(rows)
    return w


# ------------------------------------------------------------------
# stream() — temel akış
# ------------------------------------------------------------------


def test_stream_returns_total_rows(writer):
    streamer = Streamer()
    chunks = [[(1,), (2,)], [(3,)]]
    result = streamer.stream(iter(chunks), writer=writer, task_config={})
    assert result == {"rows": 3}


def test_stream_empty_source(writer):
    streamer = Streamer()
    result = streamer.stream(iter([]), writer=writer, task_config={})
    assert result == {"rows": 0}
    writer.write_batch.assert_not_called()


def test_stream_calls_write_batch_per_chunk(writer):
    streamer = Streamer()
    chunks = [[(1,)], [(2,)], [(3,)]]
    streamer.stream(iter(chunks), writer=writer, task_config={"k": "v"})
    assert writer.write_batch.call_count == 3


def test_stream_passes_task_config_to_writer(writer):
    streamer = Streamer()
    cfg = {"target_table": "orders"}
    streamer.stream(iter([[(1,)]]), writer=writer, task_config=cfg)
    writer.write_batch.assert_called_once_with([(1,)], cfg)


# ------------------------------------------------------------------
# Transformer enjeksiyonu
# ------------------------------------------------------------------


def test_stream_applies_transformer(writer):
    transformer = MagicMock()
    transformer.apply.side_effect = lambda rows, **kw: [(r[0] * 10,) for r in rows]

    streamer = Streamer()
    chunks = [[(1,), (2,)]]
    streamer.stream(iter(chunks), writer=writer, transformer=transformer, task_config={})

    transformer.apply.assert_called_once()
    written_rows = writer.write_batch.call_args[0][0]
    assert written_rows == [(10,), (20,)]


def test_stream_without_transformer_passes_raw(writer):
    streamer = Streamer()
    chunks = [[(99,)]]
    streamer.stream(iter(chunks), writer=writer, task_config={})
    written_rows = writer.write_batch.call_args[0][0]
    assert written_rows == [(99,)]


# ------------------------------------------------------------------
# Hata ve rollback
# ------------------------------------------------------------------


def test_stream_rollback_on_write_error(writer):
    writer.write_batch.side_effect = RuntimeError("write fail")
    streamer = Streamer()

    with pytest.raises(RuntimeError, match="write fail"):
        streamer.stream(iter([[(1,)]]), writer=writer, task_config={})

    writer.rollback_batch.assert_called_once()


def test_stream_raises_after_rollback(writer):
    writer.write_batch.side_effect = Exception("db error")
    streamer = Streamer()

    with pytest.raises(Exception):
        streamer.stream(iter([[(1,)]]), writer=writer, task_config={})


# ------------------------------------------------------------------
# Backpressure
# ------------------------------------------------------------------


def test_pipe_queue_max_default():
    s = Streamer()
    assert s.pipe_queue_max == 8


def test_pipe_queue_max_custom():
    s = Streamer(pipe_queue_max=4)
    assert s.pipe_queue_max == 4


def test_stream_multiple_chunks_row_count(writer):
    streamer = Streamer()
    chunks = [[(i,)] for i in range(10)]  # 10 chunk × 1 satır
    result = streamer.stream(iter(chunks), writer=writer, task_config={})
    assert result["rows"] == 10
