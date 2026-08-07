import pytest
from unittest.mock import MagicMock
from ffengine.errors.exceptions import ReconciliationError
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
    assert result["rows"] == 3


def test_stream_empty_source(writer):
    streamer = Streamer()
    result = streamer.stream(iter([]), writer=writer, task_config={})
    assert result["rows"] == 0
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
    streamer.stream(
        iter(chunks), writer=writer, transformer=transformer, task_config={}
    )

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


# ------------------------------------------------------------------
# F3.3 K1 — aktarım muhasebesi (T-F3.3-1, T-F3.3-2)
# ------------------------------------------------------------------


def test_stream_with_counts_reconciles_equal_counts(writer):
    """T-F3.3-1: okunan = yazılan + reddedilen → geçer, sayaçlar döner."""
    streamer = Streamer()
    chunks = [[(1,), (2,)], [(3,)]]
    result = streamer.stream(iter(chunks), writer=writer, task_config={})
    assert result["rows_read"] == 3
    assert result["rows_written"] == 3
    assert result["rows_rejected"] == 0
    # Legacy anahtar aynı anlamla korunur (additive genişleme).
    assert result["rows"] == 3


def test_stream_counts_are_zero_for_empty_source(writer):
    streamer = Streamer()
    result = streamer.stream(iter([]), writer=writer, task_config={})
    assert result["rows_read"] == 0
    assert result["rows_written"] == 0
    assert result["rows_rejected"] == 0


def test_rows_read_counted_before_transformer(writer):
    """Okuma tanımı kaynağa aittir: transformer satır sayısını değiştirse
    bile rows_read kaynaktan okunanı sayar (passthrough olsa da mühürle)."""
    streamer = Streamer()
    transformer = MagicMock()
    transformer.apply.side_effect = lambda rows, columns, rules: rows[:1]
    chunks = [[(1,), (2,), (3,)]]
    with pytest.raises(ReconciliationError):
        streamer.stream(
            iter(chunks),
            writer=writer,
            transformer=transformer,
            task_config={},
        )


def test_row_loss_fails_before_finalize_and_aborts():
    """T-F3.3-2 (INV-1): writer satır düşürürse partition fail-loud;
    finalize ÇAĞRILMAZ (dosya hedefinde temp promote edilmemeli)."""
    writer = MagicMock()
    writer.write_batch.side_effect = lambda rows, cfg: len(rows) - 1
    streamer = Streamer()
    with pytest.raises(ReconciliationError) as exc:
        streamer.stream(iter([[(1,), (2,)]]), writer=writer, task_config={})
    writer.finalize.assert_not_called()
    writer.abort.assert_called_once()
    details = exc.value.details
    assert details["rows_read"] == 2
    assert details["rows_written"] == 1
    assert details["delta"] == 1


def test_reconciliation_cleanup_falls_back_to_rollback_batch():
    """abort() olmayan writer'da (bulk yolu) rollback_batch denenir."""
    writer = MagicMock(spec=["write_batch", "rollback_batch", "finalize"])
    writer.write_batch.side_effect = lambda rows, cfg: 0
    streamer = Streamer()
    with pytest.raises(ReconciliationError):
        streamer.stream(iter([[(1,)]]), writer=writer, task_config={})
    writer.rollback_batch.assert_called_once()
    writer.finalize.assert_not_called()


def test_cleanup_failure_does_not_mask_reconciliation_error():
    writer = MagicMock()
    writer.write_batch.side_effect = lambda rows, cfg: 0
    writer.abort.side_effect = RuntimeError("cleanup boom")
    streamer = Streamer()
    with pytest.raises(ReconciliationError) as exc:
        streamer.stream(iter([[(1,)]]), writer=writer, task_config={})
    assert "cleanup" in str(exc.value.details.get("cleanup_error", "")).lower()


def test_under_report_cannot_be_offset_by_later_over_report():
    """Bir chunk eksik sayarsa sonraki chunk'ın fazla sayımı mahsuplaşamaz:
    over-report anında fail-loud."""
    writer = MagicMock()
    writer.write_batch.side_effect = [0, 2]
    streamer = Streamer()
    with pytest.raises(ReconciliationError):
        streamer.stream(
            iter([[(1,)], [(2,)]]), writer=writer, task_config={}
        )


@pytest.mark.parametrize("bad", [-1, True, "1", None, 1.5])
def test_invalid_write_batch_return_is_fail_loud(bad):
    writer = MagicMock()
    writer.write_batch.side_effect = lambda rows, cfg: bad
    streamer = Streamer()
    with pytest.raises(ReconciliationError):
        streamer.stream(iter([[(1,)]]), writer=writer, task_config={})


def test_finalize_authoritative_count_is_used_when_provided():
    """Bulk yolu (D1): finalize() int döndürürse otoritatif written odur."""
    writer = MagicMock()
    writer.write_batch.side_effect = lambda rows, cfg: len(rows)
    writer.finalize.return_value = 2
    streamer = Streamer()
    result = streamer.stream(iter([[(1,), (2,)]]), writer=writer, task_config={})
    assert result["rows_written"] == 2
    assert result["rows_read"] == 2


def test_finalize_authoritative_count_mismatch_fails_loud():
    writer = MagicMock()
    writer.write_batch.side_effect = lambda rows, cfg: len(rows)
    writer.finalize.return_value = 1
    streamer = Streamer()
    with pytest.raises(ReconciliationError) as exc:
        streamer.stream(iter([[(1,), (2,)]]), writer=writer, task_config={})
    assert exc.value.details["stage"] == "finalize"
