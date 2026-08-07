"""
Streamer — generator tabanlı pipeline yöneticisi.

PYTHON_ENGINE.md kuralları:
- Backpressure: Queue(maxsize=pipe_queue_max) ile throttle
- Delivery semantics: BEST_EFFORT (per-chunk commit TargetWriter'da)
- Enterprise binary API çağrısı yasak
"""

import time
from queue import Queue, Full

from ffengine.errors.exceptions import ReconciliationError


class Streamer:
    def __init__(self, pipe_queue_max: int = 8):
        self.pipe_queue_max = pipe_queue_max
        self.buffer: Queue = Queue(maxsize=pipe_queue_max)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def stream(
        self,
        source_iter,
        writer,
        transformer=None,
        task_config: dict | None = None,
    ) -> dict:
        """
        Kaynak iteratöründen chunk'ları çekip hedefe yazar.

        Parameters
        ----------
        source_iter  : SourceReader.read() gibi bir generator.
        writer       : TargetWriter instance.
        transformer  : Transformer instance (None → passthrough).
        task_config  : write_batch'e iletilen config dict.

        Returns
        -------
        dict
            ``{"rows", "rows_read", "rows_written", "rows_rejected"}``.
            Legacy ``"rows"`` anahtarı aynı anlamıyla (yazılan satır) korunur;
            F3.3 sayaçları additive olarak eklenmiştir.
        """
        if task_config is None:
            task_config = {}

        rows_read = 0
        rows_written = 0
        # F3.3 K1: bu dilimde reject-capture YOK. Satırı sessizce atlayan bir
        # yazma yolu da yok (COPY ON_ERROR / batcherrors kullanılmıyor), bu
        # yüzden sabit 0. Böyle bir mod eklenirse kendi reject sözleşmesini
        # getirmelidir — aksi hâlde kayıp buradaki denklikte patlar.
        rows_rejected = 0

        for chunk in source_iter:
            self._apply_backpressure()

            # Okuma tanımı kaynağa aittir → transformer'dan ÖNCE sayılır.
            chunk_read = len(chunk)
            rows_read += chunk_read

            if transformer is not None:
                chunk = transformer.apply(chunk, columns=[], rules=None)

            try:
                written = writer.write_batch(chunk, task_config)
                rows_written += self._validated_written(written, chunk_read)
                self._mark_done()
            except Exception:
                writer.rollback_batch()
                raise

        # F3.3 — denklik finalize'dan ÖNCE. Dosya hedefinde finalize
        # temp→final promotion'dır: mismatch'te eksik dosya YAYINLANMAMALI.
        self._reconcile(
            writer, rows_read, rows_written, rows_rejected, stage="stream"
        )

        # F1.5 — file targets promote temp→final atomically after the last
        # batch. DB writers have no finalize (per-batch commit) → no-op.
        finalize = getattr(writer, "finalize", None)
        if callable(finalize):
            finalized = finalize()
            # F3.3/D1 — staging→promotion yapan writer (bulk) otoritatif
            # promote sayısını buradan bildirebilir; bildirmezse (None) akış
            # sayacı geçerlidir.
            if isinstance(finalized, int) and not isinstance(finalized, bool):
                rows_written = finalized
                self._reconcile(
                    writer,
                    rows_read,
                    rows_written,
                    rows_rejected,
                    stage="finalize",
                )

        return {
            "rows": rows_written,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_rejected": rows_rejected,
        }

    # ------------------------------------------------------------------
    # F3.3 — aktarım muhasebesi (K1)
    # ------------------------------------------------------------------

    @staticmethod
    def _validated_written(written, chunk_read: int) -> int:
        """Writer'ın bildirdiği sayıyı sözleşmeye göre doğrular.

        Chunk bazında doğrulanır ki bir chunk'ın eksik sayımı başka bir
        chunk'ın fazla sayımıyla mahsuplaşamasın.
        """
        if isinstance(written, bool) or not isinstance(written, int):
            raise ReconciliationError(
                "write_batch() int döndürmeli; aktarım muhasebesi bozuk "
                f"(gelen tip: {type(written).__name__}).",
                details={"chunk_rows": chunk_read},
            )
        if written < 0 or written > chunk_read:
            raise ReconciliationError(
                "write_batch() sozlesme disi satir sayisi bildirdi: "
                f"{written} (0..{chunk_read} bekleniyordu).",
                details={"chunk_rows": chunk_read, "reported": written},
            )
        return written

    def _reconcile(
        self,
        writer,
        rows_read: int,
        rows_written: int,
        rows_rejected: int,
        *,
        stage: str,
    ) -> None:
        if rows_read == rows_written + rows_rejected:
            return
        cleanup_error = self._cleanup_quietly(writer)
        details = {
            "stage": stage,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_rejected": rows_rejected,
            "delta": rows_read - rows_written - rows_rejected,
        }
        if cleanup_error is not None:
            details["cleanup_error"] = cleanup_error
        raise ReconciliationError(
            "Aktarim muhasebesi tutmadi: okunan "
            f"{rows_read} != yazilan {rows_written} + reddedilen "
            f"{rows_rejected}. Partition fail-loud kapatildi.",
            details=details,
        )

    @staticmethod
    def _cleanup_quietly(writer) -> str | None:
        """Mismatch sonrası best-effort temizlik.

        Dosya writer'ında ``abort()`` temp'i düşürür; bulk writer'da
        ``abort`` yoktur → ``rollback_batch()`` provider abort'una gider.
        Temizlik hatası asıl ReconciliationError'ı MASKELEMEZ, yalnız
        details'e yazılır.
        """
        for name in ("abort", "rollback_batch"):
            candidate = getattr(writer, name, None)
            if not callable(candidate):
                continue
            try:
                candidate()
            except Exception as exc:  # pragma: no cover - defensive
                return f"{name}: {type(exc).__name__}: {exc}"
            return None
        return None

    # ------------------------------------------------------------------
    # Backpressure
    # ------------------------------------------------------------------

    def _apply_backpressure(self) -> None:
        """Buffer doluysa yazma hazır olana kadar bekle."""
        while True:
            try:
                self.buffer.put_nowait(1)
                break
            except Full:
                time.sleep(0.01)

    def _mark_done(self) -> None:
        """Buffer'dan bir slot serbest bırak."""
        try:
            self.buffer.get_nowait()
        except Exception:
            pass
