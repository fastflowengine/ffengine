"""
Streamer — generator tabanlı pipeline yöneticisi.

PYTHON_ENGINE.md kuralları:
- Backpressure: Queue(maxsize=pipe_queue_max) ile throttle
- Delivery semantics: BEST_EFFORT (per-chunk commit TargetWriter'da)
- Enterprise binary API çağrısı yasak
"""

import time
from queue import Queue, Full


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
        dict: {"rows": int}  — toplam yazılan satır sayısı.
        """
        if task_config is None:
            task_config = {}

        total_rows = 0

        for chunk in source_iter:
            self._apply_backpressure()

            if transformer is not None:
                chunk = transformer.apply(chunk, columns=[], rules=None)

            try:
                written = writer.write_batch(chunk, task_config)
                total_rows += written
                self._mark_done()
            except Exception:
                writer.rollback_batch()
                raise

        return {"rows": total_rows}

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
