# Python Engine Pattern

## Community Engine İlkesi
Community engine standart DBAPI ile çalışır:
- read: `fetchmany`
- write: `executemany`
- akış: Python generator
- hata davranışı: chunk rollback
- delivery semantics: `BEST_EFFORT`

## SourceReader Kalıbı
```python
class SourceReader:
    def __init__(self, session, config, dialect):
        self.session = session
        self.config = config
        self.dialect = dialect
        self.batch_size = config.get("batch_size", 10000)

    def read(self):
        query = self._build_query()
        cursor = self.session.cursor(server_side=True)
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(self.batch_size)
            if not rows:
                break
            yield rows
```

## Streamer Kalıbı
```python
import time
from queue import Queue

class Streamer:
    def __init__(self, pipe_queue_max: int = 8):
        self.pipe_queue_max = pipe_queue_max
        self.buffer = Queue(maxsize=pipe_queue_max)

    def stream(self, source_iter, writer, transformer=None):
        total = 0
        for chunk in source_iter:
            self._apply_backpressure()
            if transformer:
                chunk = transformer.apply(chunk, columns=[], rules=None)
            total += writer.write_batch(chunk, task_config={})
        return {"rows": total}

    def _apply_backpressure(self):
        if self.buffer.qsize() >= self.pipe_queue_max:
            time.sleep(0.01)
```

## TargetWriter Kalıbı
```python
class TargetWriter:
    def __init__(self, session, dialect):
        self.session = session
        self.dialect = dialect

    def prepare(self, task_config: dict) -> None:
        load_method = task_config["load_method"]
        # CREATE / TRUNCATE / DROP+CREATE / DELETE kararları
        ...

    def write_batch(self, rows: list[tuple], task_config: dict) -> int:
        sql = self.dialect.generate_bulk_insert_query(
            task_config["target_schema"],
            task_config["target_table"],
            task_config["target_columns"],
        )
        cursor = self.session.cursor(server_side=False)
        cursor.executemany(sql, rows)
        return len(rows)

    def rollback_batch(self, exc: Exception | None = None) -> None:
        self.session.conn.rollback()
```

## ETLManager / PythonEngine Kalıbı
```python
class PythonEngine(BaseEngine):
    def __init__(self, reader, streamer, writer, tracker):
        self.reader = reader
        self.streamer = streamer
        self.writer = writer
        self.tracker = tracker

    def is_available(self) -> bool:
        return True

    def run(self, config_path: str, task_group_id: str) -> ETLResult:
        task_config = self._load_task(config_path, task_group_id)
        self.writer.prepare(task_config)
        rows = self.streamer.stream(
            self.reader.read(),
            writer=self.writer,
            transformer=self._build_transformer(task_config),
        )["rows"]
        return ETLResult(
            rows=rows,
            duration_seconds=self.tracker.elapsed,
            throughput=self.tracker.rows_per_sec,
            partitions_completed=1,
            errors=[],
        )
```

## Zorunlu Kurallar
- Community içinde binary COPY, BCP, OCI_BATCH çağırma.
- Queue runtime varsayma.
- DLQ/checkpoint store implement etme.
- `pipe_queue_max` yalnızca throttle/backpressure yardımı için kullanılabilir.
- `load_method=upsert` ise hedef PK/UNIQUE ön koşulunu doğrula.

## Kabul Kriteri
- `SourceReader`, `Streamer`, `TargetWriter`, `ETLManager` birlikte çalışır durumda olmalı.
- `test_pg_to_pg` geçmeli.
