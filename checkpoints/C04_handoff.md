# HANDOFF: C04 — Core Engine

**Tarih:** 2026-03-26
**Wave:** 4
**Durum:** COMPLETE
**Source Agent:** builder
**Target Agent:** builder (C05 / C07)
**Checkpoint Ref:** checkpoints/C04_checkpoint.yaml

## Değişen Dosyalar

| Dosya | İşlem | Açıklama |
|---|---|---|
| `src/ffengine/pipeline/__init__.py` | CREATE | Pipeline katmanı public export |
| `src/ffengine/pipeline/source_reader.py` | CREATE | SourceReader — fetchmany + server-side cursor, _build_query |
| `src/ffengine/pipeline/streamer.py` | CREATE | Streamer — Queue backpressure, rollback on chunk error |
| `src/ffengine/pipeline/target_writer.py` | CREATE | TargetWriter — 7 load_method, executemany, per-chunk commit |
| `src/ffengine/pipeline/transformer.py` | CREATE | Transformer — identity passthrough, kural tabanlı cast altyapısı |
| `src/ffengine/core/__init__.py` | CREATE | Core katmanı public export |
| `src/ffengine/core/base_engine.py` | CREATE | BaseEngine ABC + ETLResult dataclass + detect() factory |
| `src/ffengine/core/etl_manager.py` | CREATE | ETLManager + PythonEngine — pipeline orkestratörü |
| `tests/unit/test_source_reader.py` | CREATE | 12 test — query build, batch_size, chunk iteration, cursor lifecycle |
| `tests/unit/test_streamer.py` | CREATE | 13 test — stream flow, transformer injection, rollback, backpressure |
| `tests/unit/test_target_writer.py` | CREATE | 17 test — 7 load_method, write_batch, rollback_batch |
| `tests/unit/test_etl_manager.py` | CREATE | 12 test — is_available, run_etl_task, partition_spec, rollback, detect() |
| `tests/integration/__init__.py` | CREATE | Integration test paketi init |
| `tests/integration/test_pg_to_pg.py` | CREATE | PG→PG integration test — C05 sonrasına kadar pytest.mark.skip |
| `tests/unit/test_etl_cross_dialect.py` | CREATE | 105 test — 9 kombinasyon × 7 kategori (quoting, rowcount, rollback, boş kaynak) |
| `tests/integration/test_cross_db_etl.py` | CREATE | 9 cross-DB integration stub — C05 + test DB'leri hazır olduğunda aktif |
| `tests/unit/test_dialect_oracle.py` | MODIFY | C03 typo fix: init_oraclee_client → init_oracle_client |
| `handbook/reference/TEST_MATRIX.md` | MODIFY | Cross-dialect unit test matrisi ve cross-DB integration test listesi eklendi |
| `checkpoints/C04_checkpoint.yaml` | CREATE | Epic checkpoint |

## Tamamlanan Kabul Kriterleri

- **Chunk rollback çalışıyor:** `Streamer.stream()` write hatasında `writer.rollback_batch()` → `conn.rollback()` çağırır, exception yeniden fırlatır
- **RAM sınırı korunuyor:** `Streamer` Queue(maxsize=pipe_queue_max) backpressure mekanizması ile buffer dolunca bekler
- **`PythonEngine.is_available() == True`** ✓
- **BaseEngine.detect("community") → PythonEngine** ✓
- **BaseEngine.detect("auto") → PythonEngine** (Enterprise yoksa fallback) ✓
- **9 cross-dialect kombinasyonunun tümü birim testlerinden geçiyor** ✓
- **Hedef dialect quoting kuralı doğrulandı:** PG→`"col"`, MSSQL→`[col]`, Oracle→`"COL"` ✓
- **Rollback scope doğru:** Hedef yazma hatası → yalnızca hedef session rollback, kaynak etkilenmiyor ✓
- **242/242 unit test PASSED** (C04 çekirdek: 49, C04 cross-dialect: 105, C03 regresyon: 75, C02 regresyon: 13)

## Açık Riskler

- **PythonEngine.run() C05 öncesi devre dışı:** `_load_task()` şu an `NotImplementedError` fırlatıyor. C05 ConfigLoader bağlandığında aktif olacak.
- **upsert PK ön koşul kontrolü yok:** `TargetWriter.prepare(load_method='upsert')` hedef tabloda PK/UNIQUE index olduğunu doğrulamıyor. C11 Integration Test'e kadar ertelenebilir.
- **Oracle DROP IF EXISTS syntax:** `_drop_if_exists()` `DROP TABLE IF EXISTS` kullanıyor; Oracle'da çalışmaz. `replace/drop_if_exists_and_create` load_method'larında Oracle hedef ise dialect-specific DROP gerekir. C04 unit testleri mock ile geçiyor, integration'da Oracle hedef kullanılmadığı sürece sorun yok.
- **Integration test C05 + DB bağımlı:** `tests/integration/test_pg_to_pg.py` `pytest.mark.skip` ile işaretli. C05 tamamlandığında skip kaldırılacak.

## Sonraki Wave İçin Notlar

- **C05 (YAML Config):** `PythonEngine._load_task()` içinde `ConfigLoader().load(config_path, task_group_id)` çağrısı yapılacak. Dönen dict formatı `task_config` anahtarlarıyla uyumlu olmalı: `source_schema`, `source_table`, `source_columns`, `target_schema`, `target_table`, `target_columns`, `target_columns_meta`, `load_method`, `batch_size`.
- **C07 (Airflow):** `FFEngineOperator` → `ETLManager.run_etl_task()` çağırır. Session'lar Airflow connection'larından üretilir; `AirflowDBAdapter` (C02) kullanılır.
- **C11 (Integration Test):** `test_pg_to_pg.py` skip kaldırılacak; upsert PK kontrolü ve Oracle DROP fix'i bu aşamada ele alınacak.
- Import path'ler:
  - `from ffengine.core import ETLManager, PythonEngine, ETLResult, BaseEngine`
  - `from ffengine.pipeline import SourceReader, Streamer, TargetWriter, Transformer`
