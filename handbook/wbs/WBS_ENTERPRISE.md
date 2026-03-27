# WBS — Enterprise

## Ön Koşul
Community GA tamamlanmış olmalıdır.

## Wave Planı
- Wave 6: E01 C Engine + E02 Queue Runtime
- Wave 7: E03 Native Bulk API + Worker Pool
- Wave 8: E04 DLQ / Retry / Multi-Lane / Guarantee

## Epic Listesi
| Epic | Wave | Bağımlılık | Context |
|---|---|---|---|
| E01 | 6 | Community GA | `context/E01_C_ENGINE.md` |
| E02 | 6 | E01 | `context/E02_QUEUE_RUNTIME.md` |
| E03 | 7 | E01, E02 | `context/E03_BULK_API.md` |
| E04 | 8 | E02, E03 | `context/E04_DLQ_POLICY.md` |

## Task Bazlı İş Kırılımı
### E01 — C Engine
- Oluştur:
  - `src/ffengine/enterprise/engine.py`
- Test:
  - `tests/unit/test_engine_detect.py`
  - fallback smoke test
- Gate:
  - `BaseEngine.detect(auto/community/enterprise)` doğru davranmalı
  - `CEngine.is_available()` sahte native lib ile test edilmeli

### E02 — Queue Runtime
- Oluştur:
  - `enterprise/queue_runtime/envelope.py`
  - `ingress_queue.py`
  - `egress_queue.py`
  - `checkpoint_store.py`
  - `delivery_manager.py`
  - `backpressure.py`
- Test:
  - `tests/unit/test_ingress_queue.py`
  - `tests/unit/test_checkpoint_store.py`
  - `tests/integration/test_enterprise_queue.py`
- Gate:
  - thread-safety
  - ack/nack akışı
  - resume senaryosu

### E03 — Native Bulk API
- Oluştur:
  - `enterprise/bulk/pg_copy.py`
  - `mssql_bcp.py`
  - `oracle_oci.py`
  - `worker_pool.py`
- Test:
  - `tests/unit/test_pg_copy.py`
  - bulk smoke tests
- Gate:
  - varsayılan `reader_workers=3`, `writer_workers=5`, `pipe_queue_max=8`
  - throughput smoke
  - Community path bozulmamalı

### E04 — DLQ / Multi-Lane / Guarantee
- Oluştur:
  - `enterprise/policy/dlq_policy.py`
  - `retry_policy.py`
  - `delivery_policy.py`
  - `multi_lane.py`
- Test:
  - `tests/unit/test_dlq_policy.py`
  - delivery fallback tests
  - `tests/integration/test_enterprise_e2e.py`
- Gate:
  - Guarantee Matrix ile uyum
  - poison message → DLQ
  - ordering key lane testi

## Çıkış Kriteri
Enterprise release için şu set tamamlanmış olmalıdır:
- Queue runtime
- Native bulk API
- N/M worker pool
- DLQ / retry / multi-lane
- Delivery Guarantee Matrix uygulanmış
