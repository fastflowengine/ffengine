# C10 Handoff — Error Handling (Community)

## Status
🟩 **COMPLETED** — C10_T01..C10_T09 tamamlandı

## Scope

- Community hata modeli: chunk rollback + task retry + exception standardizasyonu
- Airflow operator ve ETL Studio API katmanında tutarlı error mapping
- Structured logging alanlarının LOGGING_SCHEMA ile hizalanması

## Progress

- ✅ **C10_T01** tamamlandı:
  - `src/ffengine/errors/exceptions.py` exception modeli güçlendirildi (`code`, `details`, `cause`, `wrap`).
  - Domain exception sınıflarına `default_code` eklendi.
  - `tests/unit/test_error_handling.py` eklendi.
  - Doğrulama: `PYTHONPATH=src pytest tests/unit/test_error_handling.py tests/unit/test_partitioner.py -q` → **39 passed**

- ✅ **C10_T02** tamamlandı:
  - `src/ffengine/errors/handler.py` eklendi (`normalize_exception`, `http_status_for`, `error_payload`)
  - `src/ffengine/errors/__init__.py` export güncellendi
  - `src/ffengine/ui/api_app.py` endpoint error handling tek helper üzerinden normalize edildi
  - `tests/unit/test_error_handling.py` handler testleri eklendi
  - `tests/unit/test_etl_studio_api.py` ConnectionError -> HTTP 502 senaryosu eklendi
  - Doğrulama: `PYTHONPATH=src pytest tests/unit/test_error_handling.py tests/unit/test_etl_studio_api.py tests/unit/test_partitioner.py -q` → **57 passed**

- ✅ **C10_T03** tamamlandı:
  - `src/ffengine/core/etl_manager.py` stream aşamasında exception normalizasyonuna alındı.
  - Hata details içine `task_group_id` ve varsa `partition_spec` eklendi.

- ✅ **C10_T04** tamamlandı:
  - `src/ffengine/pipeline/target_writer.py` typed exception standardına geçirildi.
  - `load_method` doğrulaması `ValidationError`; DB/DDL yolları `ConnectionError`/`DialectError`.

- ✅ **C10_T05** tamamlandı:
  - `src/ffengine/airflow/operator.py` retry telemetry (`try_number`, `max_tries`) üretimi.
  - Hata anında `error_summary` xcom push + normalize exception raise eklendi.

- ✅ **C10_T06** tamamlandı:
  - ETL Studio API endpointleri tek tip `_raise_http_from_exception()` helper'ına bağlandı.

- ✅ **C10_T07** tamamlandı:
  - `src/ffengine/core/etl_manager.py` ve `src/ffengine/airflow/operator.py` LOGGING_SCHEMA zorunlu alanlarına uyumlu structured log payload üretir hale getirildi.
  - Hata akışında `error_type`/`error_message` alanları normalize exception ile loglanıyor.

- ✅ **C10_T08 (kısmi)**:
  - `tests/unit/test_target_writer.py`, `tests/unit/test_etl_manager.py`, `tests/unit/test_operator.py` C10 davranışına göre güncellendi.
  - Doğrulama:
    - `PYTHONPATH=src pytest tests/unit/test_target_writer.py tests/unit/test_etl_manager.py tests/unit/test_operator.py tests/unit/test_error_handling.py -q` → **76 passed**
    - `PYTHONPATH=src pytest tests/unit/test_etl_studio_api.py tests/unit/test_partitioner.py -q` → **43 passed**
    - `PYTHONPATH=src pytest tests/unit/test_operator.py tests/unit/test_airflow_dag.py tests/unit/test_etl_studio_api.py tests/unit/test_error_handling.py -q` → **78 passed**
  - Not: `test_community_e2e.py` dosyası repoda bulunmuyor; Wave5 integration gate için ayrı artefakt planı gerekli.

## Planned Tasks

- C10_T01..C10_T09 (`checkpoints/C10_checkpoint.yaml`) ✅ tamamlandı

## Wave 5 Closure Path

1. C10 taskları tamamlandı ✅
2. Unit/regression testleri PASS ✅
3. Wave 5 gate (C07 + C08 + C10) doküman/checkpoint kapanışı tamamlandı ✅

## Notes

- Community scope'ta Enterprise-only policy/queue semantiklerinin uygulanmaması zorunludur.
- Exception kontratı için ana referans: `handbook/reference/EXCEPTION_MODEL.md`.
- Wave 6 başlangıcı için sonraki odak: `C11 Integration Test + release prep`.
