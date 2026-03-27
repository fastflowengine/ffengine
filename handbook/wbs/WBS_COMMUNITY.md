# WBS — Community

## Dalga Planı
- Wave 1: C01 Proje İskeleti ✅ COMPLETED
- Wave 2: C02 DBSession ✅ COMPLETED
- Wave 3: C03 Dialect + C05 YAML Config ✅ COMPLETED
- Wave 4: C04 Engine ✅ COMPLETED / C06 Partition ✅ COMPLETED / C09 Mapping ✅ COMPLETED
- Wave 5: C07 Airflow ✅ COMPLETED / C08 ETL Studio ✅ COMPLETED / C10 Error Handling ✅ COMPLETED
- Wave 6: C11 Integration Test + release prep ✅ COMPLETED

## Epic Listesi
| Epic | Wave | Bağımlılık | Durum | Context |
|---|---|---|---|---|
| C01 | 1 | — | ✅ COMPLETED | `context/C01_PROJECT_SETUP.md` |
| C02 | 2 | C01 | ✅ COMPLETED | `context/C02_DB_SESSION.md` |
| C03 | 3 | C02 | ✅ COMPLETED | `context/C03_DIALECT.md` |
| C05 | 3 | C01 | ✅ COMPLETED | `context/C05_YAML_CONFIG.md` |
| C04 | 4 | C02, C03, C05 | ✅ COMPLETED | `context/C04_CORE_ENGINE.md` |
| C06 | 4 | C02, C05 | ✅ COMPLETED | `context/C06_PARTITION.md` |
| C09 | 4 | C03, C05 | ✅ COMPLETED | `context/C09_MAPPING_TOOLS.md` |
| C07 | 5 | C04, C06 | ✅ COMPLETED | `context/C07_AIRFLOW.md` |
| C08 | 5 | C03, C05, C07 | ✅ COMPLETED | `context/C08_ETL_STUDIO.md` |
| C10 | 5 | C04 | ✅ COMPLETED | `context/C10_ERROR_HANDLING.md` |
| C11 | 6 | C04..C10 | ✅ COMPLETED | `context/C11_INTEGRATION_TEST.md` |

## Task Bazlı Gate Notları
### C04 — Core Engine
- Dosyalar:
  - `pipeline/source_reader.py`
  - `pipeline/streamer.py`
  - `pipeline/target_writer.py`
  - `pipeline/transformer.py`
  - `core/etl_manager.py`
- Test:
  - `test_source_reader.py`
  - `test_streamer.py`
  - `test_target_writer.py`
  - `test_etl_manager.py`
  - `test_pg_to_pg.py`
- Gate:
  - chunk rollback çalışmalı
  - RAM sınırı korunmalı
  - `PythonEngine.is_available() == True`

### C07 — Airflow ✅ COMPLETED
- Dosyalar:
  - `airflow/operator.py` (FFEngineOperator + resolve_dialect + combine_where + aggregate_results)
  - `airflow/dag_generator.py` (generate_dags + register_dags)
  - `airflow/dag_patterns.py` (XComKeys + build_task_group)
  - `airflow/__init__.py` (public API)
- Test:
  - `test_operator.py` (30 tests)
  - `test_airflow_dag.py` (18 tests)
- Gate: ✅ VERIFIED
  - `plan -> prepare -> run` sırası test ile doğrulandi
  - XCom anahtarları (TASK_CONFIG_RESOLVED, PARTITION_SPECS, ROWS_TRANSFERRED, DURATION_SECONDS, ROWS_PER_SECOND)
  - WHERE kombinasyonu (base AND partition)
  - MappingResolver entegrasyonu (C09 deferred item)
  - skip_prepare parametresi (C06 deferred item)

### C10 — Error Handling ✅ COMPLETED
- Kapsam:
  - Community: chunk rollback + task retry + exception sınıflandırma
  - Enterprise sınırı: ack/nack, checkpoint, DLQ bu epic'te uygulanmaz (yalnızca uyumluluk sınırı korunur)
- Hedef dosyalar:
  - `src/ffengine/errors/exceptions.py` (hiyerarşi netleştirme)
  - `src/ffengine/errors/handler.py` (yeni; normalize error handling/format)
  - `src/ffengine/core/etl_manager.py` (typed exception + rollback akışı)
  - `src/ffengine/pipeline/target_writer.py` (yazma hatası davranışı)
  - `src/ffengine/airflow/operator.py` (task retry/telemetry, error summary)
  - `src/ffengine/ui/api_app.py` (API error response tutarlılığı)
- Test:
  - `tests/unit/test_error_handling.py` (yeni)
  - ilgili regresyonlar: `test_etl_manager.py`, `test_target_writer.py`, `test_operator.py`, `test_etl_studio_api.py`
- Gate:
  - chunk hata senaryosunda rollback doğrulanmalı
  - exception hiyerarşisi `handbook/reference/EXCEPTION_MODEL.md` ile uyumlu olmalı
  - API/UI katmanında hatalar deterministik formatta dönmeli
  - Community path'te Enterprise-only policy import/çağrısı olmamalı
  - structured logging alanları `LOGGING_SCHEMA.md` ile hizalı olmalı

### C11 — Integration Test + Release Prep ✅ COMPLETED
- Kapsam:
  - Container tabanlı gerçek DB integration testlerinin aktive edilmesi
  - Mapping generator → config → DAG → run → verify zincirinin doğrulanması
  - Community release prep artefaktlarının tamamlanması
- Hedef dosyalar:
  - `tests/integration/test_pg_to_pg.py` (stabil smoke/e2e)
  - `tests/integration/test_cross_db_etl.py` (skip kaldırma/koşullu aktivasyon)
  - `tests/unit/test_mapping_generator.py` (release prep regresyonları)
  - `docs/community_quickstart.md` (yoksa oluşturulacak)
  - `checkpoints/C11_checkpoint.yaml`, `checkpoints/C11_handoff.md`
- Task listesi (özet):
  - C11_T01: Integration ortamını doğrula (`docker-compose.test.yml`, `.env` test değişkenleri) ✅ DONE
  - C11_T02: `test_cross_db_etl.py` koşullarını aktif et / uygun marker stratejisi ✅ DONE
  - C11_T03: PG->PG/PG->MSSQL/PG->Oracle akışlarını gerçek servislerle çalıştır ✅ DONE
  - C11_T04: mapping_generator → config → DAG → verify akış testini ekle/güncelle ✅ DONE
  - C11_T05: community release prep (quickstart, test komutları, smoke checklist) ✅ DONE
  - C11_T06: C11 checkpoint/handoff ve WBS/README durum kapanışı ✅ DONE
- Gate:
  - C11 zorunlu akışları en az bir kez PASS olmalı (PG→PG, PG→MSSQL, PG→Oracle)
  - Mapping generator zinciri PASS olmalı
  - Release prep dokümanı + çalıştırma komutları doğrulanmalı

## Çıkış Kriteri
Community GA için şu set tamamlanmış olmalıdır:
- Python Engine
- 3 dialect
- YAML config + mapping
- 3 fazlı DAG pattern
- ETL Studio UI
- Email notification, dry-run, structured logging

## Wave 5 Kapanış Adımları (C07 + C08 + C10)
1. C07 gate kanıtlarını doğrula (`test_operator.py`, `test_airflow_dag.py`).
2. C08 gate kanıtlarını doğrula (`tests/unit/test_etl_studio_api.py`, UI + DAG/YAML üretimi).
3. C10 exception modelini `EXCEPTION_MODEL.md` ile birebir hizala.
4. C10 için `errors/handler.py` ve çağıran katmanlarda tek tip error mapping uygula.
5. ETLManager/TargetWriter tarafında chunk rollback + retry davranışını testle güvenceye al.
6. Operator/API katmanında error summary + status code mapping standardize et.
7. Wave 5 gate test paketini çalıştır:
   - unit: `test_operator.py`, `test_etl_studio_api.py`, `test_error_handling.py` (+ ilgili regresyonlar)
   - integration: `test_airflow_dag.py`, `test_community_e2e.py` (varsa)
8. C10 checkpoint/handoff üret, `WBS_COMMUNITY.md` ve `README.md` durumlarını güncelle.

## Wave 6 Kapanış Adımları (C11)
1. Test environment hazırlığını doğrula (`docker/docker-compose.test.yml`, `.env`).
2. Cross-DB integration test marker/skip stratejisini finalize et.
3. Zorunlu akışları çalıştır: PG→PG, PG→MSSQL, PG→Oracle.
4. mapping_generator → config → DAG → run → verify zincirini PASS hale getir.
5. Community quickstart/release prep dokümanını güncelle.
6. C11 checkpoint/handoff üret, `WBS_COMMUNITY.md` ve `README.md` durumlarını `DONE` yap.
