# WBS — Community

## Dalga Planı
- Wave 1: C01 Proje İskeleti ✅ COMPLETED
- Wave 2: C02 DBSession ✅ COMPLETED
- Wave 3: C03 Dialect + C05 YAML Config ✅ COMPLETED
- Wave 4: C04 Engine ✅ COMPLETED / C06 Partition ✅ COMPLETED / C09 Mapping ✅ COMPLETED
- Wave 5: C07 Airflow ✅ COMPLETED / C08 ETL Studio ⬜ PENDING / C10 Error Handling ⬜ PENDING
- Wave 6: C11 Integration Test + release prep

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
| C08 | 5 | C03, C05, C07 | ⬜ PENDING | `context/C08_ETL_STUDIO.md` |
| C10 | 5 | C04 | ⬜ PENDING | `context/C10_ERROR_HANDLING.md` |
| C11 | 6 | C04..C10 | ⬜ PENDING | `context/C11_INTEGRATION_TEST.md` |

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

## Çıkış Kriteri
Community GA için şu set tamamlanmış olmalıdır:
- Python Engine
- 3 dialect
- YAML config + mapping
- 3 fazlı DAG pattern
- ETL Studio UI
- Email notification, dry-run, structured logging
