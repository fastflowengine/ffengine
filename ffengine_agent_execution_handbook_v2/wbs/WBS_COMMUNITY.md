# WBS — Community

## Dalga Planı
- Wave 1: C01 Proje İskeleti
- Wave 2: C02 DBSession
- Wave 3: C03 Dialect + C05 YAML Config
- Wave 4: C04 Engine + C06 Partition + C09 Mapping
- Wave 5: C07 Airflow + C08 ETL Studio + C10 Error Handling
- Wave 6: C11 Integration Test + release prep

## Epic Listesi
| Epic | Wave | Bağımlılık | Context |
|---|---|---|---|
| C01 | 1 | — | `context/C01_PROJECT_SETUP.md` |
| C02 | 2 | C01 | `context/C02_DB_SESSION.md` |
| C03 | 3 | C02 | `context/C03_DIALECT.md` |
| C05 | 3 | C01 | `context/C05_YAML_CONFIG.md` |
| C04 | 4 | C02, C03, C05 | `context/C04_CORE_ENGINE.md` |
| C06 | 4 | C02, C05 | `context/C06_PARTITION.md` |
| C09 | 4 | C03, C05 | `context/C09_MAPPING_TOOLS.md` |
| C07 | 5 | C04, C06 | `context/C07_AIRFLOW.md` |
| C08 | 5 | C03, C05, C07 | `context/C08_ETL_STUDIO.md` |
| C10 | 5 | C04 | `context/C10_ERROR_HANDLING.md` |
| C11 | 6 | C04..C10 | `context/C11_INTEGRATION_TEST.md` |

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

### C07 — Airflow
- Dosyalar:
  - `airflow/operator.py`
  - `airflow/dag_generator.py`
  - `airflow/dag_patterns.py`
- Test:
  - `test_operator.py`
  - `test_airflow_dag.py`
- Gate:
  - `plan -> prepare -> run` sırası doğrulanmalı
  - XCom anahtarları doğru yazılmalı

## Çıkış Kriteri
Community GA için şu set tamamlanmış olmalıdır:
- Python Engine
- 3 dialect
- YAML config + mapping
- 3 fazlı DAG pattern
- ETL Studio UI
- Email notification, dry-run, structured logging
