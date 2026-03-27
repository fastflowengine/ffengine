# Epic Çıktı Artefaktları

## Amaç
Her epic için üretilmesi beklenen dosya, test ve doküman listesi.
Agent bu listeyi **tamamlama kriteri** olarak kullanır. Artefakt listesi tam olarak karşılanmadan epic COMPLETED sayılmaz.

---

## Community Epics

### C01 — Proje İskeleti & Altyapı

| Artefakt | Yol | Tür |
|----------|-----|-----|
| pyproject.toml | `/pyproject.toml` | config |
| ffengine paketi | `src/ffengine/__init__.py` | kod |
| Test conftest | `tests/conftest.py` | test |
| Dockerfile | `docker/Dockerfile` | config |
| Docker Compose | `docker/docker-compose.yml` | config |
| CI pipeline | `.github/workflows/ci.yml` | config |
| .gitignore | `/.gitignore` | config |
| Checkpoint | `projects/*/checkpoints/C01_checkpoint.yaml` | süreç |
| Handoff | `wbs/HANDOFF_TEMPLATE.md` formatında | süreç |

**Gate:** `pip install -e .` çalışır, `import ffengine` hatasız, lint + test pipeline aktif.

---

### C02 — DBSession

| Artefakt | Yol | Tür |
|----------|-----|-----|
| DBSession modülü | `src/ffengine/db/session.py` | kod |
| Airflow adapter | `src/ffengine/db/airflow_adapter.py` | kod |
| Unit test | `tests/unit/test_db_session.py` | test |
| Checkpoint | `projects/*/checkpoints/C02_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Rollback on error, commit on success. PG / MSSQL / Oracle conn_id resolution çalışır.

---

### C03 — Dialect

| Artefakt | Yol | Tür |
|----------|-----|-----|
| BaseDialect | `src/ffengine/dialects/base.py` | kod |
| PostgresDialect | `src/ffengine/dialects/postgres.py` | kod |
| MSSQLDialect | `src/ffengine/dialects/mssql.py` | kod |
| OracleDialect | `src/ffengine/dialects/oracle.py` | kod |
| TypeMapper | `src/ffengine/dialects/type_mapper.py` | kod |
| Unit test — PG | `tests/unit/test_dialect_postgres.py` | test |
| Unit test — MSSQL | `tests/unit/test_dialect_mssql.py` | test |
| Unit test — Oracle | `tests/unit/test_dialect_oracle.py` | test |
| Unit test — TypeMapper | `tests/unit/test_type_mapper.py` | test |
| Checkpoint | `projects/*/checkpoints/C03_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** TypeMapper lossless, DDL deterministic, metadata discovery ve quoting kuralları doğru.

---

### C04 — Core Engine

| Artefakt | Yol | Tür |
|----------|-----|-----|
| SourceReader | `src/ffengine/pipeline/source_reader.py` | kod |
| Streamer | `src/ffengine/pipeline/streamer.py` | kod |
| TargetWriter | `src/ffengine/pipeline/target_writer.py` | kod |
| Transformer | `src/ffengine/pipeline/transformer.py` | kod |
| ETLManager | `src/ffengine/core/etl_manager.py` | kod |
| Unit test — SourceReader | `tests/unit/test_source_reader.py` | test |
| Unit test — Streamer | `tests/unit/test_streamer.py` | test |
| Unit test — TargetWriter | `tests/unit/test_target_writer.py` | test |
| Unit test — ETLManager | `tests/unit/test_etl_manager.py` | test |
| Integration test — PG→PG | `tests/integration/test_pg_to_pg.py` | test |
| Checkpoint | `projects/*/checkpoints/C04_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Chunk rollback çalışır, RAM sınırı korunur, `PythonEngine.is_available() == True`.

---

### C05 — YAML Config

| Artefakt | Yol | Tür |
|----------|-----|-----|
| Config loader | `src/ffengine/config/loader.py` | kod |
| Config validator | `src/ffengine/config/validator.py` | kod |
| Config schema | `src/ffengine/config/schema.py` | kod |
| Binding resolver | `src/ffengine/config/binding_resolver.py` | kod |
| Unit test — loader | `tests/unit/test_config_loader.py` | test |
| Unit test — validator | `tests/unit/test_config_validator.py` | test |
| Checkpoint | `projects/*/checkpoints/C05_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Default değerler deterministik, geçersiz config yakalanır, whitelist validasyonu çalışır.

---

### C06 — Partition

| Artefakt | Yol | Tür |
|----------|-----|-----|
| Partitioner | `src/ffengine/partition/partitioner.py` | kod |
| Stratejiler | `src/ffengine/partition/strategies.py` | kod |
| Unit test | `tests/unit/test_partitioner.py` | test |
| Unit test — stratejiler | `tests/unit/test_partition_strategies.py` | test |
| Checkpoint | `projects/*/checkpoints/C06_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** 6 strateji implemente, 3-phase DAG entegrasyonuna hazır.

---

### C07 — Airflow

| Artefakt | Yol | Tür |
|----------|-----|-----|
| FFEngineOperator | `src/ffengine/airflow/operator.py` | kod |
| Auto-DAG Generator | `src/ffengine/airflow/dag_generator.py` | kod |
| DAG Patterns | `src/ffengine/airflow/dag_patterns.py` | kod |
| Unit test — operator | `tests/unit/test_operator.py` | test |
| Unit test — DAG | `tests/unit/test_airflow_dag.py` | test |
| Checkpoint | `projects/*/checkpoints/C07_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** `plan → prepare → run` sırası doğrulanmış, XCom anahtarları doğru.

---

### C08 — ETL Studio

| Artefakt | Yol | Tür |
|----------|-----|-----|
| UI plugin init | `src/ffengine/ui/__init__.py` | kod |
| Views | `src/ffengine/ui/views.py` | kod |
| Templates | `src/ffengine/ui/templates/` | kod |
| Static assets | `src/ffengine/ui/static/` | kod |
| Unit test | `tests/unit/test_ui_views.py` | test |
| Checkpoint | `projects/*/checkpoints/C08_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Schema/table discovery, pipeline form, SQL editor, tag/timeline view çalışır.

---

### C09 — Mapping Tools

| Artefakt | Yol | Tür |
|----------|-----|-----|
| Mapping generator | `src/ffengine/mapping/generator.py` | kod |
| Mapping validator | `src/ffengine/mapping/validator.py` | kod |
| Unit test — generator | `tests/unit/test_mapping_generator.py` | test |
| Unit test — validator | `tests/unit/test_mapping_validator.py` | test |
| Checkpoint | `projects/*/checkpoints/C09_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Versiyonlama (_v1, _v2), TypeMapper uyumu, UI/CLI tutarlılığı.

---

### C10 — Error Handling

| Artefakt | Yol | Tür |
|----------|-----|-----|
| Exception sınıfları | `src/ffengine/errors/exceptions.py` | kod |
| Error handler | `src/ffengine/errors/handler.py` | kod |
| Unit test | `tests/unit/test_error_handling.py` | test |
| Checkpoint | `projects/*/checkpoints/C10_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Community chunk rollback + task retry, exception hiyerarşisi `EXCEPTION_MODEL.md` ile uyumlu.

---

### C11 — Integration Test & Release

| Artefakt | Yol | Tür |
|----------|-----|-----|
| PG → PG e2e | `tests/integration/test_pg_to_pg_e2e.py` | test |
| PG → MSSQL e2e | `tests/integration/test_pg_to_mssql.py` | test |
| PG → Oracle e2e | `tests/integration/test_pg_to_oracle.py` | test |
| Full pipeline test | `tests/integration/test_community_e2e.py` | test |
| Checkpoint | `projects/*/checkpoints/C11_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** mapping_generator → config → DAG → run → verify akışı tam çalışır. Community GA kriteri sağlanır.

---

## Enterprise Epics

### E01 — C Engine

| Artefakt | Yol | Tür |
|----------|-----|-----|
| Enterprise Engine | `src/ffengine/enterprise/engine.py` | kod |
| Unit test — detect | `tests/unit/test_engine_detect.py` | test |
| Fallback smoke test | `tests/unit/test_engine_fallback.py` | test |
| Checkpoint | `projects/*/checkpoints/E01_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** `BaseEngine.detect(auto/community/enterprise)` doğru, `CEngine.is_available()` sahte lib ile test edilmiş.

---

### E02 — Queue Runtime

| Artefakt | Yol | Tür |
|----------|-----|-----|
| FFEnvelope | `src/ffengine/enterprise/queue_runtime/envelope.py` | kod |
| IngressQueue | `src/ffengine/enterprise/queue_runtime/ingress_queue.py` | kod |
| EgressQueue | `src/ffengine/enterprise/queue_runtime/egress_queue.py` | kod |
| CheckpointStore | `src/ffengine/enterprise/queue_runtime/checkpoint_store.py` | kod |
| DeliveryManager | `src/ffengine/enterprise/queue_runtime/delivery_manager.py` | kod |
| Backpressure | `src/ffengine/enterprise/queue_runtime/backpressure.py` | kod |
| Unit test — ingress | `tests/unit/test_ingress_queue.py` | test |
| Unit test — checkpoint | `tests/unit/test_checkpoint_store.py` | test |
| Integration test | `tests/integration/test_enterprise_queue.py` | test |
| Checkpoint | `projects/*/checkpoints/E02_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Thread-safety, ack/nack akışı, resume senaryosu çalışır.

---

### E03 — Native Bulk API

| Artefakt | Yol | Tür |
|----------|-----|-----|
| PG COPY | `src/ffengine/enterprise/bulk/pg_copy.py` | kod |
| MSSQL BCP | `src/ffengine/enterprise/bulk/mssql_bcp.py` | kod |
| Oracle OCI | `src/ffengine/enterprise/bulk/oracle_oci.py` | kod |
| Worker Pool | `src/ffengine/enterprise/bulk/worker_pool.py` | kod |
| Unit test — pg_copy | `tests/unit/test_pg_copy.py` | test |
| Bulk smoke tests | `tests/unit/test_bulk_smoke.py` | test |
| Checkpoint | `projects/*/checkpoints/E03_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** reader_workers=3, writer_workers=5, pipe_queue_max=8 varsayılanları, throughput smoke, Community path bozulmamış.

---

### E04 — DLQ / Multi-Lane / Guarantee

| Artefakt | Yol | Tür |
|----------|-----|-----|
| DLQ Policy | `src/ffengine/enterprise/policy/dlq_policy.py` | kod |
| Retry Policy | `src/ffengine/enterprise/policy/retry_policy.py` | kod |
| Delivery Policy | `src/ffengine/enterprise/policy/delivery_policy.py` | kod |
| Multi-Lane | `src/ffengine/enterprise/policy/multi_lane.py` | kod |
| Unit test — DLQ | `tests/unit/test_dlq_policy.py` | test |
| Delivery fallback tests | `tests/unit/test_delivery_fallback.py` | test |
| Integration test — e2e | `tests/integration/test_enterprise_e2e.py` | test |
| Checkpoint | `projects/*/checkpoints/E04_checkpoint.yaml` | süreç |
| Handoff | — | süreç |

**Gate:** Guarantee Matrix uyumu, poison message → DLQ, ordering key lane testi çalışır.

---

## Artefakt Tamamlama Kuralları

1. Her **kod** türündeki artefaktın karşılığında en az bir **test** türünde artefakt olmalıdır.
2. Her epic sonunda **checkpoint** kapatılmış (`status: COMPLETED`) ve **handoff** üretilmiş olmalıdır.
3. Gate testleri **PASS** olmadan artefakt listesi "tamamlandı" sayılmaz.
4. `wbs/REVIEW_PROMPT.md` checklist'i uygulanmış olmalıdır.
5. Dosya yolları `reference/PROJECT_STRUCTURE.md` ile uyumlu olmalıdır.
