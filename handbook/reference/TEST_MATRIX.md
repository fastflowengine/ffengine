# Test Matrix

## Unit
- Config parser / validator
- DBSession commit/rollback
- Her dialect için schema + DDL + bulk insert query + pagination query
- SourceReader chunk boyutu ve sabit RAM
- Streamer backpressure throttle
- TargetWriter load_method davranışları
- Transformer TABLE_RULES
- Partitioner stratejileri
- Airflow binding resolver
- Queue runtime unit tests
- Checkpoint store recovery
- Delivery policy resolution

### Cross-Dialect ETL Unit Testleri (C04)

`tests/unit/test_etl_cross_dialect.py` — 105 test, tümü mock tabanlı

Her kombinasyon için doğrulananlar:

| Kategori | Test sayısı | Açıklama |
|---|---|---|
| Row count | 9 | 9 kombinasyonun tümünde ETLResult.rows doğru |
| Hedef sütun quoting | 9 | INSERT SQL hedef dialect'ın `quote_identifier` kuralını uyguluyor |
| Hedef tablo quoting | 9 | INSERT SQL tablo adı hedef dialect ile quote edilmiş |
| Kaynak tablo quoting | 9 | SELECT SQL kaynak dialect'ın kuralını uyguluyor |
| ETLResult type | 9 | ETLResult instance döner |
| errors == [] | 9 | Başarılı akışta hata listesi boş |
| duration >= 0 | 9 | Süre negatif değil |
| throughput >= 0 | 9 | Throughput negatif değil |
| partitions_completed | 9 | Her çalışma 1 partition |
| Self-transfer row count | 3 | PG→PG, MSSQL→MSSQL, Oracle→Oracle |
| Self-transfer quoting | 3 | Self-transfer'da kaynak ve hedef aynı kuralı kullanıyor |
| Rollback scope | 9 | Hata → hedef session rollback, kaynak session etkilenmiyor |
| Boş kaynak | 9 | rows==0, INSERT çağrılmıyor |

Quoting kuralları özeti:

| Dialect | Sütun quoting | Tablo quoting örneği |
|---|---|---|
| PostgreSQL | `"id"` | `"tgt"."employees"` |
| MSSQL | `[id]` | `[tgt].[employees]` |
| Oracle | `"ID"` (uppercase) | `"TGT"."EMPLOYEES"` |

## Integration
- PG → PG
- PG → MSSQL
- PG → Oracle
- MSSQL → PG
- MSSQL → MSSQL
- MSSQL → Oracle
- Oracle → PG
- Oracle → MSSQL
- Oracle → Oracle
- Mapping generator → YAML → DAG → run → verify tam akış
- Airflow 3-faz DAG order testi
- Community E2E
- Enterprise queue runtime E2E
- Enterprise bulk E2E
- Enterprise DLQ / retry / resume senaryosu

### Cross-DB Integration Testleri (C05 sonrası)

`tests/integration/test_cross_db_etl.py` — 9 test, şu an `pytest.mark.skip`

Aktifleştirme koşulları:
- C05 (YAML Config) tamamlanmış
- `docker/docker-compose.test.yml` içinde `postgres-test`, `mssql-test`, `oracle-test` servisleri çalışıyor
- `.env` dosyasında `PG_TEST_*`, `MSSQL_TEST_*`, `ORACLE_TEST_*` değişkenleri tanımlı

## Wave Gate
### Community
- W1: config + session unit testleri geçmeli
- W2: dialect + type mapper + DDL testleri geçmeli
- W3: `test_source_reader`, `test_streamer`, `test_target_writer`, `test_etl_manager`, `test_pg_to_pg`
- W4: `test_operator`, `test_validate_config`, `test_partitioner`, `test_airflow_dag`
- W5: mapping generator + ETL Studio + community E2E

### Enterprise
- W6: `test_ingress_queue`, `test_checkpoint_store`, `test_enterprise_queue`
- W7: `test_pg_copy` + worker pool + throughput smoke
- W8: `test_dlq_policy`, delivery fallback, multi-lane, enterprise E2E

## Fail Fast Kuralı
- Bir wave'in gate testleri geçmeden sonraki wave açılmaz.
