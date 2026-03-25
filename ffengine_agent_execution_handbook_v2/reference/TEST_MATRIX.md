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

## Integration
- PG → PG
- PG → MSSQL
- PG → Oracle
- Mapping generator → YAML → DAG → run → verify tam akış
- Airflow 3-faz DAG order testi
- Community E2E
- Enterprise queue runtime E2E
- Enterprise bulk E2E
- Enterprise DLQ / retry / resume senaryosu

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
