# Functional Requirement Table

| FR | Scope | Gereksinim | Öncelik | Agent Notu |
|---|---|---|---|---|
| FR01 | C | YAML tabanlı konfigürasyon ile ETL job oluşturma ve çalıştırma | P0 | Tüm config üretiminin temeli |
| FR02 | C | PostgreSQL, MSSQL, Oracle arasında çift yönlü veri aktarımı | P0 | Dialect sınıfı gerekir |
| FR03 | C | Server-side cursor ile sabit bellek kullanımlı veri çekme | P0 | `fetchmany(10000)` zorunlu |
| FR04 | C | Kaynak metadata'dan otomatik DDL üretimi | P0 | Auto-DDL Generator |
| FR05 | E | Binary streaming: PostgreSQL `COPY FROM STDIN` | P0 | Enterprise only |
| FR06 | C | Apache Airflow `FFEngineOperator` entegrasyonu | P0 | Her iki versiyonda ortak |
| FR07 | C | Partitioning desteği (5+ mod) | P1 | `auto/percentile/hash_mod/explicit/distinct/full_scan` |
| FR08 | C | Binding parametreleri ile dinamik WHERE | P1 | `source/target/literal/airflow_var` |
| FR09 | C | Backpressure yönetimi | P1 | Community: generator, Enterprise: queue |
| FR10 | C | Auto-DAG Generator | P1 | YAML → Airflow DAG |
| FR11 | C | ETL Studio UI | P1 | Admin > ETL Studio |
| FR12 | E | Checkpoint & Resume (PK/Offset bazlı) | P0 | Enterprise only |
| FR13 | E | Dead Letter Queue (DLQ) | P0 | Enterprise only |
| FR14 | C | Dry-Run modu | P2 | DDL + bağlantı testi; veri aktarımı yok |
| FR15 | C | XCom metrik aktarımı (`rows/duration/throughput`) | P1 | `FFEngineOperator.xcom_push()` |
| FR16 | E | Queue-centric hedef yazma: kontrollü mikro-batch commit | P0 | Enterprise only |
| FR17 | E | Multi-Lane Pipeline: paralel bağımsız akışlar | P1 | Enterprise only |
| FR18 | E | Adaptif Batch Policy (`row+byte+time+feedback`) | P1 | Enterprise only |
| FR19 | E | FFEnvelope: Ack/Nack + at-least-once + exactly-once (koşullu) | P1 | Guarantee Matrix gerekir |
| FR20 | E | Oracle AQ / TxEventQ source ve sink adapter | P2 | Enterprise gelecek faz |
| FR21 | E | PostgreSQL Queue Table (`SKIP LOCKED`) sink | P2 | Enterprise gelecek faz |
| FR22 | E | PostgreSQL `COPY TO STDOUT (FORMAT BINARY)` extraction | P0 | Enterprise only |
| FR23 | E | Asimetrik `reader_workers + writer_workers` | P0 | Enterprise only |
| FR24 | C | 3 fazlı DAG pattern | P0 | Ortak davranış |
| FR25 | C | `column_mapping_mode` (`source`, `mapping_file`) | P0 | Ortak |
| FR26 | C | `TriggerDagRunOperator` ile kademeli DAG | P1 | Level1 → Level2 |
| FR27 | C | `validate_config()` çalışma öncesi doğrulama | P1 | Ortak |
| FR28 | C | `source_type: script` + `load_method: script` | P1 | Hedef DB script yürütme |
| FR29 | C | `sql_file` parametresi ile harici `.sql` okuma | P1 | ETL Studio SQL ayrı dosyada |
| FR30 | C | Email notification | P1 | Türkçe HTML |
| FR31 | C | ETL Studio tablo keşfi + DAG güncelleme + tag + timeline | P1 | 50 tablo limiti |
| FR32 | C | Transformer `TABLE_RULES` | P2 | strip / parse_dates / to_numeric / empty_to_null / add_etl_date |
