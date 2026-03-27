# CONTEXT: C11 — Entegrasyon Testleri

## Amaç
Container tabanlı gerçek DB integration testlerini oluştur.

## Zorunlu Akışlar
- PG→PG
- PG→MSSQL
- PG→Oracle
- mapping_generator → config → DAG → run → verify

## Wave 6 Task Listesi (C11)
- C11_T01: Integration ortamı hazırlığı (`docker-compose.test.yml`, `.env`, servis health-check)
- C11_T02: `test_cross_db_etl.py` skip/marker aktivasyonu
- C11_T03: Zorunlu akışların çalıştırılması ve PASS kanıtları (PG->PG / PG->MSSQL / PG->Oracle)
- C11_T04: mapping_generator zinciri doğrulama testi
- C11_T05: release prep dokümantasyonu (`docs/community_quickstart.md`, README notları)
- C11_T06: checkpoint/handoff kapanışı ve durum güncellemeleri

Referans: `checkpoints/C11_checkpoint.yaml`
