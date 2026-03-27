# CONTEXT: C10 — Hata Yönetimi & Delivery Guarantee

## Amaç
Community ve Enterprise hata modelini scope'a uygun şekilde uygula.

## Kural
- Community: chunk rollback + task retry
- Enterprise: ack/nack, checkpoint, DLQ, delivery policy

## C10 Tamamlanan Kapsam (Community)
- Exception hiyerarşisi `EXCEPTION_MODEL.md` ile hizalandı (`FFEngineError` + typed alt sınıflar).
- Merkezi hata normalizasyonu eklendi: `normalize_exception`, `http_status_for`, `error_payload`.
- ETLManager / TargetWriter / Operator / ETL Studio API katmanlarında typed exception ve tutarlı error mapping uygulandı.
- Operator retry telemetry (`try_number`, `max_tries`) ve `error_summary` xcom üretimi eklendi.
- Structured logging, `LOGGING_SCHEMA.md` zorunlu alanlarına hizalandı.

## Doğrulama Notu
- Wave 5 gate için mevcut test seti başarıyla çalıştırıldı:
  - `test_operator.py`, `test_airflow_dag.py`, `test_etl_studio_api.py`, `test_error_handling.py`
- `test_community_e2e.py` artefaktı bu repoda bulunmadığı için C11 kapsamında integration/release prep altında ele alınacaktır.
