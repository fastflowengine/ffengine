# CONTEXT: C02 — Veritabanı Bağlantı Katmanı

## Amaç
Sürücü bağımsız DBSession ve Airflow connection adapter katmanını kur.

## Kapsam
- commit/rollback yönetimi
- cursor üretimi
- health check
- Airflow conn_id çözümleme

## Kabul Kriterleri
- Hata varsa rollback, yoksa commit
- PostgreSQL, MSSQL, Oracle için bağlantı parametre çözümü çalışır
