# CONTEXT: C03 — Dialect Soyutlama Katmanı

## Amaç
PostgreSQL, MSSQL ve Oracle dialect implementasyonlarını ortak BaseDialect kontratı üzerinden sun.

## Kritik Noktalar
- TypeMapper kayıpsız olmalı
- DDL üretimi deterministik olmalı
- Metadata keşfi ve quoting kuralları sürücüye göre doğru uygulanmalı
