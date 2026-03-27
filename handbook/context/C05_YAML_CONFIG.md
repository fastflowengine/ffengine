# CONTEXT: C05 — YAML Konfigürasyon & Doğrulama

## Amaç
YAML parse, normalize, validate ve binding resolve katmanını geliştir.

## Kritik Noktalar
- Varsayılan değerler deterministik uygulanmalı
- Hatalı config çalışma öncesi yakalanmalı
- `column_mapping_mode` ve `load_method` değerleri whitelist ile doğrulanmalı
