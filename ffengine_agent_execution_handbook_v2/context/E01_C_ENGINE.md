# CONTEXT: E01 — C Engine ve Build Sistemi

## Amaç
Enterprise tarafında `CEngine` iskeletini ve Python binding / binary yükleme modelini tanımla.

## Dahil
- `enterprise/engine.py`
- `BaseEngine` kontratına uygun `CEngine`
- `is_available()` davranışı
- build / load stratejisi
- fallback kuralları

## Hariç
- UI değişikliği
- Dialect değişikliği
- YAML format kırımı

## Zorunlu Davranışlar
- `BaseEngine.detect("auto")` C Engine mevcutsa Enterprise, yoksa Community seçer.
- `BaseEngine.detect("enterprise")` C Engine yoksa hata verir.
- `CEngine.run()` aynı `ETLResult` kontratını döndürür.
- Build sistemi shared library veya Python extension yükleyebilir; agent build yaklaşımını seçse bile interface değişmez.

## is_available() Kriterleri
- native lib bulunuyor mu?
- versiyon uyumlu mu?
- gerekli semboller yüklenebiliyor mu?
- smoke-call başarılıyor mu?

## Teslim Çıktısı
- engine wrapper
- availability check
- smoke unit tests
- fallback integration test
