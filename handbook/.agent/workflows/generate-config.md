# /generate-config

FFEngine `config.yaml` üretim workflow'u.

## Girdi
- `source_db`
- `target_db`
- `source_type`
- `load_method`
- `scope`
- `task_group_id`
- `source/target schema-table`
- `partitioning ihtiyacı`
- `mapping ihtiyacı`

## Protokol
1. `reference/CONFIG_SCHEMA.md` oku.
2. Gerekli minimum alanları doldur.
3. Scope Community ise Enterprise-only alanları **yorum satırı** veya açıklama notu olarak bırak; aktif kullanma.
4. Scope Enterprise ise `reader_workers`, `writer_workers`, `pipe_queue_max`, `extraction_method`, `passthrough_format`, `passthrough_full` alanlarını kullanım senaryosuna göre değerlendir.
5. `load_method` ile delivery semantics uyumunu `reference/DELIVERY_GUARANTEE_MATRIX.md` üzerinden kontrol et.
6. `column_mapping_mode=mapping_file` ise mapping dosyası yolunu ekle.
7. `validate_config()` açısından zorunlu alanları ikinci kez kontrol et.

## Uyarılar
- Community için `extraction_method=copy_binary` aktif etme.
- `truncate+insert` seçildiğinde partial failure riskini not et.
- `upsert` seçildiğinde hedef PK/UNIQUE gereksinimini not et.
- LOB içeren kaynaklarda passthrough kararını açıklama notunda belirt.

## Çıktı
- Tek bir `config.yaml`
- Ardından kısa bir `Config Rationale` notu
