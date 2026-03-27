# Config Schema

## Root
```yaml
source_db_var: "source_conn_id"
target_db_var: "target_conn_id"
etl_tasks:
  - task_group_id: sample_task
    source_schema: public
    source_table: sample
    source_type: table
    target_schema: dwh_stg
    target_table: sample
    load_method: create_if_not_exists_or_truncate
    column_mapping_mode: source
    batch_size: 10000
    where: "status = 'A'"
    sql_file: null
    partitioning:
      enabled: false
      mode: auto
      parts: 4
    reader_workers: 3
    writer_workers: 5
    pipe_queue_max: 8
    extraction_method: auto
    passthrough_format: binary     # binary | text | csv
    passthrough_full: true
```

## Minimum Zorunlu Alanlar
- `source_db_var`
- `target_db_var`
- `etl_tasks[]`
- `task_group_id`
- `source_schema`
- `target_schema`
- `target_table`
- `source_type`
- `load_method`

## source_type
- `table`
- `view`
- `sql`
- `csv`
- `script`

## load_method
- `create_if_not_exists_or_truncate`
- `append`
- `replace`
- `upsert`
- `delete_from_table`
- `drop_if_exists_and_create`
- `truncate+insert`
- `script`

## column_mapping_mode
- `source`
- `mapping_file`

## Binding kaynakları
- `source`
- `target`
- `literal`
- `airflow_var`

## Partitioning
```yaml
partitioning:
  enabled: true
  column: id
  mode: auto  # auto|percentile|hash_mod|explicit|distinct|full_scan
  parts: 4
```

## Performance / Runtime Parametreleri
### `batch_size`
- Varsayılan: `10000`
- Community ve Enterprise fallback path için geçerlidir.

### `reader_workers`
- Tip: `int`
- Varsayılan: `3`
- Enterprise bulk / queue path için kullanılır.
- Community parse edebilir ama aktif paralel bulk reader olarak kullanmaz.

### `writer_workers`
- Tip: `int`
- Varsayılan: `5`
- Enterprise controlled writer sayısı.
- Community parse edebilir ama native writer pool açmaz.

### `pipe_queue_max`
- Tip: `int`
- Varsayılan: `8`
- Queue derinliği / backpressure sınırı.
- Community Streamer bunu throttle sinyali için okuyabilir; gerçek queue runtime Enterprise'dadır.

### `extraction_method`
- Tip: `enum`
- Varsayılan: `auto`
- Seçenekler:
  - `auto`
  - `cursor`
  - `copy_binary`
- Kural:
  - Community çalıştırma modunda etkin yol `cursor`
  - Enterprise'da `auto`, kaynağa göre `copy_binary` veya `cursor` seçebilir
  - `copy_binary` yalnızca PostgreSQL kaynak ve Enterprise path için uygundur

### `passthrough_format`
- Tip: `enum`
- Varsayılan: `binary`
- Seçenekler:
  - `binary` — ham binary blok; Enterprise bulk path ile uyumlu
  - `text`   — string dönüşümlü satır; debugging ve CSV kaynak için
  - `csv`    — CSV kaynaklı veri için
- Kural:
  - Community tipik olarak `binary` veya `text`
  - Enterprise bulk path `binary` bekler
  - `copy_binary` extraction metoduyla birlikte her zaman `binary` seçilmeli

### `passthrough_full`
- Tip: `bool`
- Varsayılan: `true`
- `true` → tüm kolonlar aktarılır, column_mapping_mode: source ile uyumludur
- `false` → yalnızca mapping dosyasında tanımlı kolonlar aktarılır

## Agent Validation Rules
1. Community config üretirken Enterprise-only alanları yorumla ama aktif bulk mode açma.
2. `column_mapping_mode=mapping_file` ise mapping yolu belirtilmeli.
3. `source_type=sql` ise `sql_file` veya inline SQL alanı bulunmalı.
4. `load_method=script` yalnızca hedef tarafta script çalıştırma için kullanılmalı.
5. `truncate+insert` ve `upsert` için delivery notu ayrıca yazılmalı.
