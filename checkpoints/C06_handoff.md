# C06 Handoff — Partition

**Epic:** C06 — Partition Planlama
**Wave:** 4
**Durum:** COMPLETED
**Tarih:** 2026-03-27

## Değişen Dosyalar

| Dosya | İşlem | Açıklama |
|---|---|---|
| `src/ffengine/errors/exceptions.py` | MODIFY | `PartitionError(FFEngineError)` eklendi |
| `src/ffengine/errors/__init__.py` | MODIFY | `PartitionError` export edildi |
| `src/ffengine/config/schema.py` | MODIFY | `VALID_PARTITION_MODES` frozenset + `TASK_DEFAULTS["partitioning"]` genişletildi (`column: None`, `ranges: []`) |
| `src/ffengine/config/validator.py` | MODIFY | `_check_partitioning()` kuralı eklendi (8. kural), `VALID_PARTITION_MODES` import |
| `src/ffengine/partition/__init__.py` | CREATE | `Partitioner` export |
| `src/ffengine/partition/partitioner.py` | CREATE | 6 strateji — `full_scan`, `explicit`, `auto_numeric`, `percentile`, `hash_mod`, `distinct` |
| `tests/unit/test_partitioner.py` | CREATE | 25 test — tüm stratejiler, hata senaryoları |
| `tests/unit/test_config_validator.py` | MODIFY | `TestPartitioningValidation` sınıfı (12 test) eklendi |
| `tests/unit/test_config_loader.py` | MODIFY | `column` ve `ranges` default kontrolleri eklendi |

## Tamamlanan Kabul Kriterleri

- **`partitioning.enabled=False` → `[{"part_id": 0, "where": None}]`** — DB sorgusu yok ✓
- **`full_scan` modu → tek spec, `where=None`** ✓
- **`explicit` modu → `ranges` listesinden spec'ler, boş list → `PartitionError`** ✓
- **`auto_numeric` → MIN/MAX sorgusu, eşit genişlikli aralıklar, son partition `<=`** ✓
- **`auto_numeric` boş tablo / tek değer → `full_scan` fallback** ✓
- **`percentile` → PERCENTILE_CONT; desteklenmiyorsa `auto_numeric` fallback** ✓
- **`hash_mod` → DB sorgusu yok, `MOD(col, n) = i` / MSSQL `%`** ✓
- **`distinct` → DISTINCT sorgu, IN grupları, boş tablo → fallback, string quote** ✓
- **`ConfigValidator` partitioning kuralları — mod whitelist, parts>=1, column, ranges** ✓
- **`"auto"` alias → `"auto_numeric"` normalize edilir** ✓
- **`TASK_DEFAULTS["partitioning"]` artık `column: None` ve `ranges: []` içeriyor** ✓
- **345/345 unit test PASSED** (C06: 25+14, C05 regresyon: 62, C04: 59, C03: 75, C02: 13, C01: 95) ✓

## Partition Stratejileri Özeti

| Mod | DB sorgusu? | Kolon gerekli? | Açıklama |
|---|---|---|---|
| `full_scan` / disabled | Hayır | Hayır | `[{"part_id": 0, "where": None}]` |
| `explicit` | Hayır | Hayır | `ranges` listesinden spec |
| `auto_numeric` | Evet (MIN/MAX) | Evet | `col >= lo AND col < hi` / son `<=` |
| `percentile` | Evet (PERCENTILE_CONT) | Evet | Hata → `auto_numeric` fallback |
| `hash_mod` | Hayır | Evet | `MOD(col, n) = i` veya MSSQL `%` |
| `distinct` | Evet (DISTINCT) | Evet | `col IN (v1, v2, ...)` grupları |

## `partition_spec` Kontratı

```python
{"part_id": int, "where": str | None}
```

- `where=None`: tam tablo tarama (WHERE yok)
- `where` bir SQL fragmentidir — `WHERE` anahtar kelimesi yok, noktalı virgül yok
- ETLManager.run_etl_task() `partition_spec["where"]`'i `effective_config["_resolved_where"]`'e yazar

## Açık Riskler

### 1. Base WHERE + Partition WHERE Çakışması (C07'ye ertelendi)
`PythonEngine.run()` task config'inde `where` alanı varsa VE `partitioning.enabled=True` ise, ETLManager şu an partition spec WHERE'ini `_resolved_where`'e yazar ve base task WHERE'ini ezer. **AND kombinasyonu** C07 Airflow Operator sorumluluğudur.

C07 operatörü şu akışı izlemeli:
1. `BindingResolver.resolve(task_config, context)` → `_resolved_where` set edilir
2. `Partitioner.plan(task_config, src_conn, src_dialect)` → spec listesi
3. Her spec için, base `_resolved_where` varsa AND ile birleştir:
   ```python
   combined_where = f"({base_where}) AND ({spec['where']})" if base_where and spec["where"] else (base_where or spec["where"])
   ```
4. `ETLManager.run_etl_task(..., partition_spec={"part_id": i, "where": combined_where})`

### 2. hash_mod + VARCHAR Kolon (C10'a ertelendi)
`MOD(varchar_col, n)` çoğu RDBMS'de çalışma zamanı hatası verir. C06 validator DB introspection yapmaz (yalnız config doğrular). C10 kapsamında `EngineError`'a wrap edilmeli.

### 3. distinct Strateji Ölçeklenebilirliği
Yüksek kardinaliteli kolonlar için `SELECT DISTINCT` pahalıdır. Kullanıcılar bu durumda `auto_numeric` tercih etmeli.

### 4. percentile Dialect Tespiti
`type(src_dialect).__name__` tabanlı dispatch: bilinmeyen dialect sessiz `auto_numeric` fallback'e düşer. Yeni dialect eklendiğinde `_query_percentiles` güncellenmeli.

## Sonraki Wave İçin Notlar

### C07 (Airflow Operator) — Wave 5 öncelikli
```
FFEngineOperator.execute(context):
  1. AirflowDBAdapter → src_session, tgt_session, src_dialect, tgt_dialect oluştur
  2. ConfigLoader().load(config_path, task_group_id) → task_config
  3. BindingResolver().resolve(task_config, airflow_context) → task_config (XCom binding)
  4. Partitioner().plan(task_config, src_session.conn, src_dialect) → specs
  5. TargetWriter.prepare(task_config)  ← bir kez çağrılır
  6. DAG Dynamic Task Mapping üzerinden:
     ETLManager.run_etl_task(src_session, tgt_session, src_dialect, tgt_dialect,
                              task_config, partition_spec=spec)
```

**Önemli:** `PythonEngine.run()` şu an `_src_session` vb. enjeksiyonu bekliyor ve olmadığında `ConfigError` fırlatıyor. C07 bu sarımayı yapacak.

### Import path'ler:
- `from ffengine.partition import Partitioner`
- `from ffengine.errors.exceptions import PartitionError`
- `from ffengine.config.schema import VALID_PARTITION_MODES`
