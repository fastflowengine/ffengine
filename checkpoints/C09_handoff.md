# C09 Handoff — Mapping Tools

**Epic:** C09 — Mapping Tools
**Wave:** 4
**Durum:** COMPLETED
**Tarih:** 2026-03-27

## Değişen Dosyalar

| Dosya | İşlem | Açıklama |
|---|---|---|
| `src/ffengine/mapping/resolver.py` | CREATE | `MappingResult` dataclass + `MappingResolver` (source + mapping_file modları) + `_dialect_name()` yardımcısı |
| `src/ffengine/mapping/generator.py` | CREATE | `MappingGenerator.generate()` + `save()` |
| `src/ffengine/mapping/__init__.py` | CREATE | `MappingResolver`, `MappingResult`, `MappingGenerator` export |
| `src/ffengine/config/validator.py` | MODIFY | `_check_passthrough_config()` kuralı eklendi (9. kural) |
| `tests/unit/test_mapping_resolver.py` | CREATE | 22 test — `_dialect_name()`, source modu, mapping_file modu, dispatch |
| `tests/unit/test_mapping_generator.py` | CREATE | 12 test — generate yapısı, tür çevirisi, save, roundtrip |

## Tamamlanan Kabul Kriterleri

- **`MappingResult` dataclass** — `source_columns`, `target_columns`, `target_columns_meta: list[ColumnInfo]` ✓
- **`source` modu, `passthrough_full=True`** — tüm kolonlar TypeMapper ile çevrilir ✓
- **`source` modu, `passthrough_full=False`** — `source_columns` listesiyle filtrelenir; listede olmayan kolon → `MappingError` ✓
- **`source` modu, desteklenmeyen tip** — `UnsupportedTypeError` → `MappingError` sarma ✓
- **`source` modu, `get_table_schema` hatası** — `Exception` → `MappingError` sarma ✓
- **`mapping_file` modu, YAML yükleme** — `source_name`/`target_name`/`target_type`/`nullable` okunur ✓
- **`mapping_file` modu, `nullable` default `True`** — alan yoksa `True` kabul edilir ✓
- **`mapping_file` modu, eksik dosya** — `MappingError` fırlatır ✓
- **`mapping_file` modu, bozuk YAML** — `MappingError` fırlatır ✓
- **`mapping_file` modu, eksik/bilinmeyen versiyon** — `MappingError` fırlatır ✓
- **`column_mapping_mode` bilinmiyor** — `MappingError` fırlatır ✓
- **`_dialect_name()`** — `PostgresDialect` → `"postgres"`, `MSSQLDialect` → `"mssql"`, `OracleDialect` → `"oracle"` ✓
- **`MappingGenerator.generate()`** — şema okur, TypeMapper ile çevirir, YAML dict döndürür ✓
- **`MappingGenerator.save()`** — YAML dosyaya yazar; üst dizin yoksa `MappingError` ✓
- **generate → save → resolve roundtrip** — üretilen YAML `MappingResolver` tarafından doğru okunur ✓
- **`ConfigValidator._check_passthrough_config()`** — `passthrough_full=False` + `source_columns` yoksa `ValidationError` ✓
- **384/384 unit test PASSED** ✓

## Mimari Kararlar

### `_dialect_name()` Yardımcısı
```python
import re

def _dialect_name(dialect) -> str:
    raw = type(dialect).__name__.lower()
    return re.sub(r"dialect$", "", raw) or raw
```
`type(dialect).__name__` üzerinden çalışır; bu nedenle test yardımcılarında `MagicMock.__class__` değil, gerçek sınıf
örnekleri kullanılmalıdır:
```python
def _make_dialect(class_name: str, cols=None):
    class _D:
        pass
    _D.__name__ = class_name
    _D.get_table_schema = lambda self, *a, **kw: (cols or [])
    return _D()
```

### ETLManager Entegrasyonu (Ertelendi → C07)
`MappingResolver`, `ETLManager.run_etl_task()` içine entegre **edilmedi**. Mevcut testler `source_columns`/`target_columns`'ı fixture üzerinde doğrudan sağlıyor; entegrasyon bu testleri kırar. C07 Airflow Operator bu sarımı yapacak:
```python
mapping = MappingResolver().resolve(task_config, src_conn, src_dialect, tgt_dialect)
task_config["source_columns"] = mapping.source_columns
task_config["target_columns"] = mapping.target_columns
```

### Mapping YAML v1 Şeması
```yaml
version: "v1"
source_dialect: oracle        # isteğe bağlı
target_dialect: postgres      # isteğe bağlı
columns:
  - source_name: ORDER_ID
    target_name: order_id
    source_type: "NUMBER(10)"  # isteğe bağlı
    target_type: INTEGER
    nullable: false            # isteğe bağlı, default: true
```

## Import Path'ler

```python
from ffengine.mapping import MappingResolver, MappingResult, MappingGenerator
from ffengine.mapping.resolver import _dialect_name   # iç yardımcı
```

## Açık Riskler

### 1. ETLManager Entegrasyonu (C07'ye ertelendi)
`MappingResolver` doğrudan `ETLManager.run_etl_task()` içinde çağrılmıyor. C07 Airflow Operator bu entegrasyonu yapacak.

### 2. TypeMapper Kapsam Sınırı
Bilinmeyen kaynak tipler `UnsupportedTypeError` fırlatır. Yeni dialect veya özel tipler eklendiğinde `TypeMapper` güncellenmeli; aksi hâlde tüm mapping işlemleri `MappingError` ile durur.

## Wave 4 Tamamlandı

C04 + C06 + C09 tamamlanarak Wave 4 kapatıldı:
- **C04** (Core Engine): ETLManager, SourceReader, TargetWriter, Streamer, Transformer
- **C06** (Partition): 6 strateji, ConfigValidator entegrasyonu
- **C09** (Mapping Tools): MappingResolver, MappingGenerator, ConfigValidator passthrough kuralı

## Sonraki Wave İçin Notlar

### C07 (Airflow Operator) — Wave 5 öncelikli
```
FFEngineOperator.execute(context):
  1. AirflowDBAdapter → src_session, tgt_session, src_dialect, tgt_dialect
  2. ConfigLoader().load(config_path, task_group_id) → task_config
  3. BindingResolver().resolve(task_config, airflow_context)
  4. MappingResolver().resolve(task_config, src_conn, src_dialect, tgt_dialect)
     → task_config["source_columns"] = mapping.source_columns
     → task_config["target_columns"] = mapping.target_columns
  5. Partitioner().plan(task_config, src_conn, src_dialect) → specs
  6. TargetWriter.prepare(task_config)  ← bir kez
  7. Her spec için ETLManager.run_etl_task(...)
```
