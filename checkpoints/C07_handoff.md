# C07 Handoff — Airflow Entegrasyonu

## Status
✅ **COMPLETED** — 2026-03-27

---

## Summary

C07, FFEngine ETL pipeline'ını Airflow ortamında orkestre eden kompleks entegrasyon katmanını implementer etti. 3-fazlı iç orkestrasyon pattern'i ve otomatik DAG üreteci, Wave 4 tamamlanmıştır.

---

## Deliverables

### Tier 1: Core Operator
**File:** `src/ffengine/airflow/operator.py` (280+ lines)

```python
class FFEngineOperator:
    """3-fazlı pipeline orkestrasyon (iç)."""

    template_fields = ("config_path", "task_group_id", "source_conn_id", "target_conn_id")

    def execute(self, context):
        # 1. Config yükle, binding/mapping/partition çöz
        # 2. TargetWriter.prepare() (bir kez)
        # 3. Her partition: combine_where(), ETLManager.run_etl_task(skip_prepare=True)
        # 4. Sonuçları aggregate et + XCom push
```

**Key Functions:**
- `resolve_dialect(conn_type: str) → BaseDialect` — Airflow conn_type → dialect eşlemesi
- `combine_where(base, partition) → str` — WHERE AND kombinasyonu
- `aggregate_results(results: list) → ETLResult` — Partition sonuçlarını birleştirme
- `build_airflow_variable_context() → dict` — Airflow Variable proxy

### Tier 2: DAG Patterns
**File:** `src/ffengine/airflow/dag_patterns.py` (180+ lines)

```python
class XComKeys:
    TASK_CONFIG_RESOLVED = "task_config_resolved"
    PARTITION_SPECS = "partition_specs"
    ROWS_TRANSFERRED = "rows_transferred"
    DURATION_SECONDS = "duration_seconds"
    ROWS_PER_SECOND = "rows_per_second"

def build_task_group(dag, *, config_path, task_group_id, source_conn_id, target_conn_id):
    """3-fazlı Airflow TaskGroup pattern."""
    # plan_partitions → xcom: (task_config, specs)
    # prepare_target  → xcom pull config + prepare
    # run_partitions  → xcom pull specs + loop ETLManager
```

**Bağımlılık:** plan >> prepare >> run

### Tier 3: DAG Generator
**File:** `src/ffengine/airflow/dag_generator.py` (100+ lines)

```python
def generate_dags(config_dir, dag_prefix="ffengine", schedule=None, tags=None) → dict[str, DAG]:
    """YAML config dir → {dag_id: DAG} dict."""
    # *.yaml dosyaları tara
    # etl_tasks listesi oku
    # Her task → FFEngineOperator(task_id, dag)

def register_dags(config_dir, globals_dict, **kwargs):
    """DAG'ları globals() içine inject et."""
```

---

## C06 & C09 Deferred Items — Resolved

### 1. Base WHERE + Partition WHERE AND Kombinasyonu ✅

**C06 Problem:** Partition spec WHERE, base task WHERE'ini ezerdi.

**Solution:** Operator'da pre-combine:
```python
def combine_where(base_where, partition_where):
    if base_where and partition_where:
        return f"({base_where}) AND ({partition_where})"
    return base_where or partition_where or None
```

**Call Site:** FFEngineOperator.execute() line 290-295
```python
base_where = task_config.get("_resolved_where")
for spec in specs:
    effective["_resolved_where"] = combine_where(base_where, spec.get("where"))
    manager.run_etl_task(..., skip_prepare=True)
```

### 2. MappingResolver Entegrasyonu ✅

**C09 Problem:** ETLManager'a MappingResolver entegre edilmedi (breaking 10 test).

**Solution:** Operator plan fazında MappingResolver çağrı:
```python
mapping = MappingResolver().resolve(task_config, src_conn, src_dialect, tgt_dialect)
task_config["source_columns"] = mapping.source_columns
task_config["target_columns"] = mapping.target_columns
task_config["target_columns_meta"] = mapping.target_columns_meta
```

**Call Site:** FFEngineOperator.execute() line 248-256

### 3. TargetWriter.prepare() Tek Sefer ✅

**C06 Problem:** Partition loop'u hazırlanmasında, prepare() partitions_count × defa çağrılırdı.

**Solution A (Operator):** Prepare BEFORE partition loop
```python
writer.prepare(task_config)  # Bir kez
for spec in specs:
    manager.run_etl_task(..., skip_prepare=True)  # Hazırlama atlanır
```

**Solution B (DAG Pattern):** Ayrı prepare_target task
```python
plan_partitions >> prepare_target >> run_partitions
```

**Call Site:**
- FFEngineOperator line 271-272
- build_task_group() prepare_target task

---

## ETLManager Changes

**File:** `src/ffengine/core/etl_manager.py`

```python
def run_etl_task(
    self,
    ...,
    partition_spec: dict | None = None,
    skip_prepare: bool = False,  # ← NEW
) -> ETLResult:
    if not skip_prepare:
        writer.prepare(effective_config)
```

**Backward Compatible:** Default `skip_prepare=False` → existing callers unaffected.

---

## Test Coverage

### test_operator.py — 30 tests
- `TestResolveDialect` (7): postgres, postgresql, mssql, tds, oracle, unknown, case-insensitive
- `TestCombineWhere` (4): both, base-only, part-only, neither
- `TestAggregateResults` (4): empty, single, multiple, with-errors
- `TestFFEngineOperatorInit` (3): required params, defaults, template_fields
- `TestFFEngineOperatorExecute` (9): happy-path, multi-partition, prepare-once, skip-prepare, where-combo, mapping-integration, xcom-push, config-loader, adapter
- `TestFFEngineOperatorErrors` (3): bad-conn-type, config-error, where-preservation

### test_airflow_dag.py — 18 tests
- `TestXComKeys` (4): string checks + uniqueness
- `TestBuildTaskGroup` (6): returns TaskGroup, 3 tasks, task IDs, dependency order (×2), custom group_id
- `TestGenerateDags` (6): single-yaml, multi-task, empty-dir, invalid-yaml-skip, nonexistent-dir, custom prefix/tags
- `TestRegisterDags` (2): updates globals, dag-id format

### test_etl_manager.py — 3 regression tests
- `test_skip_prepare_true_skips_writer_prepare`
- `test_skip_prepare_false_calls_writer_prepare`
- `test_skip_prepare_default_is_false`

**Total:** 435 passing (384 existing + 51 new)

---

## Critical Implementation Notes

### Lazy Imports
Operator ve dag_patterns'daki imports lazy (execute() içinde), Airflow olmayan ortamlarda import error vermemek için:
```python
def execute(self, context):
    from ffengine.config.loader import ConfigLoader  # Lazy
    from ffengine.db.session import DBSession
    ...
```

### Airflow Variable Context
Operator BindingResolver'a `airflow_context` dict iletir:
```python
airflow_ctx = self._airflow_context or build_airflow_variable_context()
task_config = BindingResolver().resolve(task_config, airflow_ctx)
```

Fallback test mode (manual dict) desteklenir.

### XCom Serialization
TaskGroup pattern'de XCom push/pull JSON serializable türleri kullanır:
- `task_config_resolved: dict`
- `partition_specs: list[dict]`
- `rows_transferred: int`
- `duration_seconds: float`

ColumnInfo listesi dict'e dönüştürülür (JSON compat):
```python
task_config["target_columns_meta"] = [
    {"name": c.name, "data_type": c.data_type, ...}
    for c in mapping.target_columns_meta
]
```

### Partition WHERE Handling
Operator `partition_spec=None` ile ETLManager çağrır çünkü WHERE kombinasyonunu kendisi `_resolved_where`'e yazmıştır. ETLManager'ın mevcut partition_spec WHERE override davranışı preserved (geriye uyumluluk):
```python
# ETLManager line 56-57 (unchanged):
if partition_spec and partition_spec.get("where"):
    effective_config["_resolved_where"] = partition_spec["where"]

# Operator passes partition_spec=None, where already in effective["_resolved_where"]
manager.run_etl_task(..., partition_spec=None, skip_prepare=True)
```

---

## Gate Verification ✅

1. **plan → prepare → run sırası:** test_dependency_order_* ile doğrulandi
2. **XCom anahtarları:** TestXComKeys ve task xcom_push assertions
3. **WHERE kombinasyonu:** test_where_combination
4. **MappingResolver:** test_mapping_integration
5. **skip_prepare:** test_skip_prepare_used, 3 regression test
6. **Multi-partition:** test_happy_path_multi_partition (prepare 1× called)

---

## Known Limitations & Future Work

1. **Airflow 3.x Deprecation Warnings:** TaskGroup/PythonOperator imports deprecated. Try/except fallback implemented but warnings persist (not critical).
2. **No Dynamic Task Mapping in Operator:** Operator kendisi loop yapıyor. DAG pattern'de `expand()` mevcut (advanced use case).
3. **No Error Recovery DAG:** Hatalar exception'a neden oluyor; retry logic Airflow level'de yapılır.
4. **No C08 ETL Studio Integration:** C08 (UI) separate epic.

---

## Files Summary

| Path | Lines | Purpose |
|------|-------|---------|
| `src/ffengine/airflow/operator.py` | 280+ | FFEngineOperator + utilities |
| `src/ffengine/airflow/dag_patterns.py` | 180+ | XComKeys + 3-phase TaskGroup |
| `src/ffengine/airflow/dag_generator.py` | 100+ | YAML → DAG auto-generator |
| `src/ffengine/airflow/__init__.py` | 15 | Public API |
| `src/ffengine/core/etl_manager.py` | +5 | skip_prepare parameter |
| `tests/unit/test_operator.py` | 370+ | 30 operator tests |
| `tests/unit/test_airflow_dag.py` | 230+ | 18 DAG tests |
| `tests/unit/test_etl_manager.py` | +50 | 3 skip_prepare tests |

---

## Next Steps

- **C08 (ETL Studio UI):** Frontend dashboard & REST API
- **E01-E04 (Enterprise):** CEngine integration, advanced features
- **Operational:** Deploy to Airflow 3.1.6+, configure Airflow Connections

---

Generated: 2026-03-27 by C07 Epic Implementation
