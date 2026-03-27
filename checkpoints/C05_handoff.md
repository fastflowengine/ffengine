# C05 Handoff — YAML Config

**Epic:** C05 — YAML Konfigürasyon & Doğrulama
**Wave:** 3
**Durum:** COMPLETED
**Tarih:** 2026-03-26

## Değişen Dosyalar

| Dosya | İşlem | Açıklama |
|---|---|---|
| `src/ffengine/errors/__init__.py` | CREATE | FFEngineError hiyerarşisi export |
| `src/ffengine/errors/exceptions.py` | CREATE | 9 exception sınıfı — C10'a kadar stub |
| `src/ffengine/config/__init__.py` | CREATE | ConfigLoader, ConfigValidator, BindingResolver export |
| `src/ffengine/config/schema.py` | CREATE | Whitelist frozenset'ler + TASK_DEFAULTS + REQUIRED_FIELDS |
| `src/ffengine/config/validator.py` | CREATE | 7 doğrulama kuralı — ConfigError / ValidationError fırlatır |
| `src/ffengine/config/loader.py` | CREATE | YAML yükle → task bul → default uygula → validate → döndür |
| `src/ffengine/config/binding_resolver.py` | CREATE | source/target/literal/airflow_var binding çözümleme |
| `tests/unit/test_config_validator.py` | CREATE | 35 test — whitelist, zorunlu alan, koşullu kurallar |
| `tests/unit/test_config_loader.py` | CREATE | 25 test — geçerli config, defaults, hata senaryoları |
| `src/ffengine/core/etl_manager.py` | MODIFY | PythonEngine._load_task() aktif — ConfigLoader bağlandı |
| `tests/unit/test_etl_manager.py` | MODIFY | test_python_engine_run_raises_before_c05 → ConfigError testi |

## Tamamlanan Kabul Kriterleri

- **Default değerler deterministik:** `TASK_DEFAULTS` her yüklemede `copy.deepcopy` ile uygulanır, orijinal mutate edilmez ✓
- **Geçersiz source_type/load_method → exception:** `ConfigValidator` whitelist kontrolü ✓
- **source_type=sql + sql_file=None → ValidationError** ✓
- **column_mapping_mode=mapping_file + path yok → ValidationError** ✓
- **Zorunlu alan eksik → ConfigError** ✓
- **YAML parse hatası → ConfigError** ✓
- **Dosya bulunamadı → ConfigError** ✓
- **PythonEngine.run() ConfigLoader üzerinden çalışıyor** ✓
- **302/302 unit test PASSED** (C05: 60, C04 regresyon: 59, C03: 75, C02: 13, C01: 95)

## Açık Riskler

- **Session/dialect enjeksiyonu C07'e ertelenmiş:** `PythonEngine.run()` şu an task_config içinde `_src_session`, `_tgt_session`, `_src_dialect`, `_tgt_dialect` key'leri bekliyor. Bu key'lerin caller tarafından enjekte edilmesi gerekiyor. C07 (Airflow Operator) `AirflowDBAdapter` üzerinden bu session'ları otomatik oluşturacak.
- **BindingResolver caller sorumluluğunda:** `ConfigLoader.load()` otomatik binding çözmez; çağıran `BindingResolver.resolve()` ile WHERE bağlamayı ayrıca yapmalı. C07 entegrasyonunda Airflow XCom context aktarımı planlanmalı.
- **errors/exceptions.py C10 stub'ı:** `handler.py`, retry semantics ve structured logging C10 kapsamında eklenecek. Mevcut exception sınıfları `EXCEPTION_MODEL.md` ile uyumludur.

## Sonraki Wave İçin Notlar

- **C07 (Airflow Operator):**
  - `FFEngineOperator` şu akışı izlemeli:
    1. `AirflowDBAdapter` ile `src_session`, `tgt_session` oluştur
    2. `ConfigLoader().load(config_path, task_group_id)` ile `task_config` yükle
    3. `BindingResolver().resolve(task_config, context={"airflow_var_KEY": value})` ile binding çöz
    4. `ETLManager.run_etl_task(src_session, tgt_session, ...)` çağır
  - `PythonEngine.run()` şu an session enjeksiyonunu caller'a bırakıyor; C07'de bu sarılacak.
- **C06 (Partition):**
  - `ConfigLoader().load()` dönen dict'teki `partitioning` bloğu `Partitioner` tarafından okunabilir.
  - `partition_spec` formatı: `{"part_id": int, "where": str | None}` — API_CONTRACTS.md ile uyumlu.
- **C10 (Error Handling):**
  - `src/ffengine/errors/exceptions.py` hazır; `handler.py` ve retry decoratorları eklenecek.
  - Raw DB exception'ların `EngineError`'a wrap edilmesi C10 kapsamında yapılacak.
- Import path'ler:
  - `from ffengine.config import ConfigLoader, ConfigValidator, BindingResolver`
  - `from ffengine.errors.exceptions import ConfigError, ValidationError`
