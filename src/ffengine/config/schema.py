"""
C05 — Config sabitleri, whitelist'ler ve varsayılan değerler.

CONFIG_SCHEMA.md ile senkronize edilmiştir.
"""

VALID_SOURCE_TYPES: frozenset[str] = frozenset(
    {"table", "view", "sql", "csv", "json", "script"}
)

# F1.4/F1.5 — file source/target (transport ⟂ format).
# source_type ∈ {csv, json} = FILE source; json_mode "flat" only ("raw" → F1.4b).
FILE_SOURCE_TYPES: frozenset[str] = frozenset({"csv", "json"})
VALID_JSON_MODES: frozenset[str] = frozenset({"flat"})  # "raw" deferred (F1.4b)
VALID_TARGET_TYPES: frozenset[str] = frozenset({"db", "file"})

VALID_LOAD_METHODS: frozenset[str] = frozenset(
    {
        "create_if_not_exists_or_truncate",
        "append",
        "replace",
        "upsert",
        "drop_if_exists_and_create",
        "script",
    }
)

VALID_COLUMN_MAPPING_MODES: frozenset[str] = frozenset({"source", "mapping_file"})

VALID_EXTRACTION_METHODS: frozenset[str] = frozenset({"auto", "cursor", "copy_binary"})

VALID_PASSTHROUGH_FORMATS: frozenset[str] = frozenset({"binary", "text", "csv"})

# C06 — Partition mode whitelist
VALID_PARTITION_MODES: frozenset[str] = frozenset(
    {
        "auto_numeric",
        "auto_datetime",
        "percentile",
        "hash_mod",
        "distinct",
        "explicit",
    }
)

# F1.3 — Operasyonel bildirim (Community): desteklenen tetikleyiciler.
# F1.3c — deadline eklendi (Airflow 3.2+ DeadlineAlert; süre = notify_deadline_minutes).
# reconciliation/threshold bu dilimde YOK (Enterprise / sonraki dalga).
VALID_NOTIFY_TRIGGERS: frozenset[str] = frozenset({"failure", "success", "deadline"})

# Root seviyesinde zorunlu alanlar
REQUIRED_ROOT_FIELDS: tuple[str, ...] = ("source_db_var", "target_db_var", "flow_tasks")

# F6.0 — kök `engine:` bloğu (opsiyonel, additive).
#
#     engine:
#       preference: auto        # auto | standard | pipeline | spark
#
# Kanonik değer listesi **tek kaynakta** yaşar:
# ``ffengine.core.base_engine.CANONICAL_ENGINE_PREFERENCES``. Burada ikinci
# bir liste tanımlanmaz; yalnız blok adı/alanı ve varsayılan tutulur.
# Doğrulama ``ConfigValidator._check_engine`` içinde (lazy import ile).
ENGINE_BLOCK_FIELD: str = "engine"
ENGINE_PREFERENCE_FIELD: str = "preference"
ENGINE_SPARK_FIELD: str = "spark"
VALID_ENGINE_BLOCK_FIELDS: frozenset[str] = frozenset(
    {ENGINE_PREFERENCE_FIELD, ENGINE_SPARK_FIELD}
)
SPARK_SUBMIT_MODE_FIELD: str = "submit_mode"
SPARK_CONN_ID_FIELD: str = "conn_id"
VALID_ENGINE_SPARK_FIELDS: frozenset[str] = frozenset(
    {SPARK_SUBMIT_MODE_FIELD, SPARK_CONN_ID_FIELD}
)
#: F6.1'de SEVK EDİLEN submit modları (EX-D026).
VALID_SPARK_SUBMIT_MODES: frozenset[str] = frozenset({"k8s", "local"})
#: TANINAN ama gerekçeli REDDEDİLEN modlar (EX-D026). Sessizce "unsupported"
#: demek yerine ayrı tutuluyor: kullanıcı YARN yazdığında neden çalışmadığını
#: ve neyin beklendiğini öğrenmeli. Klasik YARN'da konteyner imajı yoktur, bu
#: yüzden imaja bake edilen driver script'i ve Python ortamı NodeManager'da
#: bulunmaz; ayrı bir `--archives`/`PYSPARK_PYTHON` dağıtım hattı gerekir.
DEFERRED_SPARK_SUBMIT_MODES: dict[str, str] = {
    "yarn": (
        "classic YARN has no container image, so the driver script and Python "
        "runtime this delivery bakes into the Spark image are absent on the "
        "NodeManager; shipping it would need a second distribution path "
        "(--archives/PYSPARK_PYTHON) and its own test surface"
    ),
}
VALID_SPARK_SOURCE_TYPES: frozenset[str] = frozenset(
    {"table", "view", "sql", "iceberg"}
)
#: Alan verilmezse kullanılan belirgin varsayılan (EX-D021: `auto` korunur).
DEFAULT_ENGINE_PREFERENCE: str = "auto"
#: Loader'ın task runtime dict'ine koyduğu private anahtar
#: (`_resolved_where` konvansiyonu).
ENGINE_PREFERENCE_KEY: str = "_engine_preference"
ENGINE_SPARK_KEY: str = "_engine_spark"

# Task seviyesinde zorunlu alanlar
REQUIRED_TASK_FIELDS: tuple[str, ...] = (
    "task_group_id",
    "source_schema",
    "target_schema",
    "target_table",
    "source_type",
    "load_method",
)

# Task varsayılan değerleri (CONFIG_SCHEMA.md §Performance / Runtime)
TASK_DEFAULTS: dict = {
    "batch_size": 10_000,
    "reader_workers": 3,
    # F2.1 — writer count (M). Default None = auto-from-target-capability
    # (bulk: PG COPY / Oracle direct-path force M=1). A concrete value is an
    # EXPLICIT developer override; the legacy default 5 must NOT be treated as an
    # explicit M (review §8; TAD A6.4#1 "writer_workers -> hedeften otomatik").
    # Full reader_workers/parallel_degree cleanup stays a separate task.
    "writer_workers": None,
    "pipe_queue_max": 8,
    # F2.1 — Native bulk API (Enterprise providers). Default OFF. Method is
    # EXPLICIT (no "auto" — INV-7 developer-controlled, TAD v26.5 A4.3): when
    # use_bulk_api is True a concrete bulk_api_method (e.g. "postgres_copy") must
    # be given. Valid methods are capability-driven from the bulk provider
    # registry (ffengine.pipeline.bulk_registry), NOT a static enum.
    "use_bulk_api": False,
    "bulk_api_method": None,
    "extraction_method": "auto",
    "passthrough_format": "binary",
    "passthrough_full": True,
    "column_mapping_mode": "source",
    "where": None,
    "sql_file": None,
    "inline_sql": None,
    "source_table": None,
    "source_columns": None,
    "target_columns": None,
    "target_columns_meta": None,
    "upsert_match_columns": None,
    "mapping_file": None,
    "partitioning": {
        "enabled": False,
        "mode": "auto_numeric",
        "parts": 4,
        "distinct_limit": 16,
        "column": None,
        "ranges": [],
    },
}
