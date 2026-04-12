"""
C05 — Config sabitleri, whitelist'ler ve varsayılan değerler.

CONFIG_SCHEMA.md ile senkronize edilmiştir.
"""

VALID_SOURCE_TYPES: frozenset[str] = frozenset({"table", "view", "sql", "csv", "script"})

VALID_LOAD_METHODS: frozenset[str] = frozenset({
    "create_if_not_exists_or_truncate",
    "append",
    "replace",
    "upsert",
    "delete_from_table",
    "drop_if_exists_and_create",
    "script",
})

VALID_COLUMN_MAPPING_MODES: frozenset[str] = frozenset({"source", "mapping_file"})

VALID_EXTRACTION_METHODS: frozenset[str] = frozenset({"auto", "cursor", "copy_binary"})

VALID_PASSTHROUGH_FORMATS: frozenset[str] = frozenset({"binary", "text", "csv"})

# C06 — Partition mod whitelist ("auto" is a legacy alias for "auto_numeric")
VALID_PARTITION_MODES: frozenset[str] = frozenset({
    "auto",
    "auto_numeric",
    "percentile",
    "hash_mod",
    "distinct",
    "explicit",
})

# Root seviyesinde zorunlu alanlar
REQUIRED_ROOT_FIELDS: tuple[str, ...] = ("source_db_var", "target_db_var", "flow_tasks")

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
    "writer_workers": 5,
    "pipe_queue_max": 8,
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
    "mapping_file": None,
    "partitioning": {
        "enabled": False,
        "mode": "auto",
        "parts": 4,
        "distinct_limit": 16,
        "column": None,
        "ranges": [],
    },
}
