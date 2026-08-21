"""
C05 — Config sabitleri, whitelist'ler ve varsayılan değerler.

CONFIG_SCHEMA.md ile senkronize edilmiştir.
"""

VALID_SOURCE_TYPES: frozenset[str] = frozenset(
    {"table", "view", "sql", "file", "script", "iceberg", "parquet", "kafka"}
)

# F1.4/F1.5 — file source/target (transport ⟂ format).
# Kaynak ve hedef simetriktir: tasima `source_type: file` / `target_type: file`
# ile, format ayri bir alanla secilir (`source_file_format` / `target_file_format`).
FILE_SOURCE_TYPE = "file"
VALID_SOURCE_FILE_FORMATS: frozenset[str] = frozenset({"csv", "json"})
VALID_JSON_MODES: frozenset[str] = frozenset({"flat"})  # "raw" deferred (F1.4b)
# F6.2 — `iceberg` additive olarak eklendi. Kendi başına bir motor seçimi
# DEĞİLDİR: `_check_engine` bu uç noktayı yalnız AÇIK `engine.preference: spark`
# ile kabul eder, Community'de edition kapısına takılır.
VALID_TARGET_TYPES: frozenset[str] = frozenset({"db", "file", "iceberg"})
# Hedef dosya formati (transport ⟂ format): `target_type: file` DEGISMEZ,
# format ayri bir alanla secilir. Verilmezse CSV -- geriye uyum.
VALID_TARGET_FILE_FORMATS: frozenset[str] = frozenset({"csv", "json"})

# Format okumanin TEK noktasi. Dogrudan `task["source_file_format"]` okumayin:
# bos string / None / buyuk harf normalizasyonu burada tek yerde yapilir, aksi
# halde her cagri noktasi kendi varsayilanini uydurur.
DEFAULT_FILE_FORMAT = "csv"


def source_file_format(task: dict) -> str:
    """Dosya kaynaginin formatini dondurur (normalize, varsayilan csv)."""
    return str(task.get("source_file_format") or DEFAULT_FILE_FORMAT).strip().lower()


def is_file_source(task: dict) -> bool:
    """Task bir DOSYA kaynagi mi? (`source_type: file`)"""
    return str(task.get("source_type") or "").strip().lower() == FILE_SOURCE_TYPE


VALID_LOAD_METHODS: frozenset[str] = frozenset(
    {
        "create_if_not_exists_or_truncate",
        "append",
        "replace",
        "upsert",
        "drop_if_exists_and_create",
        "script",
        "cdc_apply",
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

# F7.1 — DAG seviyesi retry blogu (opsiyonel; Airflow-native).
# Retry MOTORU yazilmaz: bu degerler Airflow'un `default_args.retries` /
# `retry_delay` alanlarina cevrilir (ARCH-09). Blok yoksa DAG'a `default_args`
# HIC emit edilmez -- legacy config'ler bayt-ayni kalir (ARCH-11).
RETRY_KEY: str = "retry"
VALID_RETRY_FIELDS: frozenset[str] = frozenset({"retries", "delay_seconds"})
#: Ust sinir kazara sonsuz donguye karsi: 10 deneme x 24 saat = pratik tavan.
MIN_RETRIES: int = 1
MAX_RETRIES: int = 10
MIN_RETRY_DELAY_SECONDS: int = 1
MAX_RETRY_DELAY_SECONDS: int = 86_400  # 24 saat
DEFAULT_RETRY_DELAY_SECONDS: int = 60


def _retry_int(raw, field: str, low: int, high: int) -> int:
    """`retry` blogundaki bir tamsayiyi araligi ile birlikte cozer (fail-loud).

    `bool` bilincli olarak reddedilir: Python'da `True == 1` oldugu icin
    `retries: true` sessizce "1 deneme"ye donusurdu. Float da reddedilir --
    2.5 deneme diye bir sey yok, sessiz kirpma INV-1 ihlali olurdu.
    """
    if isinstance(raw, (bool, float)):
        raise ValueError(f"retry.{field} must be an integer.")
    try:
        value = int(str(raw).strip()) if isinstance(raw, str) else int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"retry.{field} must be an integer.")
    if value < low or value > high:
        raise ValueError(
            f"retry.{field} must be between {low} and {high} (got {value})."
        )
    return value


def normalize_retry(raw) -> dict | None:
    """F7.1 — DAG seviyesi retry politikasini normalize eder.

    Bos/tanimsiz veya ``retries < 1`` -> **None** (retry kapali). Bu durumda
    cagiran taraf blogu config'e HIC yazmaz; boylece retry kullanmayan legacy
    config'lerin YAML'i bayt-ayni kalir ve DAG'a `default_args` emit edilmez
    (ARCH-11 byte-stability).

    Retry MOTORU yazilmaz: cikti Airflow'un `default_args.retries` /
    `retry_delay` alanlarina cevrilir (ARCH-09, bkz. ``airflow/retry.py``).

    Bu fonksiyon bilincli olarak **schema** icinde yasar: DAG-parse yolundaki
    ``airflow/retry.py`` bunu cagirir ve `ui.studio_service` gibi agir bir UI
    modulunu o yola sokmak import maliyetini ~3.3 sn'ye cikariyordu.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("retry must be an object.")
    if not raw:
        return None

    unknown = sorted(set(raw) - VALID_RETRY_FIELDS)
    if unknown:
        raise ValueError(
            f"retry: unknown field(s) {unknown}. "
            f"Valid fields: {sorted(VALID_RETRY_FIELDS)}."
        )

    retries_raw = raw.get("retries")
    if retries_raw in (None, ""):
        return None
    # Kapali (0 veya negatif) -> omit. Ust sinir yine de fail-loud dogrulanir,
    # aksi halde `retries: 999` sessizce yok sayilirdi.
    retries = _retry_int(retries_raw, "retries", -(2**31), MAX_RETRIES)
    if retries < MIN_RETRIES:
        return None

    delay_raw = raw.get("delay_seconds")
    delay = (
        DEFAULT_RETRY_DELAY_SECONDS
        if delay_raw in (None, "")
        else _retry_int(
            delay_raw,
            "delay_seconds",
            MIN_RETRY_DELAY_SECONDS,
            MAX_RETRY_DELAY_SECONDS,
        )
    )
    # Default explicit yazilir: config'i okuyan kisi gecikmeyi gorebilmeli.
    return {"retries": retries, "delay_seconds": delay}


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
    {"table", "view", "sql", "iceberg", "parquet", "kafka"}
)
#: Yalniz Spark motorunda anlamli kaynak/hedef tipleri. Bunlar Standard/Pipeline
#: motorlarinda **calismaz**, bu yuzden acik `engine.preference: spark` sart --
#: aksi halde config dogrulamayi gecer ve hata calisma anina kayardi (INV-1).
#: `parquet` F6.2'de eklendi (EX-D033): dosya kaynagi ama Spark yolunda okunur.
SPARK_ONLY_ENDPOINT_TYPES: frozenset[str] = frozenset({"iceberg", "parquet"})
# F6.2 — Iceberg uç noktası (EX-D030).
#
# `catalog_type` config'de **zorunlu ve sırsızdır**; `auto` yoktur. Bağlantı
# detayı (uri, warehouse) ve kimlik **yalnız Airflow Connection**'da yaşar —
# validator Connection okumaz (DAG-parse'ta ağ/DB erişimi yok), bu yüzden tip
# seçimi burada kalır ve reddi DAG-parse'ta doğar.
ICEBERG_TYPE: str = "iceberg"
ICEBERG_CATALOG_TYPE_FIELD: str = "catalog_type"
ICEBERG_PUBLISH_MODE_FIELD: str = "publish_mode"
VALID_ICEBERG_CATALOG_TYPES: frozenset[str] = frozenset({"jdbc", "hive", "rest"})
#: TANINAN ama gerekçeli REDDEDİLEN katalog tipleri. `DEFERRED_SPARK_SUBMIT_MODES`
#: ile aynı desen: "geçersiz tip" mesajı kullanıcıya NEDENİNİ söylemez.
REJECTED_ICEBERG_CATALOG_TYPES: dict[str, str] = {
    "glue": (
        "the Glue catalog is AWS-only and needs outbound internet, which "
        "violates this delivery's air-gapped guarantee; the image also ships "
        "no AWS SDK (B12). Use 'jdbc' (the reference: the catalog store is "
        "your existing RDBMS), 'hive' or 'rest'"
    ),
    "hadoop": (
        "the Hadoop catalog resolves the current table version from the "
        "warehouse directory, and object stores have no atomic rename, so two "
        "concurrent commits silently lose a snapshot. Use 'jdbc', 'hive' or "
        "'rest' -- all of them keep a transactional pointer"
    ),
}
#: `write.wap.enabled` tablo özelliğiyle çalışan staged yayım. Alan verilmezse
#: **direct**: bu bir "auto" değil, belgelenmiş varsayılandır (T-F6.1-6 ayrımı).
VALID_ICEBERG_PUBLISH_MODES: frozenset[str] = frozenset({"direct", "wap"})
DEFAULT_ICEBERG_PUBLISH_MODE: str = "direct"
#: `VALID_LOAD_METHODS` **genişletilmez**; Iceberg bağlamında koşullu reddedilen
#: üç değer ve her birinin doğru alternatifi.
REJECTED_ICEBERG_LOAD_METHODS: dict[str, str] = {
    "create_if_not_exists_or_truncate": (
        "truncate + insert is two commits on an Iceberg table, so readers see "
        "an EMPTY table between them. Use load_method='replace': "
        "overwritePartitions() lands the same result in a single atomic commit"
    ),
    "drop_if_exists_and_create": (
        "dropping the table destroys its snapshot history, so time-travel and "
        "rollback -- the audit trail this delivery relies on -- are gone. Use "
        "load_method='replace', which rewrites the data and keeps the history"
    ),
    "script": (
        "load_method='script' runs arbitrary SQL through the writer path, "
        "which bypasses the Iceberg commit and the K1 reconciliation built on "
        "its snapshot summary. Use task_type='script_run' for standalone SQL"
    ),
}
# F6.3 — CDC uç noktası (EX-D036). Debezium/Connect deployment-owned;
# FFEngine hazır CDC topic'lerini okur. Teslim garantisi sözleşmesi:
# at-least-once consumption + transactionally idempotent / effectively-once
# target effects — uçtan uca "exactly-once" İDDİA EDİLMEZ.
#
# `kafka` kendi başına motor seçimi DEĞİLDİR:
#   target_type: db      → Track A (sıralı transactional apply, Spark'sız)
#   target_type: iceberg → Track B/F6.3b (açık `engine.preference: spark` —
#                          `iceberg` hedefi SPARK_ONLY üzerinden zaten zorlar)
# Broker adresi/kimliği YALNIZ Airflow Connection'da yaşar; validator
# Connection okumaz (DAG-parse'ta ağ erişimi yok — Iceberg emsali).
KAFKA_TYPE: str = "kafka"
CDC_APPLY_LOAD_METHOD: str = "cdc_apply"
KAFKA_TOPIC_FIELD: str = "kafka_topic"
CDC_START_POLICY_FIELD: str = "cdc_start_policy"
CDC_START_OFFSETS_FIELD: str = "cdc_start_offsets"
CDC_MAX_BATCH_RECORDS_FIELD: str = "max_batch_records"
#: Başlangıç politikası AÇIK seçimdir; sessiz default YOK (EX-D036).
#: `earliest` = partition'ın LOW WATERMARK'ı (sıfır değil — retention sonrası
#: gerçek alt sınır) · `latest` = aktivasyon anındaki high watermark ·
#: `explicit` = tüm partition'lar için offset haritası zorunlu.
VALID_CDC_START_POLICIES: frozenset[str] = frozenset(
    {"earliest", "latest", "explicit"}
)
#: `max_batch_records` verilmezse kullanılan BELGELENMİŞ toplam bütçe
#: (partition başına değil — bounded allocator toplamı water-fill dağıtır;
#: `sum(ending-starting) <= bütçe` invariant'ı).
DEFAULT_CDC_MAX_BATCH_RECORDS: int = 100_000

#: Config'de sır aranırken kullanılan anahtar parçaları (INV-5). Değer değil
#: ANAHTAR adına bakılır: değeri incelemek sırrı log'a taşıma riski doğurur.
SECRET_FIELD_HINTS: tuple[str, ...] = (
    "password",
    "secret",
    "token",
    "credential",
    "access_key",
    "private_key",
    "passphrase",
)

# F4.3E — sessiz garanti düşürme yasağı (kök alan, opsiyonel, additive).
#
#     require_reconciliation: true   # varsayılan; false = bilinçli vazgeçiş
#
# `counter_source = unavailable` bildiren bir motorla `true` birleşirse task
# BAŞINDA, kaynağa hiç bağlanılmadan fail-loud olunur (T-F4.3E-10). Tip
# kontrolü `ConfigValidator._check_require_reconciliation` içindedir.
REQUIRE_RECONCILIATION_FIELD: str = "require_reconciliation"
#: Loader'ın task runtime dict'ine koyduğu private anahtar
#: (`_engine_preference` konvansiyonu).
REQUIRE_RECONCILIATION_KEY: str = "_require_reconciliation"
DEFAULT_REQUIRE_RECONCILIATION: bool = True

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
    # F6.2 — Iceberg uç noktası. Varsayılanları None: "verilmedi" ile
    # "açıkça verildi" ayrımı korunur (T-F6.1-6). `publish_mode` yokken
    # DEFAULT_ICEBERG_PUBLISH_MODE uygulanır.
    "catalog_type": None,
    "publish_mode": None,
    # F6.3 — CDC uç noktası. Varsayılanlar None: "verilmedi" ile "açıkça
    # verildi" ayrımı korunur. `cdc_start_policy` kafka kaynağında ZORUNLUDUR
    # (sessiz default yok); `max_batch_records` yokken
    # DEFAULT_CDC_MAX_BATCH_RECORDS uygulanır (belgelenmiş varsayılan).
    "kafka_topic": None,
    "cdc_start_policy": None,
    "cdc_start_offsets": None,
    "max_batch_records": None,
    "partitioning": {
        "enabled": False,
        "mode": "auto_numeric",
        "parts": 4,
        "distinct_limit": 16,
        "column": None,
        "ranges": [],
    },
}
