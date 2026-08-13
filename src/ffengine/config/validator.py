"""
C05 - Config dogrulama kurallari.

ConfigValidator.validate(task) gecersiz config icin
ffengine.errors.exceptions.ConfigError veya ValidationError firlatir.
"""

import logging

from ffengine.errors.exceptions import ConfigError, ValidationError
from ffengine.config.schema import (
    DEFAULT_ENGINE_PREFERENCE,
    ENGINE_PREFERENCE_KEY,
    ENGINE_SPARK_KEY,
    REQUIRED_TASK_FIELDS,
    VALID_SOURCE_TYPES,
    VALID_LOAD_METHODS,
    VALID_COLUMN_MAPPING_MODES,
    VALID_EXTRACTION_METHODS,
    VALID_PARTITION_MODES,
    FILE_SOURCE_TYPES,
    VALID_JSON_MODES,
    VALID_TARGET_TYPES,
    VALID_ENGINE_SPARK_FIELDS,
    VALID_SPARK_SOURCE_TYPES,
    DEFERRED_SPARK_SUBMIT_MODES,
    VALID_SPARK_SUBMIT_MODES,
)

_log = logging.getLogger(__name__)


class ConfigValidator:
    """
    Task config dict'ini dogrular.

    Kontroller sirasiyla:
      1. Zorunlu alan varligi
      2. source_type whitelist
      3. load_method whitelist
      4. column_mapping_mode whitelist
      5. extraction_method whitelist
      6. source_type=sql -> sql_file veya inline_sql zorunlu
      7. column_mapping_mode=mapping_file -> mapping_file yolu zorunlu
      8. partitioning kurallari (C06)
      9. passthrough_full=False -> source_columns zorunlu (C09)
    """

    def validate(self, task: dict) -> None:
        self._check_required(task)
        # F6.0 — engine bağlama kuralları source/target whitelist'inden ÖNCE
        # çalışır: `iceberg` hedefinde "açık spark gerekir" mesajı, "geçersiz
        # target_type" mesajından daha aktörlenebilirdir. Mevcut kontrollerin
        # sırası değişmedi; bu YENİ bir kontroldür ve engine bloğu olmayan +
        # iceberg olmayan config'ler için no-op'tur.
        self._check_engine(task)
        self._check_source_type(task)
        self._check_target_type(task)
        self._check_load_method(task)
        self._check_upsert_config(task)
        self._check_column_mapping_mode(task)
        self._check_extraction_method(task)
        self._check_bulk_api(task)
        self._check_sql_source(task)
        self._check_mapping_file(task)
        self._check_partitioning(task)
        self._check_passthrough_config(task)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_required(self, task: dict) -> None:
        required_fields = list(REQUIRED_TASK_FIELDS)
        source_type = task.get("source_type")
        # sql and file sources carry no source_schema.
        if source_type == "sql" or source_type in FILE_SOURCE_TYPES:
            required_fields = [f for f in required_fields if f != "source_schema"]
        # A file target is a path, not a schema.table.
        if task.get("target_type") == "file":
            required_fields = [
                f
                for f in required_fields
                if f not in ("target_schema", "target_table")
            ]
        for field in required_fields:
            if task.get(field) is None:
                raise ConfigError(f"Zorunlu alan eksik: '{field}'")

    def _check_source_type(self, task: dict) -> None:
        value = task.get("source_type")
        if value not in VALID_SOURCE_TYPES:
            raise ValidationError(
                f"Gecersiz source_type: '{value}'. "
                f"Gecerli degerler: {sorted(VALID_SOURCE_TYPES)}"
            )

    def _check_load_method(self, task: dict) -> None:
        value = task.get("load_method")
        if value not in VALID_LOAD_METHODS:
            raise ValidationError(
                f"Gecersiz load_method: '{value}'. "
                f"Gecerli degerler: {sorted(VALID_LOAD_METHODS)}"
            )

    def _check_upsert_config(self, task: dict) -> None:
        raw_cols = task.get("upsert_match_columns")
        normalized: list[str] = []
        if isinstance(raw_cols, list):
            seen: set[str] = set()
            for raw in raw_cols:
                col = str(raw or "").strip()
                if not col or col in seen:
                    continue
                seen.add(col)
                normalized.append(col)
            task["upsert_match_columns"] = normalized or None
        elif raw_cols is not None:
            raise ValidationError(
                "upsert_match_columns listesi zorunludur (verildiginde)."
            )

        task_type = str(task.get("task_type") or "source_target").strip()
        if task_type != "source_target" and task.get("load_method") == "upsert":
            raise ValidationError(
                "load_method='upsert' sadece task_type='source_target' icin desteklenir."
            )
        if task_type != "source_target" and normalized:
            raise ValidationError(
                "upsert_match_columns sadece task_type='source_target' icin desteklenir."
            )

        if task.get("load_method") == "upsert" and not normalized:
            raise ValidationError(
                "load_method='upsert' icin upsert_match_columns zorunludur."
            )

    def _check_column_mapping_mode(self, task: dict) -> None:
        value = task.get("column_mapping_mode", "source")
        if value not in VALID_COLUMN_MAPPING_MODES:
            raise ValidationError(
                f"Gecersiz column_mapping_mode: '{value}'. "
                f"Gecerli degerler: {sorted(VALID_COLUMN_MAPPING_MODES)}"
            )

    def _check_extraction_method(self, task: dict) -> None:
        value = task.get("extraction_method", "auto")
        if value not in VALID_EXTRACTION_METHODS:
            raise ValidationError(
                f"Gecersiz extraction_method: '{value}'. "
                f"Gecerli degerler: {sorted(VALID_EXTRACTION_METHODS)}"
            )
        if value == "copy_binary":
            _log.warning(
                "extraction_method=copy_binary Community modunda etkin degildir; "
                "cursor modu kullanilacak."
            )

    def validate_engine_selection(self, task: dict) -> None:
        """F6.1 — yalnız motor seçim kurallarını doğrular (public giriş).

        `validate()` bunu config yolunda zaten çalıştırır. Ayrı bir giriş
        gerekiyor çünkü `FFEngineOperator(engine=...)` argümanı kök YAML'da
        olmayan bir tercih dayatabiliyor: o durumda knob kilidi, `submit_mode`
        zorunluluğu ve endpoint matrisi hiç değerlendirilmemiş olurdu
        (programatik DAG'lar için sessiz INV-1 deliği). Runtime preflight
        çözülen tercihi enjekte edip burayı yeniden koşar.
        """
        self._check_engine(task)

    def _check_engine(self, task: dict) -> None:
        """F6.0 — engine.preference config sözleşmesi (EX-D021).

        Yalnız **config** soruları burada: enum, legacy alias reddi, edition
        gate ve Iceberg bağlama kuralı. Provider kayıtlı mı / `is_available()`
        **runtime** sorusudur ve `BaseEngine.detect()`'e aittir — burada
        registry'ye dokunulmaz (DAG parse'ta entry-point yüklenmesin ve
        `EngineError` 422 ile gölgelenmesin).
        """
        preference_provided = ENGINE_PREFERENCE_KEY in task
        spark_options_provided = ENGINE_SPARK_KEY in task
        raw_pref = task.get(ENGINE_PREFERENCE_KEY)
        source_type = str(task.get("source_type") or "").strip().lower()
        target_type = str(task.get("target_type") or "").strip().lower()
        iceberg_endpoint = "iceberg" in (source_type, target_type)

        # Sıcak yol: her DAG-parse'ta çalışır → engine bloğu yoksa ve Iceberg
        # uç yoksa sıfır import/iş (bkz. _check_bulk_api deseni).
        if not preference_provided and not spark_options_provided and not iceberg_endpoint:
            return

        from ffengine.core.base_engine import normalize_engine_preference
        from ffengine.core.edition import is_enterprise_enabled

        if not preference_provided:
            preference = DEFAULT_ENGINE_PREFERENCE
        else:
            if not isinstance(raw_pref, str) or not raw_pref.strip():
                raise ValidationError(
                    "engine.preference must be a non-empty string; "
                    f"received type {type(raw_pref).__name__}."
                )
            try:
                preference = normalize_engine_preference(
                    raw_pref, allow_legacy_aliases=False
                )
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc

        if preference in ("pipeline", "spark") and not is_enterprise_enabled():
            raise ValidationError(
                f"engine.preference='{preference}' Enterprise gerektirir "
                "(FFENGINE_EDITION=enterprise). Community 'standard' veya "
                "'auto' kullanir."
            )

        self._check_spark_options(task, preference)

        if iceberg_endpoint and preference != "spark":
            raise ValidationError(
                "Iceberg kaynak/hedef yalniz ACIK engine.preference: spark ile "
                f"kullanilir; verilen: '{preference}'. 'auto' Spark'i asla "
                "secmez (yalniz 'pipeline' provider'ini arar) -> sessizce "
                "yanlis motorda calisma riski fail-loud kapatildi."
            )

    def _check_spark_options(self, task: dict, preference: str) -> None:
        options_present = ENGINE_SPARK_KEY in task
        if preference != "spark":
            if options_present:
                raise ValidationError(
                    "engine.spark is valid only with engine.preference='spark'."
                )
            return
        if not options_present:
            raise ValidationError(
                "engine.preference='spark' requires the nested engine.spark block."
            )
        options = task.get(ENGINE_SPARK_KEY)
        if not isinstance(options, dict):
            raise ValidationError("engine.spark must be a mapping.")
        unknown = sorted(set(options) - VALID_ENGINE_SPARK_FIELDS)
        if unknown:
            raise ValidationError(f"Unknown engine.spark field(s): {unknown}.")
        self._check_spark_submit(options)
        self._check_spark_knobs(task)
        self._check_spark_endpoints(task)

    def _check_spark_submit(self, options: dict) -> None:
        raw_mode = options.get("submit_mode")
        if not isinstance(raw_mode, str) or not raw_mode.strip():
            raise ValidationError("engine.spark.submit_mode is required; auto is not supported.")
        mode = raw_mode.strip().lower()
        # Ertelenmis modlar TANINIR ve gerekcesiyle reddedilir (EX-D026):
        # "gecersiz mod" mesaji kullaniciya nedenini soylemez.
        deferred_reason = DEFERRED_SPARK_SUBMIT_MODES.get(mode)
        if deferred_reason is not None:
            raise ValidationError(
                f"engine.spark.submit_mode='{mode}' is recognised but deliberately "
                f"not shipped in this release: {deferred_reason}. "
                f"Supported modes: {sorted(VALID_SPARK_SUBMIT_MODES)}."
            )
        if mode not in VALID_SPARK_SUBMIT_MODES:
            raise ValidationError(
                f"Unsupported engine.spark.submit_mode='{raw_mode}'. "
                f"Valid modes: {sorted(VALID_SPARK_SUBMIT_MODES)}."
            )
        raw_conn = options.get("conn_id")
        if mode == "k8s" and (not isinstance(raw_conn, str) or not raw_conn.strip()):
            raise ValidationError(f"engine.spark.conn_id is required for {mode} submit.")
        # Normalize edilmis degeri geri yaz: motor tarafi tam esleme ariyor.
        # Aksi halde 'K8s' dogrulamayi gecer, submit'te "Unsupported mode" alir.
        options["submit_mode"] = mode
        if isinstance(raw_conn, str):
            options["conn_id"] = raw_conn.strip()

    def _check_spark_knobs(self, task: dict) -> None:
        locked = []
        if task.get("writer_workers") is not None:
            locked.append("writer_workers")
        if task.get("use_bulk_api") is True:
            locked.append("use_bulk_api")
        part = task.get("partitioning")
        if isinstance(part, dict) and part.get("enabled") is True:
            locked.append("partitioning.enabled")
        # Deger-bazli: diger uc knob gibi. Anahtar varligina bakmak
        # `parallel_degree: null` gibi ACIK BIR VARSAYILANI reddederdi ve
        # T-F6.1-6'nin yasakladigi "default != explicit" ayrimini bozardi.
        if task.get("parallel_degree") is not None:
            locked.append("parallel_degree")
        if locked:
            raise ValidationError(
                "SparkEngine owns its executor model; these Pipeline knobs are "
                f"not supported: {locked}."
            )

    def _check_spark_endpoints(self, task: dict) -> None:
        target_type = str(task.get("target_type") or "db").strip().lower()
        source_type = str(task.get("source_type") or "").strip().lower()
        if target_type != "iceberg":
            raise ValidationError(
                "SparkEngine target must be Iceberg so authoritative write "
                "reconciliation remains available."
            )
        if source_type not in VALID_SPARK_SOURCE_TYPES:
            raise ValidationError(
                "Unsupported Spark source for this delivery: "
                f"'{source_type}'. Valid sources: {sorted(VALID_SPARK_SOURCE_TYPES)}."
            )

    def _check_bulk_api(self, task: dict) -> None:
        """F2.1 — native bulk API: Enterprise + developer-controlled (TAD v26.5 A4.3).

        Off by default. When on: the method is EXPLICIT (no 'auto'), the edition
        must be Enterprise, and the method must be a *registered* provider method
        (capability-driven, not a static enum). Unsupported -> fail-loud; never a
        silent fallback to executemany.
        """
        use_bulk = bool(task.get("use_bulk_api", False))
        method = str(task.get("bulk_api_method") or "").strip()

        if not use_bulk:
            if method:
                raise ValidationError(
                    "bulk_api_method verildi ama use_bulk_api=False. "
                    "Bulk API acmak icin use_bulk_api=True yapin ya da "
                    "bulk_api_method'i bos birakin."
                )
            return

        # Imported here (not at function top) so the common off-path — hit on
        # every DAG-parse validate() — does zero import/registry work.
        from ffengine.core.edition import is_enterprise_enabled
        from ffengine.pipeline.bulk_registry import registered_methods

        if not is_enterprise_enabled():
            raise ValidationError(
                "Native bulk API Enterprise gerektirir "
                "(FFENGINE_EDITION=enterprise). Community executemany kullanir."
            )

        available = sorted(registered_methods())
        listing = available or "(hicbir bulk provider kurulu degil)"
        if not method:
            raise ValidationError(
                "use_bulk_api=True icin bulk_api_method zorunludur "
                f"(explicit; 'auto' yok). Kayitli yontemler: {listing}."
            )
        if method.lower() not in registered_methods():
            raise ValidationError(
                f"Desteklenmeyen bulk_api_method: '{method}'. "
                f"Kayitli yontemler: {listing}. "
                "Sessizce executemany'e dusulmez (fail-loud)."
            )

    def _check_sql_source(self, task: dict) -> None:
        source_type = task.get("source_type")

        if source_type == "sql":
            has_sql = task.get("sql_file") or task.get("inline_sql")
            if not has_sql:
                raise ValidationError(
                    "source_type='sql' icin 'sql_file' veya 'inline_sql' zorunludur."
                )
            return

        if source_type in FILE_SOURCE_TYPES:
            self._check_file_source(task)
            return

        source_table = str(task.get("source_table") or "").strip()
        if not source_table:
            raise ValidationError(
                "source_type='table|view|script' icin 'source_table' zorunludur."
            )

    def _check_file_source(self, task: dict) -> None:
        """F1.4/F1.5 — csv/json file source: file_path + explicit mapping."""
        file_path = str(task.get("file_path") or "").strip()
        if not file_path:
            raise ValidationError(
                "source_type='csv|json' icin 'file_path' zorunludur "
                "('source_table' degil)."
            )
        if task.get("column_mapping_mode") != "mapping_file":
            raise ValidationError(
                "Dosya kaynagi (csv/json) explicit mapping gerektirir: "
                "column_mapping_mode='mapping_file' olmali (tip cikarimi yok)."
            )
        if task.get("source_type") == "json":
            json_mode = str(task.get("json_mode") or "flat").strip().lower()
            if json_mode not in VALID_JSON_MODES:
                raise ValidationError(
                    f"Gecersiz json_mode: '{json_mode}'. "
                    f"Desteklenen: {sorted(VALID_JSON_MODES)} "
                    "('raw' bu surumde yok — F1.4b)."
                )

    def _check_target_type(self, task: dict) -> None:
        """F1.5 — target_type db|file; file target needs a path."""
        target_type = task.get("target_type", "db")
        if target_type not in VALID_TARGET_TYPES:
            raise ValidationError(
                f"Gecersiz target_type: '{target_type}'. "
                f"Gecerli degerler: {sorted(VALID_TARGET_TYPES)}"
            )
        if target_type == "file":
            if not str(task.get("target_file_path") or "").strip():
                raise ValidationError(
                    "target_type='file' icin 'target_file_path' zorunludur."
                )

    def _check_mapping_file(self, task: dict) -> None:
        if task.get("column_mapping_mode") == "mapping_file":
            if not task.get("mapping_file"):
                raise ValidationError(
                    "column_mapping_mode='mapping_file' icin 'mapping_file' yolu zorunludur."
                )

    def _check_partitioning(self, task: dict) -> None:
        part = task.get("partitioning")
        if not isinstance(part, dict) or not part.get("enabled"):
            return

        # File endpoints stream as a single writer (M=1) → no partitioning.
        if (
            task.get("source_type") in FILE_SOURCE_TYPES
            or task.get("target_type") == "file"
        ):
            raise ValidationError(
                "Dosya kaynagi/hedefi partitioning desteklemez (tek akis, M=1)."
            )

        mode = part.get("mode", "auto_numeric")
        if mode not in VALID_PARTITION_MODES:
            raise ValidationError(
                f"Gecersiz partitioning.mode: '{mode}'. "
                f"Gecerli degerler: {sorted(VALID_PARTITION_MODES)}"
            )

        parts = part.get("parts", 4)
        if not isinstance(parts, int) or parts < 1:
            raise ValidationError(f"partitioning.parts >= 1 olmali, su an: {parts!r}")

        column_modes = {
            "auto_numeric",
            "auto_datetime",
            "percentile",
            "hash_mod",
            "distinct",
        }
        if mode in column_modes and not part.get("column"):
            raise ValidationError(
                f"partitioning.mode='{mode}' icin 'partitioning.column' zorunludur."
            )

        distinct_limit = part.get("distinct_limit", 16)
        if not isinstance(distinct_limit, int) or distinct_limit < 1:
            raise ValidationError(
                f"partitioning.distinct_limit >= 1 olmali, su an: {distinct_limit!r}"
            )
        part["distinct_limit"] = distinct_limit

        if mode == "explicit":
            ranges = part.get("ranges")
            if not isinstance(ranges, list) or not ranges:
                raise ValidationError(
                    "partitioning.mode='explicit' icin 'partitioning.ranges' listesi bos olamaz."
                )
            cleaned_ranges: list[str] = []
            for clause in ranges:
                if not isinstance(clause, str) or not clause.strip():
                    raise ValidationError(
                        "partitioning.mode='explicit' icin 'partitioning.ranges' yalnizca dolu string ifadeler icermelidir."
                    )
                cleaned_ranges.append(clause.strip())
            part["ranges"] = cleaned_ranges

    def _check_passthrough_config(self, task: dict) -> None:
        if task.get("column_mapping_mode", "source") != "source":
            return
        if task.get("passthrough_full", True) is False and not task.get(
            "source_columns"
        ):
            raise ValidationError(
                "passthrough_full=False iken 'source_columns' listesi zorunludur."
            )
