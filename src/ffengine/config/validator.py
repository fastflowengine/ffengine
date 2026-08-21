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
    REQUIRE_RECONCILIATION_FIELD,
    REQUIRE_RECONCILIATION_KEY,
    REQUIRED_TASK_FIELDS,
    VALID_SOURCE_TYPES,
    VALID_LOAD_METHODS,
    VALID_COLUMN_MAPPING_MODES,
    VALID_EXTRACTION_METHODS,
    VALID_PARTITION_MODES,
    FILE_SOURCE_TYPE,
    VALID_SOURCE_FILE_FORMATS,
    is_file_source,
    source_file_format,
    VALID_JSON_MODES,
    VALID_TARGET_FILE_FORMATS,
    VALID_TARGET_TYPES,
    VALID_ENGINE_SPARK_FIELDS,
    VALID_SPARK_SOURCE_TYPES,
    SPARK_ONLY_ENDPOINT_TYPES,
    DEFERRED_SPARK_SUBMIT_MODES,
    VALID_SPARK_SUBMIT_MODES,
    DEFAULT_ICEBERG_PUBLISH_MODE,
    ICEBERG_CATALOG_TYPE_FIELD,
    ICEBERG_PUBLISH_MODE_FIELD,
    ICEBERG_TYPE,
    REJECTED_ICEBERG_CATALOG_TYPES,
    REJECTED_ICEBERG_LOAD_METHODS,
    SECRET_FIELD_HINTS,
    VALID_ICEBERG_CATALOG_TYPES,
    VALID_ICEBERG_PUBLISH_MODES,
    CDC_APPLY_LOAD_METHOD,
    CDC_MAX_BATCH_RECORDS_FIELD,
    CDC_START_OFFSETS_FIELD,
    CDC_START_POLICY_FIELD,
    KAFKA_TOPIC_FIELD,
    KAFKA_TYPE,
    VALID_CDC_START_POLICIES,
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
        # F6.3 — INV-5 sır taraması artık kaynak/hedef-agnostik ve KOŞULSUZ
        # çalışır (eskiden yalnız Iceberg'de). Ucuz bir anahtar-adı taramasıdır;
        # kafka SASL alanlarının config'e sızmasını da aynı kapı yakalar.
        self._check_no_secret_fields(task)
        # F6.0 — engine bağlama kuralları source/target whitelist'inden ÖNCE
        # çalışır: `iceberg` hedefinde "açık spark gerekir" mesajı, "geçersiz
        # target_type" mesajından daha aktörlenebilirdir. Mevcut kontrollerin
        # sırası değişmedi; bu YENİ bir kontroldür ve engine bloğu olmayan +
        # iceberg olmayan config'ler için no-op'tur.
        self._check_engine(task)
        # F4.3E — kök `require_reconciliation` tip kontrolü (additive alan).
        self._check_require_reconciliation(task)
        self._check_source_type(task)
        self._check_target_type(task)
        self._check_load_method(task)
        # F6.2 — genel whitelist'ten SONRA: "gecersiz load_method" ile
        # "Iceberg'de bu load_method reddedildi, sunu kullan" farkli mesajlar.
        self._check_iceberg(task)
        self._check_upsert_config(task)
        # F6.3 — `_check_upsert_config`'tan SONRA: normalize edilmiş
        # `upsert_match_columns` değerine ihtiyaç duyar.
        self._check_kafka(task)
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

    def _check_require_reconciliation(self, task: dict) -> None:
        """F4.3E — `require_reconciliation` kuralları.

        - Task seviyesinde public ad **fail-loud reddedilir** (review-fix
          MINOR-1): bu bir garanti bayrağıdır; sessiz yok-sayma, sessiz
          garanti değişikliğine bitişik bir tuzaktır. Task-level override
          spec'te TANIMLI DEĞİLDİR (epic kartı + CONFIG_AND_DATA_CONTRACTS:
          "kök alan").
        - Kök değer boolean olmalıdır (verilmişse). Sessiz tip düzeltmesi
          yapılmaz: "yes"/1 truthy diye kabul edilirse geliştirici garantiyi
          yanlışlıkla kapatabilir/açabilir.
        """
        if REQUIRE_RECONCILIATION_FIELD in task:
            raise ValidationError(
                f"'{REQUIRE_RECONCILIATION_FIELD}' is a root-level field and "
                "is not supported at task level. Move it to the top of the "
                "flow config (next to source_db_var/target_db_var)."
            )
        value = task.get(REQUIRE_RECONCILIATION_KEY)
        if value is not None and not isinstance(value, bool):
            raise ValidationError(
                "Root field 'require_reconciliation' must be a boolean "
                f"(true/false); got {value!r}."
            )

    def _check_required(self, task: dict) -> None:
        required_fields = list(REQUIRED_TASK_FIELDS)
        source_type = task.get("source_type")
        # sql and file sources carry no source_schema.
        if (
            source_type == "sql"
            or source_type == FILE_SOURCE_TYPE
            or source_type == "parquet"
            # F6.3 — bir Kafka topic'i schema.table cifti degildir.
            or source_type == KAFKA_TYPE
        ):
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

    def _check_iceberg(self, task: dict) -> None:
        """F6.2 — Iceberg uc noktasinin kosullu kurallari (EX-D030).

        `VALID_LOAD_METHODS` GENISLETILMEZ; uc deger yalnizca Iceberg
        baglaminda ve GEREKCESIYLE reddedilir. `catalog_type` config'de zorunlu
        ve sirsizdir: baglanti detayi/kimlik Airflow Connection'da yasar ama
        validator Connection okumaz (DAG-parse'ta ag/DB erisimi yok), bu yuzden
        tip secimi burada kalir ve red DAG-parse'ta dogar.
        """
        source_type = str(task.get("source_type") or "").strip().lower()
        target_type = str(task.get("target_type") or "db").strip().lower()
        is_iceberg = ICEBERG_TYPE in (source_type, target_type)

        if not is_iceberg:
            # Iceberg'e ozgu alanlari sessizce yok saymak, kullaniciya HIC
            # uygulanmayan bir ayar sunmaktir (INV-1). Deger-bazli kontrol:
            # `catalog_type: null` acik bir varsayilandir, reddedilmez.
            stray = [
                field
                for field in (ICEBERG_CATALOG_TYPE_FIELD, ICEBERG_PUBLISH_MODE_FIELD)
                if task.get(field) is not None
            ]
            if stray:
                raise ValidationError(
                    f"{stray} yalniz Iceberg kaynak/hedefinde anlamlidir; "
                    f"bu task'in source_type='{source_type}', "
                    f"target_type='{target_type}'."
                )
            return

        self._check_iceberg_catalog(task)
        self._check_iceberg_publish_mode(task)
        self._check_iceberg_load_method(task)
        self._check_iceberg_connection(task)

    def _check_no_secret_fields(self, task: dict) -> None:
        """INV-5: kimlik yalniz Airflow Connection'da; config'de sir olamaz.

        F6.3'te `_check_iceberg_secrets`'tan hoist edildi ve KOSULSUZ calisir:
        kural hicbir zaman Iceberg'e ozgu degildi, config'e yazilan her sir
        DAG dosyasina ve loglara sizar (kafka SASL alanlari dahil).

        ANAHTAR adina bakilir, degere degil: degeri incelemek (ornegin
        "sifreye benziyor mu") sirri hata mesajina ve log'a tasima riski dogurur.
        """
        leaked = sorted(
            key
            for key in task
            if isinstance(key, str)
            and any(hint in key.lower() for hint in SECRET_FIELD_HINTS)
        )
        if leaked:
            raise ValidationError(
                f"Config'de kimlik tasiyan alan(lar) var: {leaked}. "
                "Kimlik yalniz Airflow Connection / Secrets Backend'de "
                "yasar; config'e yazilan sir DAG dosyasina ve loglara sizar."
            )

    def _check_iceberg_catalog(self, task: dict) -> None:
        raw = task.get(ICEBERG_CATALOG_TYPE_FIELD)
        if not isinstance(raw, str) or not raw.strip():
            raise ValidationError(
                f"Iceberg icin '{ICEBERG_CATALOG_TYPE_FIELD}' zorunludur; "
                "'auto' yoktur (katalog tipi calisma aninda tahmin edilemez). "
                f"Gecerli degerler: {sorted(VALID_ICEBERG_CATALOG_TYPES)}."
            )
        catalog_type = raw.strip().lower()
        reason = REJECTED_ICEBERG_CATALOG_TYPES.get(catalog_type)
        if reason is not None:
            raise ValidationError(
                f"{ICEBERG_CATALOG_TYPE_FIELD}='{catalog_type}' taniniyor ama "
                f"bu teslimde reddedilir: {reason}."
            )
        if catalog_type not in VALID_ICEBERG_CATALOG_TYPES:
            raise ValidationError(
                f"Gecersiz {ICEBERG_CATALOG_TYPE_FIELD}: '{raw}'. "
                f"Gecerli degerler: {sorted(VALID_ICEBERG_CATALOG_TYPES)}."
            )
        # Normalize edilmis degeri geri yaz: motor tarafi tam esleme arar
        # (F6.1'de `submit_mode`'da ogrenilen tuzak).
        task[ICEBERG_CATALOG_TYPE_FIELD] = catalog_type

    def _check_iceberg_publish_mode(self, task: dict) -> None:
        raw = task.get(ICEBERG_PUBLISH_MODE_FIELD)
        if raw is None:
            return  # belgelenmis varsayilan: DEFAULT_ICEBERG_PUBLISH_MODE
        if not isinstance(raw, str) or not raw.strip():
            raise ValidationError(
                f"{ICEBERG_PUBLISH_MODE_FIELD} bos olamaz. Gecerli degerler: "
                f"{sorted(VALID_ICEBERG_PUBLISH_MODES)} "
                f"(verilmezse '{DEFAULT_ICEBERG_PUBLISH_MODE}')."
            )
        mode = raw.strip().lower()
        if mode not in VALID_ICEBERG_PUBLISH_MODES:
            raise ValidationError(
                f"Gecersiz {ICEBERG_PUBLISH_MODE_FIELD}: '{raw}'. "
                f"Gecerli degerler: {sorted(VALID_ICEBERG_PUBLISH_MODES)}."
            )
        task[ICEBERG_PUBLISH_MODE_FIELD] = mode

    def _check_iceberg_load_method(self, task: dict) -> None:
        load_method = task.get("load_method")
        reason = REJECTED_ICEBERG_LOAD_METHODS.get(load_method)
        if reason is not None:
            raise ValidationError(
                f"load_method='{load_method}' Iceberg hedefinde reddedilir: "
                f"{reason}."
            )

    def _check_iceberg_connection(self, task: dict) -> None:
        """Katalog kanali = Airflow Connection, submit modundan bagimsiz.

        F6.1'de `conn_id` yalniz `k8s` icin zorunluydu (submit hedefi). Iceberg
        icin Connection AYRICA katalog uri/warehouse/kimlik kanalidir; `local`
        submit'te de eksik olamaz, yoksa motor katalogu kuramaz.
        """
        options = task.get(ENGINE_SPARK_KEY)
        if not isinstance(options, dict):
            return  # engine blogu kurallari `_check_engine`de zaten zorlandi
        conn_id = options.get("conn_id")
        if not isinstance(conn_id, str) or not conn_id.strip():
            raise ValidationError(
                "Iceberg kaynak/hedefi icin engine.spark.conn_id zorunludur: "
                "katalog uri/warehouse ve kimligi yalniz Airflow Connection'dan "
                "gelir."
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

    def _check_kafka(self, task: dict) -> None:
        """F6.3 — CDC/kafka uc noktasinin kosullu kurallari (EX-D036).

        Debezium/Connect deployment-owned'dir; burasi yalnizca config
        sozlesmesini dogrular. Broker adresi/kimligi Airflow Connection'da
        yasar ve validator Connection OKUMAZ (DAG-parse'ta ag erisimi yok).
        Teslim garantisi: at-least-once consumption + transactionally
        idempotent / effectively-once target effects.
        """
        source_type = str(task.get("source_type") or "").strip().lower()
        load_method = task.get("load_method")
        is_kafka = source_type == KAFKA_TYPE

        if not is_kafka:
            # cdc_apply yalniz kafka kaynaginda anlamlidir; CDC alanlarini
            # sessizce yok saymak kullaniciya HIC uygulanmayan ayar sunmaktir
            # (INV-1, Iceberg stray-field emsali).
            if load_method == CDC_APPLY_LOAD_METHOD:
                raise ValidationError(
                    f"load_method='{CDC_APPLY_LOAD_METHOD}' yalniz "
                    f"source_type='{KAFKA_TYPE}' ile kullanilir; bu task'in "
                    f"source_type='{source_type}'. Debezium CDC olmayan "
                    "yukler icin 'append'/'replace'/'upsert' kullanin."
                )
            stray = [
                field
                for field in (
                    KAFKA_TOPIC_FIELD,
                    CDC_START_POLICY_FIELD,
                    CDC_START_OFFSETS_FIELD,
                    CDC_MAX_BATCH_RECORDS_FIELD,
                )
                if task.get(field) is not None
            ]
            if stray:
                raise ValidationError(
                    f"{stray} yalniz source_type='{KAFKA_TYPE}' ile anlamlidir; "
                    f"bu task'in source_type='{source_type}'."
                )
            return

        # --- kafka kaynagi ---
        # Edition kapisi: CDC runtime'i Enterprise provider'dadir. Lazy import
        # (_check_bulk_api deseni): sicak yol kafka-olmayan configlerde sifir is.
        from ffengine.core.edition import is_enterprise_enabled

        if not is_enterprise_enabled():
            raise ValidationError(
                f"source_type='{KAFKA_TYPE}' Enterprise gerektirir "
                "(FFENGINE_EDITION=enterprise): CDC apply/ledger runtime'i "
                "Enterprise provider'da yasar."
            )

        if load_method != CDC_APPLY_LOAD_METHOD:
            raise ValidationError(
                f"source_type='{KAFKA_TYPE}' yalniz "
                f"load_method='{CDC_APPLY_LOAD_METHOD}' ile kullanilir "
                f"(verilen: '{load_method}'). Debezium envelope'u op bazli "
                "(c/r/u/d + tombstone) uygulanir; append/replace bu kaynakta "
                "veri kaybi/cift riski tasir (fail-loud, sessiz donusum yok)."
            )

        target_type = str(task.get("target_type") or "db").strip().lower()
        if target_type not in ("db", ICEBERG_TYPE):
            raise ValidationError(
                f"kafka kaynagi icin target_type='db' (Track A, sirali "
                f"transactional apply) ya da '{ICEBERG_TYPE}' (F6.3b, acik "
                f"engine.preference: spark ile) gerekir; verilen: "
                f"'{target_type}'."
            )

        topic = task.get(KAFKA_TOPIC_FIELD)
        if not isinstance(topic, str) or not topic.strip():
            raise ValidationError(
                f"source_type='{KAFKA_TYPE}' icin '{KAFKA_TOPIC_FIELD}' "
                "zorunludur (dolu string)."
            )
        task[KAFKA_TOPIC_FIELD] = topic.strip()

        raw_policy = task.get(CDC_START_POLICY_FIELD)
        if not isinstance(raw_policy, str) or not raw_policy.strip():
            raise ValidationError(
                f"'{CDC_START_POLICY_FIELD}' zorunludur; SESSIZ VARSAYILAN "
                f"YOKTUR (EX-D036). Gecerli degerler: "
                f"{sorted(VALID_CDC_START_POLICIES)}. 'earliest' partition'in "
                "low watermark'idir (retention sonrasi gercek alt sinir), "
                "'latest' aktivasyon anindaki high watermark, 'explicit' tam "
                "partition->offset haritasi ister."
            )
        policy = raw_policy.strip().lower()
        if policy not in VALID_CDC_START_POLICIES:
            raise ValidationError(
                f"Gecersiz {CDC_START_POLICY_FIELD}: '{raw_policy}'. "
                f"Gecerli degerler: {sorted(VALID_CDC_START_POLICIES)}."
            )
        task[CDC_START_POLICY_FIELD] = policy

        offsets = task.get(CDC_START_OFFSETS_FIELD)
        if policy == "explicit":
            if not isinstance(offsets, dict) or not offsets:
                raise ValidationError(
                    f"{CDC_START_POLICY_FIELD}='explicit' icin "
                    f"'{CDC_START_OFFSETS_FIELD}' zorunludur: TUM "
                    "partition'lar icin {partition: offset} haritasi "
                    "(eksik partition insan karari olmadan calistirilmaz — "
                    "fail-loud)."
                )
            normalized_offsets: dict[int, int] = {}
            for raw_part, raw_off in offsets.items():
                try:
                    part = int(raw_part)
                    off = int(raw_off)
                except (TypeError, ValueError):
                    raise ValidationError(
                        f"'{CDC_START_OFFSETS_FIELD}' anahtar ve degerleri "
                        f"tamsayi olmalidir; verilen: {raw_part!r}: {raw_off!r}."
                    )
                if part < 0 or off < 0:
                    raise ValidationError(
                        f"'{CDC_START_OFFSETS_FIELD}' negatif olamaz: "
                        f"{part}: {off}."
                    )
                normalized_offsets[part] = off
            task[CDC_START_OFFSETS_FIELD] = normalized_offsets
        elif offsets is not None:
            raise ValidationError(
                f"'{CDC_START_OFFSETS_FIELD}' yalniz "
                f"{CDC_START_POLICY_FIELD}='explicit' ile anlamlidir "
                f"(verilen politika: '{policy}')."
            )

        raw_budget = task.get(CDC_MAX_BATCH_RECORDS_FIELD)
        if raw_budget is not None:
            if not isinstance(raw_budget, int) or isinstance(raw_budget, bool) \
                    or raw_budget < 1:
                raise ValidationError(
                    f"'{CDC_MAX_BATCH_RECORDS_FIELD}' pozitif tamsayi olmali "
                    f"(TOPLAM butce, partition basina degil); verilen: "
                    f"{raw_budget!r}."
                )

        # Debezium key kolonlari = MERGE/UPSERT eslesme anahtari. Normalize
        # edilmis deger `_check_upsert_config`ta yazildi (cagri sirasi).
        if not task.get("upsert_match_columns"):
            raise ValidationError(
                f"load_method='{CDC_APPLY_LOAD_METHOD}' icin "
                "'upsert_match_columns' zorunludur: Debezium key kolonlariyla "
                "birebir eslesir (calisma aninda key payload'ina karsi "
                "dogrulanir; uyusmazlik fail-loud)."
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
        iceberg_endpoint = bool(
            SPARK_ONLY_ENDPOINT_TYPES & {source_type, target_type}
        )

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
                "Iceberg/parquet kaynak/hedef yalniz ACIK engine.preference: spark ile "
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

        if source_type == "parquet":
            self._check_parquet_source(task)
            return

        if source_type == FILE_SOURCE_TYPE:
            self._check_file_source(task)
            return

        # F6.3 — kafka kaynagi source_table tasimaz; topic zorunlulugu
        # `_check_kafka`da dogrulanir.
        if source_type == KAFKA_TYPE:
            return

        source_table = str(task.get("source_table") or "").strip()
        if not source_table:
            raise ValidationError(
                "source_type='table|view|script' icin 'source_table' zorunludur."
            )

    def _check_parquet_source(self, task: dict) -> None:
        """F6.2/EX-D033 — parquet dosya kaynagi (yalniz Spark yolunda).

        `csv|json`in `column_mapping_mode='mapping_file'` sarti BURAYA
        UYGULANMAZ: o kural tipsiz metin formatlari icindir (tip cikarimi
        yasak). Parquet **kendi semasini tasir**, dolayisiyla ayni sarti
        dayatmak kullaniciya gereksiz bir mapping dosyasi yazdirmak olurdu.
        """
        file_path = str(task.get("file_path") or "").strip()
        if not file_path:
            raise ValidationError(
                "source_type='parquet' icin 'file_path' zorunludur "
                "('source_table' degil)."
            )

    def _check_file_source(self, task: dict) -> None:
        """F1.4/F1.5 — file source: file_path + format + explicit mapping.

        Format bir **veri** sozlesmesidir (hangi ayristirici kosacagini
        belirler), bu yuzden burada fail-loud dogrulanir — hedef tarafla
        (`_check_target_type`) simetrik.
        """
        file_path = str(task.get("file_path") or "").strip()
        if not file_path:
            raise ValidationError(
                "source_type='file' icin 'file_path' zorunludur "
                "('source_table' degil)."
            )
        fmt = source_file_format(task)
        if fmt not in VALID_SOURCE_FILE_FORMATS:
            raise ValidationError(
                f"Gecersiz source_file_format: '{fmt}'. "
                f"Gecerli degerler: {sorted(VALID_SOURCE_FILE_FORMATS)}"
            )
        if task.get("column_mapping_mode") != "mapping_file":
            raise ValidationError(
                "Dosya kaynagi explicit mapping gerektirir: "
                "column_mapping_mode='mapping_file' olmali (tip cikarimi yok)."
            )
        if fmt == "json":
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
        fmt_raw = task.get("target_file_format")
        if target_type == "file":
            if not str(task.get("target_file_path") or "").strip():
                raise ValidationError(
                    "target_type='file' icin 'target_file_path' zorunludur."
                )
            if fmt_raw is not None:
                fmt = str(fmt_raw).strip().lower()
                if fmt not in VALID_TARGET_FILE_FORMATS:
                    raise ValidationError(
                        f"Gecersiz target_file_format: '{fmt_raw}'. "
                        f"Gecerli degerler: {sorted(VALID_TARGET_FILE_FORMATS)}"
                    )
                # JSON (JSONL) satir basina obje yazar: baslik satiri ve
                # ayrac kavrami yoktur. Sessizce yok saymak yerine fail-loud
                # (kafka "stray field" deseni) -- kullanici "JSON yazdim ama
                # CSV cikti" tuzagina dusmemeli.
                if fmt == "json":
                    stray = [
                        name
                        for name in ("target_header", "target_delimiter")
                        if task.get(name) is not None
                    ]
                    if stray:
                        raise ValidationError(
                            f"{stray} yalniz target_file_format='csv' ile "
                            "anlamlidir; JSON hedefi satir basina obje yazar."
                        )
        elif fmt_raw is not None:
            raise ValidationError(
                "target_file_format yalniz target_type='file' ile anlamlidir."
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
        if is_file_source(task) or task.get("target_type") == "file":
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
