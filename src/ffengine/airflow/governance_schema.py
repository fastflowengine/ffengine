"""
F4.3E — Kanonik governance event modeli (`ffgov` v1, REVISED OL-first v27.5).

Şema **tek yerde** burada tanımlıdır; `airflow/operator.py`'nin bütün rapor
yüzeyleri (summary, mapped-partition payload'ı, aggregate, error payload'ı ve
`_log_structured`) bu modülü kullanır. Konum gerekçesi: tüketiciler airflow
katmanındadır; modül Community-pure'dur ve core'a yeni bağımlılık eklemez.

Tasarım sözleşmeleri (epic-F4.3E + EX-D039.6 + CONFIG_AND_DATA_CONTRACTS
"ffgov v1" bloğu):

- **Taşıyıcı mevcut XCom satırlarıdır** — yeni store/XCom anahtarı YOK; alanlar
  mevcut return-value/error_summary sözlüklerinin İÇİNE additive eklenir.
- **Çekirdek alanlar None-filtresine takılmaz** — ``null`` bir DEĞERDİR
  (`CORE_NULLABLE_FIELDS`).
- **Dataset kimliği tahmin edilmez** — `source_type: sql` opak
  ``sql:<task_group_id>`` kalır (INV-1, T-F4.3E-6); event'te veri değeri ve
  credential taşınmaz, bağlantı yalnız ``conn_id`` ile anılır (INV-5).
- **`counter_source` dürüstlük kanalıdır**: ``engine`` (Streamer'ın
  doğrulanmış toplamı) · ``target_native`` (hedefin otoritatif sayısı —
  Iceberg snapshot summary / CDC ledger) · ``unavailable`` (otoritatif sayı
  yok → sayaçlar açık ``null``, ``reconciliation_status = not_applicable``).
- **Sessiz garanti düşürme yasağı**: ``require_reconciliation`` (varsayılan
  ``true``) + ``unavailable`` → task başında, kaynağa hiç bağlanılmadan
  fail-loud (`ReconciliationUnavailableError`).
- **Sıcak yol maliyeti sıfır** (INV-3): satır başına ek iş yok; task başına
  en fazla BİR ucuz manifest dosyası okuması (`config_revision_for`),
  ek DB sorgusu yok.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any

from ffengine.errors.exceptions import (
    ReconciliationError,
    ReconciliationUnavailableError,
)

_log = logging.getLogger(__name__)

#: `ffgov` v1 — şema sürüm etiketi (EX-D038: bağımsız protokol değil, FFEngine
#: deep-metadata modeli; bu XCom şeması ffgovernance gateway'inin (F7.3)
#: OKUMA sözleşmesidir).
SCHEMA_VERSION: str = "ffgov-1"

EVENT_SCOPE_TASK: str = "task"
EVENT_SCOPE_PARTITION: str = "partition"
EVENT_SCOPES: frozenset[str] = frozenset({EVENT_SCOPE_TASK, EVENT_SCOPE_PARTITION})

OUTCOME_STARTED: str = "started"
OUTCOME_SUCCEEDED: str = "succeeded"
OUTCOME_FAILED: str = "failed"
OUTCOMES: frozenset[str] = frozenset(
    {OUTCOME_STARTED, OUTCOME_SUCCEEDED, OUTCOME_FAILED}
)

COUNTER_SOURCE_ENGINE: str = "engine"
COUNTER_SOURCE_TARGET_NATIVE: str = "target_native"
COUNTER_SOURCE_UNAVAILABLE: str = "unavailable"
COUNTER_SOURCES: frozenset[str] = frozenset(
    {
        COUNTER_SOURCE_ENGINE,
        COUNTER_SOURCE_TARGET_NATIVE,
        COUNTER_SOURCE_UNAVAILABLE,
    }
)

#: `counter_source = unavailable` beyanının zorunlu muhasebe durumu.
RECONCILIATION_NOT_APPLICABLE: str = "not_applicable"

#: Motor kimliği → SAYAÇ VARLIĞI taban sınıfı (iddiasız etiket). Bilinmeyen/
#: boş kimlik dürüstçe ``unavailable``dır — varsayım üretilmez (INV-1).
#:
#: Review-fix (F4.3E MAJOR-1, koda dayalı dürüstlük): kaba engine_type →
#: `target_native` eşlemesi YANLIŞTI. Motor gerçeği (Enterprise kaynakları):
#:
#: - **spark append/replace:** ``rows_written = added_records(snapshot
#:   summary)`` + eşitlik denetimi → status ``"passed"``
#:   (`engines/spark_job.py:305-314`, `spark_iceberg.py:83-114`). Sayı hedefin
#:   KENDİ kaydıdır → yalnız bu sinyal `target_native` hak eder.
#: - **spark merge:** ``rows_written = rows_read`` (motorun okuma sayısının
#:   EKOsu, `spark_job.py:306-308`) + ``not_applicable_merge`` → `engine`.
#: - **spark CDC (Track B):** ``not_applicable_cdc_merge`` → `engine`.
#: - **cdc (Track A):** ``records_applied`` koordinatörün KENDİ apply
#:   sayacıdır (`cdc/core.py:512-523` "fiziksel değişen satır sayısı
#:   DEĞİLDİR"; read==applied motor tarafında doğrulanır) → `engine`.
#: - **standard/pipeline:** Streamer'ın doğrulanmış toplamı → `engine`.
#:   Not: bulk `finalize()->int` hedef-attested bir sayı DÖNDÜRÜR (F3.3), ama
#:   FlowResult bu ayrımı taşımıyor → ayırt edici sinyal yokken yukarı
#:   yuvarlanmaz, `engine` kalır (ileride additive sinyalle yükseltilebilir).
#:
#: `target_native` yükseltmesi haritada DEĞİL, `resolve_counter_source`'ta
#: sinyal-bazlı yapılır; belirsiz her durum `engine` (iddiasız) kalır.
_ENGINE_COUNTER_AUTHORITY: dict[str, str] = {
    "standard": COUNTER_SOURCE_ENGINE,
    "pipeline": COUNTER_SOURCE_ENGINE,
    "spark": COUNTER_SOURCE_ENGINE,
    "cdc": COUNTER_SOURCE_ENGINE,
}

#: Hedef-attested muhasebe SİNYALİ: yalnız Spark Iceberg `added-records`
#: eşitlik yolunun ürettiği status (`spark_iceberg.RECONCILIATION_PASSED`
#: literal'i; Community Enterprise'ı import edemez → literal + bu kayıt).
#: `not_applicable_merge`/`not_applicable_cdc_merge`/`probe`/
#: `cdc_ordered_applied` BİLEREK dışarıda: sayıları motor tarafı üretir.
_TARGET_ATTESTED_STATUSES: frozenset[str] = frozenset({"passed"})

#: `_log_structured` içinde None iken de YAYIMLANAN çekirdek şema alanları.
#: `null` bir değerdir (kabul kriteri); şema dışı opsiyonel alanların mevcut
#: None-düşürme davranışı değişmez.
CORE_NULLABLE_FIELDS: frozenset[str] = frozenset(
    {
        "schema_version",
        "event_scope",
        "outcome",
        "dag_id",
        "run_id",
        "task_id",
        "map_index",
        "partition_id",
        "source_conn_id",
        "target_conn_id",
        "source_dataset",
        "source_dataset_resolved",
        "target_dataset",
        "target_dataset_resolved",
        "source_type",
        "target_type",
        "engine_type",
        "counter_source",
        "config_revision",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "reconciliation_status",
        "error_code",
    }
)

#: `ui/studio_service.py` revision-manifest deseninin operator tarafındaki
#: eşleri. studio_service BURADAN import EDİLMEZ (UI yığını sıcak yola
#: girmesin); literal eşitliği unit test mühürler
#: (`test_history_dir_literal_matches_studio_service`).
STUDIO_HISTORY_DIR_NAME: str = ".flow_studio_history"
_REVISION_DIR_RE = re.compile(r"^rev_(\d{6})$")

#: Opak SQL dataset kimliği öneki (T-F4.3E-6 — tablo tahmini YASAK).
_SQL_OPAQUE_PREFIX: str = "sql:"
#: SQL gibi opak ele alınan kaynak tipleri (`script` de gövdesi serbest SQL'dir).
_OPAQUE_SQL_SOURCE_TYPES: frozenset[str] = frozenset({"sql", "script"})
#: Dosya-yolu taşıyan kaynak tipleri (şablonlu yol: dataset=şablon,
#: `*_resolved`=render sonrası — T-F4.3E-5). `parquet` F6.2'de dosya
#: kaynağıdır (Spark yolunda okunur) — kimliği yine dosya yoludur.
_FILE_PATH_SOURCE_TYPES: frozenset[str] = frozenset({"csv", "json", "parquet"})


def event_header(*, event_scope: str, outcome: str) -> dict[str, Any]:
    """Kontrat başlığı — geçersiz enum fail-loud (sessiz üretim yok)."""
    if event_scope not in EVENT_SCOPES:
        raise ValueError(
            f"Unknown event_scope '{event_scope}'; expected one of "
            f"{sorted(EVENT_SCOPES)}."
        )
    if outcome not in OUTCOMES:
        raise ValueError(
            f"Unknown outcome '{outcome}'; expected one of {sorted(OUTCOMES)}."
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "event_scope": event_scope,
        "outcome": outcome,
    }


def expected_counter_source(engine_type: str | None) -> str:
    """Motor kimliğinin sayaç otoritesi sınıfı (preflight'ta da kullanılır)."""
    return _ENGINE_COUNTER_AUTHORITY.get(
        str(engine_type or "").strip().lower(), COUNTER_SOURCE_UNAVAILABLE
    )


def resolve_counter_source(
    engine_type: str | None,
    *,
    rows_read: int | None,
    rows_written: int | None,
    snapshot_id: Any = None,
    reconciliation_status: str | None = "",
) -> str:
    """Tamamlanmış sonuçtan fiili `counter_source` değerini türetir.

    Sinyal-bazlı dürüstlük (review-fix MAJOR-1):

    - Bilinmeyen motor kimliği → ``unavailable`` (sayaçlar zaten null
      yayımlanır; bu dala yalnız `require_reconciliation: false` ile gelinir).
    - Bilinen motor + ``null`` sayaç = sözleşme ihlali →
      `ReconciliationError` (T-F4.3E-9): dört motorun da sonuç kanalı sayaç
      doldurmayı garanti eder; eksikse kanal bozuktur, sessiz devam edilmez.
    - ``target_native`` yalnız gerçek hedef-attestasyon sinyalinde: Spark +
      snapshot kimliği + `added-records` eşitlik yolunun status'u
      (`_TARGET_ATTESTED_STATUSES`). Diğer HER durum — spark merge (okuma
      ekosu), CDC (koordinatör sayacı), probe, bilinmeyen status —
      **iddiasız** ``engine`` etiketi alır; asla yukarı yuvarlama yapılmaz.
    """
    if expected_counter_source(engine_type) == COUNTER_SOURCE_UNAVAILABLE:
        return COUNTER_SOURCE_UNAVAILABLE
    if rows_read is None or rows_written is None:
        raise ReconciliationError(
            "counter_source contract violated: "
            f"engine_type='{engine_type}' guarantees transfer counters but "
            f"reported null (rows_read={rows_read!r}, "
            f"rows_written={rows_written!r}). The engine counter channel is "
            "broken; refusing to publish unverified accounting.",
            details={"engine_type": engine_type},
        )
    if (
        str(engine_type or "").strip().lower() == "spark"
        and snapshot_id is not None
        and str(reconciliation_status or "").strip() in _TARGET_ATTESTED_STATUSES
    ):
        return COUNTER_SOURCE_TARGET_NATIVE
    return COUNTER_SOURCE_ENGINE


def preflight_reconciliation(
    engine_type: str | None, *, require_reconciliation: bool
) -> None:
    """Sessiz garanti düşürme yasağı (T-F4.3E-10).

    Sayaç otoritesi ``unavailable`` olan bir motorla `require_reconciliation`
    (varsayılan ``true``) birleşirse task **başında**, kaynağa hiç
    bağlanılmadan fail-loud olunur.
    """
    if not require_reconciliation:
        return
    if expected_counter_source(engine_type) == COUNTER_SOURCE_UNAVAILABLE:
        raise ReconciliationUnavailableError(
            f"engine_type='{engine_type}' provides no authoritative transfer "
            "counters (counter_source=unavailable) while "
            "require_reconciliation=true (default). Refusing to start the "
            "transfer. To knowingly opt out, set root "
            "'require_reconciliation: false' in the flow config; counters "
            "will then be published as null with "
            f"reconciliation_status='{RECONCILIATION_NOT_APPLICABLE}'.",
            details={"engine_type": engine_type},
        )


def airflow_identity(context: dict | None) -> dict[str, Any]:
    """Airflow kimlik bloğu — context yoksa alanlar ``null`` (uydurma yok)."""
    ctx = context or {}
    ti = ctx.get("ti")
    dag_id = getattr(ti, "dag_id", None)
    if dag_id is None:
        dag_id = getattr(ctx.get("dag"), "dag_id", None)
    run_id = getattr(ti, "run_id", None)
    if run_id is None:
        run_id = getattr(ctx.get("dag_run"), "run_id", None)
    task_id = getattr(ti, "task_id", None)
    map_index = getattr(ti, "map_index", None)
    try:
        map_index = int(map_index) if map_index is not None else None
    except (TypeError, ValueError):
        map_index = None
    if map_index is not None and map_index < 0:
        # Airflow, mapped olmayan task'ta -1 kullanır; şemada null yayımlanır.
        map_index = None
    return {
        "dag_id": None if dag_id is None else str(dag_id),
        "run_id": None if run_id is None else str(run_id),
        "task_id": None if task_id is None else str(task_id),
        "map_index": map_index,
    }


def dataset_identity(
    task_config: dict[str, Any],
    *,
    task_group_id: str,
    source_conn_id: str | None,
    target_conn_id: str | None,
    raw_file_path: str | None = None,
    raw_target_file_path: str | None = None,
    paths_rendered: bool = True,
) -> dict[str, Any]:
    """Dataset kimlik bloğu (T-F4.3E-4/5/6, INV-5).

    - DB uçları: ``SCHEMA.TABLE`` (bileşen eksikse ``null`` — tahmin yok).
    - ``sql``/``script``: opak ``sql:<task_group_id>`` — SQL gövdesinden tablo
      TÜRETİLMEZ (INV-1).
    - Dosya uçları: ``dataset`` = render **edilmemiş** şablon (verildiyse),
      ``*_resolved`` = render sonrası fiili yol. ``paths_rendered=False``
      (harici motor yolu: dispatch `_render_file_paths`'ten ÖNCE olur) →
      ``*_resolved`` dürüstçe ``null`` — render edilmemiş şablonu "resolved"
      diye yayımlamak T-F4.3E-5 semantiğini bozardı (review-fix MINOR-3).
    - ``kafka``: ``kafka:<topic>``.
    - Bağlantı yalnız ``conn_id`` ile anılır; host/port/kimlik yayımlanmaz.
    """
    identity = {
        "source_conn_id": None if source_conn_id is None else str(source_conn_id),
        "target_conn_id": None if target_conn_id is None else str(target_conn_id),
    }
    identity.update(
        _source_dataset_identity(
            task_config,
            task_group_id=task_group_id,
            raw_file_path=raw_file_path,
            paths_rendered=paths_rendered,
        )
    )
    identity.update(
        _target_dataset_identity(
            task_config,
            raw_target_file_path=raw_target_file_path,
            paths_rendered=paths_rendered,
        )
    )
    return identity


def _source_dataset_identity(
    task_config: dict[str, Any],
    *,
    task_group_id: str,
    raw_file_path: str | None,
    paths_rendered: bool,
) -> dict[str, Any]:
    source_type = str(task_config.get("source_type") or "").strip().lower() or None
    if source_type in _OPAQUE_SQL_SOURCE_TYPES:
        opaque = f"{_SQL_OPAQUE_PREFIX}{task_group_id}"
        dataset, resolved = opaque, opaque
    elif source_type in _FILE_PATH_SOURCE_TYPES:
        current = task_config.get("file_path")
        current = None if current is None else str(current)
        dataset = str(raw_file_path) if raw_file_path is not None else current
        resolved = current if paths_rendered else None
    elif source_type == "kafka":
        topic = str(task_config.get("kafka_topic") or "").strip()
        dataset = f"kafka:{topic}" if topic else None
        resolved = dataset
    else:
        dataset = _qualified_name(
            task_config.get("source_schema"), task_config.get("source_table")
        )
        resolved = dataset
    return {
        "source_type": source_type,
        "source_dataset": dataset,
        "source_dataset_resolved": resolved,
    }


def _target_dataset_identity(
    task_config: dict[str, Any],
    *,
    raw_target_file_path: str | None,
    paths_rendered: bool,
) -> dict[str, Any]:
    target_type = str(task_config.get("target_type") or "db").strip().lower()
    if target_type == "file":
        current = task_config.get("target_file_path")
        current = None if current is None else str(current)
        dataset = (
            str(raw_target_file_path) if raw_target_file_path is not None else current
        )
        resolved = current if paths_rendered else None
    else:
        dataset = _qualified_name(
            task_config.get("target_schema"), task_config.get("target_table")
        )
        resolved = dataset
    return {
        "target_type": target_type,
        "target_dataset": dataset,
        "target_dataset_resolved": resolved,
    }


def config_revision_for(config_path: str | None, dag_id: str | None) -> str | None:
    """Aktif revision manifest'inin bundle hash'i (EX-D039.6).

    Kaynak: Flow Studio'nun save-time revision manifest'i
    (``<config_dir>/.flow_studio_history/<dag_id>/rev_NNNNNN/manifest.json``,
    ``hashes.bundle``). En yeni revision okunur; revision yoksa sessizce
    ``None`` (EX-D039.6 — hiçbir T-ID fail-loud istemez). Manifest ANOMALİSİ
    (bozuk/JSON-ama-dict-değil/`hashes` dict değil ve her türlü beklenmedik
    hata) da ``None``dır ama **görünür log uyarısıyla** — governance
    metadata'sı ETL task'ını hiçbir koşulda düşürmez (review-fix MAJOR-2,
    GOV-INV-4 ruhu). Eski revision'a SESSİZCE düşülmez: aktif olan en yeni
    manifest'tir, bozuksa yerine eskisini beyan etmek yanlış kimlik olurdu.
    Sıcak yola DB sorgusu girmez (task başına tek ucuz dosya okuması,
    T-F4.3E-18).
    """
    if not config_path or not dag_id:
        return None
    dag_key = str(dag_id).strip()
    if not dag_key:
        return None
    try:
        history_root = (
            Path(config_path).resolve().parent / STUDIO_HISTORY_DIR_NAME / dag_key
        )
        if not history_root.is_dir():
            return None
        revisions = sorted(
            item
            for item in history_root.iterdir()
            if item.is_dir() and _REVISION_DIR_RE.fullmatch(item.name)
        )
        for revision_dir in reversed(revisions):
            manifest_path = revision_dir / "manifest.json"
            if not manifest_path.is_file():
                continue
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                _log.warning(
                    "config_revision unavailable: revision manifest %s is "
                    "valid JSON but not an object; publishing null.",
                    manifest_path.as_posix(),
                )
                return None
            hashes = manifest.get("hashes")
            if not isinstance(hashes, dict):
                _log.warning(
                    "config_revision unavailable: revision manifest %s has a "
                    "non-object 'hashes' entry; publishing null.",
                    manifest_path.as_posix(),
                )
                return None
            bundle_hash = hashes.get("bundle")
            return str(bundle_hash) if bundle_hash else None
        return None
    except Exception:  # noqa: BLE001 - governance alanı ETL'i asla düşürmez
        _log.warning(
            "config_revision unavailable: unexpected error while reading the "
            "revision manifest for dag_id=%s; publishing null.",
            dag_key,
            exc_info=True,
        )
        return None


_MISSING = object()


def uniform_value(payloads: list[dict[str, Any]] | None, field: str) -> Any:
    """Partition payload'ları boyunca tek tutarlı değer; yoksa/karışıksa None.

    Aynı task'ın partition'ları aynı kimliği taşır; legacy payload'lar alanı
    hiç taşımaz. Tek ayrık non-None değer → o değer (ORİJİNAL tipiyle —
    review-fix NIT-3: str dönüşümü yok, `True`/`1` tip kontrolüyle ayrışır);
    sıfır ya da birden çok ayrık değer → ``null`` (uydurma birleştirme yok).
    """
    found: Any = _MISSING
    for item in payloads or []:
        if not isinstance(item, dict):
            continue
        value = item.get(field)
        if value is None:
            continue
        if found is _MISSING:
            found = value
        elif type(found) is not type(value) or found != value:
            return None
    return None if found is _MISSING else found


def _qualified_name(schema: Any, table: Any) -> str | None:
    table_name = str(table or "").strip()
    if not table_name:
        return None
    schema_name = str(schema or "").strip()
    return f"{schema_name}.{table_name}" if schema_name else table_name
