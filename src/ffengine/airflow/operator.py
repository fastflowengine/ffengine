"""
C07 — FFEngineOperator + yardımcı fonksiyonlar.

FFEngineOperator, Airflow ortamında FFEngine Flow pipeline'ını orkestre eder:
  plan → prepare → run (3-fazlı iç orkestrasyon).
"""

import json
import logging
import re
from typing import Any

try:
    from airflow.sdk.bases.operator import BaseOperator
except Exception:  # pragma: no cover - airflow olmayan ortamlarda import fallback
    try:
        from airflow.models.baseoperator import BaseOperator
    except Exception:

        class BaseOperator:  # type: ignore[no-redef]
            template_fields: tuple[str, ...] = ()

            def __init__(self, *args, **kwargs):
                self.task_id = kwargs.get("task_id")


from ffengine.airflow import governance_schema as _gov
from ffengine.core.base_engine import FlowResult
from ffengine.config.dag_param_flow import BUILTIN_DAG_PARAM_BINDING_ERROR
from ffengine.config.schema import (
    DEFAULT_REQUIRE_RECONCILIATION,
    REQUIRE_RECONCILIATION_KEY,
)
from ffengine.errors import error_payload, normalize_exception
from ffengine.errors.exceptions import ConfigError, EngineError

_log = logging.getLogger(__name__)


def _require_reconciliation(task_config: dict) -> bool:
    """F4.3E — kök `require_reconciliation` (varsayılan ``true``)."""
    value = task_config.get(REQUIRE_RECONCILIATION_KEY)
    if value is None:
        return DEFAULT_REQUIRE_RECONCILIATION
    return bool(value)


def _log_structured(
    *,
    level: int,
    stage: str,
    message: str,
    # Airflow log kaydinda zaten var (asagidaki nota bakiniz); imzada 18 cagri
    # yeri oldugu icin korunur, log govdesine YAZILMAZ.
    task_group_id: str,  # noqa: ARG001
    source_db: str,
    target_db: str,
    rows: int = 0,
    duration_seconds: float = 0.0,
    **optional,
) -> None:
    # Airflow log kaydi zaten timestamp/level/logger/dag_id/run_id/task_id ve
    # task_group_id'yi kendi basina yaziyor; ayni degerleri tekrar etmek asil
    # parametreleri (uc kimligi, satir sayisi, hata detayi) gorunmez kiliyordu
    # (kullanici geri bildirimi 2026-08-19). Zarf alanlari cikarildi.
    #
    # NOT: bu YALNIZCA log satirinin bicimidir. F4.3E kanonik event semasi
    # (XCom payload'i) degismedi -- orada tam alan kumesi korunur.
    payload = {
        "message": message,
        "stage": stage,
        "source_db": source_db,
        "target_db": target_db,
    }
    if rows:
        payload["rows"] = rows
    if duration_seconds:
        payload["duration_seconds"] = round(float(duration_seconds), 3)
    for k, v in optional.items():
        # F4.3E — kanonik şemanın çekirdek alanları None-filtresine takılmaz:
        # `null` bir DEĞERDİR (örn. hata yolunda reconciliation_status=null).
        # Şema dışı opsiyonel alanların davranışı değişmedi.
        if v is not None or k in _gov.CORE_NULLABLE_FIELDS:
            payload[k] = v
    _log.log(level, "%s", payload)


def _endpoint_host(params: Any) -> str | None:
    """Log icin uc sunucu adi (yalnizca HOST).

    Kullanici karari (2026-08-19): "hangi sunucudan nereye" sorusu loglardan
    okunabilmeli; host yayimlanir. PORT, KULLANICI ADI ve PAROLA yayimlanmaz
    (AGENTS.md "no credentials": secret yalnizca Connection/Secrets Backend'de).
    Dosya/SFTP uclarinda host yoksa ``null`` doner -- uydurma deger yazilmaz.
    """
    if not isinstance(params, dict):
        return None
    host = params.get("host")
    if host is None:
        return None
    host = str(host).strip()
    return host or None


# ---------------------------------------------------------------------------
# Dialect çözümleme
# ---------------------------------------------------------------------------

_CONN_TYPE_TO_DIALECT: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mssql": "mssql",
    "tds": "mssql",
    "oracle": "oracle",
}


def _get_dialect_class(dialect_key: str):
    """Dialect key'e göre sınıf döndürür (lazy import)."""
    from ffengine.dialects import PostgresDialect, MSSQLDialect, OracleDialect

    _map = {
        "postgres": PostgresDialect,
        "mssql": MSSQLDialect,
        "oracle": OracleDialect,
    }
    return _map[dialect_key]


def resolve_dialect(conn_type: str):
    """
    Airflow conn_type string'ini BaseDialect örneğine çözer.

    Raises
    ------
    ConfigError : Bilinmeyen conn_type.
    """
    key = _CONN_TYPE_TO_DIALECT.get(conn_type.lower() if conn_type else "")
    if key is None:
        raise ConfigError(
            f"Desteklenmeyen Airflow connection tipi: {conn_type!r}. "
            f"Geçerli değerler: {sorted(_CONN_TYPE_TO_DIALECT)}"
        )
    return _get_dialect_class(key)()


# ---------------------------------------------------------------------------
# WHERE kombinasyonu
# ---------------------------------------------------------------------------


def combine_where(base_where: str | None, partition_where: str | None) -> str | None:
    """
    Base WHERE ve partition WHERE'i AND ile birleştirir.

    Her ikisi de varsa ``(base) AND (partition)`` döner.
    Yalnız biri varsa o döner. İkisi de None ise None döner.
    """
    if base_where and partition_where:
        return f"({base_where}) AND ({partition_where})"
    return base_where or partition_where or None


# ---------------------------------------------------------------------------
# Sonuç birleştirme
# ---------------------------------------------------------------------------


def _single_engine_type(values, *, source: str) -> str | None:
    """Return one declared engine type; reject mixed identities fail-loud."""
    engine_types = {str(value) for value in values if value is not None}
    if len(engine_types) > 1:
        raise EngineError(
            f"{source} birden fazla engine_type kimligi tasiyor: "
            f"{sorted(engine_types)}. Sessiz birlestirme yapilmaz."
        )
    return next(iter(engine_types), None)


def aggregate_results(results: list[FlowResult]) -> FlowResult:
    """
    Birden fazla partition sonucunu tek bir FlowResult'a birleştirir.

    duration_seconds en uzun partition süresidir (wall-clock).
    """
    if not results:
        return FlowResult(
            rows=0,
            duration_seconds=0.0,
            throughput=0.0,
            partitions_completed=0,
            errors=[],
        )

    total_rows = sum(r.rows for r in results)
    max_duration = max(r.duration_seconds for r in results)
    throughput = total_rows / max_duration if max_duration > 0 else 0.0
    all_errors = [e for r in results for e in r.errors]
    # F3.3 — sayaçlar partition'lar boyunca toplanır; muhasebe durumu ancak
    # HER partition doğrulanmışsa "passed" kalır (biri legacy ise legacy).
    statuses = {r.reconciliation_status for r in results}
    status = "passed" if statuses == {"passed"} else "legacy"
    engine_type = _single_engine_type(
        (result.engine for result in results), source="FlowResult listesi"
    )

    return FlowResult(
        rows=total_rows,
        duration_seconds=round(max_duration, 3),
        throughput=round(throughput, 2),
        partitions_completed=len(results),
        errors=all_errors,
        rows_read=sum(int(r.rows_read or 0) for r in results),
        rows_written=sum(int(r.rows_written or 0) for r in results),
        rows_rejected=sum(int(r.rows_rejected or 0) for r in results),
        reconciliation_status=status,
        engine=engine_type,
    )


# ---------------------------------------------------------------------------
# Airflow Variable proxy
# ---------------------------------------------------------------------------


class _AirflowVarProxy(dict):
    """BindingResolver context olarak Airflow Variable'larını lazy okur."""

    def __contains__(self, key):
        if super().__contains__(key):
            return True
        try:
            from airflow.models import Variable

            self[key] = Variable.get(key)
            return True
        except KeyError:
            return False

    def __getitem__(self, key):
        if super().__contains__(key):
            return super().__getitem__(key)
        from airflow.models import Variable

        val = Variable.get(key)
        self[key] = val
        return val


def build_airflow_variable_context() -> dict:
    """Airflow Variable'larından BindingResolver context'i oluşturur."""
    return _AirflowVarProxy()


def _extract_dag_run_conf(context: dict[str, Any]) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) if dag_run is not None else None
    return dict(conf) if isinstance(conf, dict) else {}


def coerce_dag_param_value(value: Any, param_type: str) -> Any:
    """Convert an accepted Trigger/Binding representation to its declared type."""
    if not isinstance(value, str):
        return value
    if param_type == "integer" and re.fullmatch(r"-?\d+", value):
        return int(value)
    if param_type == "number" and re.fullmatch(
        r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value
    ):
        return float(value)
    if param_type == "boolean" and value in {"true", "false"}:
        return value == "true"
    return value


def _dag_param_type(context: dict[str, Any], name: str) -> str | None:
    params = getattr(context.get("dag"), "params", None)
    get_param = getattr(params, "get_param", None)
    if not callable(get_param):
        return None
    try:
        schema = getattr(get_param(name), "schema", {})
    except KeyError:
        return None
    param_type = schema.get("x-ffengine-type") if isinstance(schema, dict) else None
    return param_type if param_type in {"string", "integer", "number", "boolean"} else None


def _normalize_dag_run_conf(
    context: dict[str, Any], dag_run_conf: dict[str, Any]
) -> dict[str, Any]:
    from airflow.sdk import Param

    normalized: dict[str, Any] = {}
    for name, original in dag_run_conf.items():
        param_type = _dag_param_type(context, name)
        if param_type is None:
            normalized[name] = original
            continue
        if original is None:
            continue
        value = coerce_dag_param_value(original, param_type)
        try:
            normalized[name] = Param(type=param_type).resolve(value)
        except Exception as exc:
            raise ConfigError(
                f"DAG parameter '{name}' cannot be normalized as {param_type}."
            ) from exc
    return normalized


def build_runtime_binding_context(
    context: dict | None = None,
    *,
    airflow_variables: dict | None = None,
    binding_task_ids: list[str] | None = None,
    binding_sources: dict[str, str] | None = None,
) -> dict[str, Any]:
    ctx = dict(context or {})
    runtime_keys = {"airflow_variables", "binding_values", "dag_run_conf"}
    if runtime_keys <= set(ctx):
        return ctx
    variables = (
        airflow_variables
        if airflow_variables is not None
        else build_airflow_variable_context()
    )
    ti = ctx.get("ti")
    if binding_sources is not None:
        binding_values = _select_compiled_binding_values(ti, binding_sources)
    else:
        binding_values = _merge_legacy_binding_values(ti, binding_task_ids)
    return {
        "airflow_variables": variables,
        "airflow_params": dict(ctx.get("params") or {}),
        "binding_values": binding_values,
        "dag_run_conf": _normalize_dag_run_conf(ctx, _extract_dag_run_conf(ctx)),
    }


_RUN_DAG_PARAM_RE = re.compile(r"\{\{\s*dag\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_RUN_SIMPLE_PARAM_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_RUN_AIRFLOW_RE = re.compile(r"\{\{\s*airflow\.([^\s{}]+)\s*\}\}")
_RUN_LEGACY_AIRFLOW_RE = re.compile(r"\{\{\s*airflow_var\.([^\s{}]+)\s*\}\}")
_MAX_RUN_CONTRACT_BYTES = 64 * 1024


def _task_template_references(task_config: dict) -> tuple[set[str], set[str]]:
    dag_names: set[str] = set()
    airflow_names: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)
        elif isinstance(value, str):
            dag_names.update(_RUN_DAG_PARAM_RE.findall(value))
            dag_names.update(_RUN_SIMPLE_PARAM_RE.findall(value))
            airflow_names.update(_RUN_AIRFLOW_RE.findall(value))
            airflow_names.update(_RUN_LEGACY_AIRFLOW_RE.findall(value))

    visit(task_config)
    local_names = {
        str(item.get("variable_name") or "").strip()
        for item in task_config.get("bindings") or []
        if isinstance(item, dict)
    }
    return dag_names - local_names, airflow_names


def task_runtime_token_names(task_config: dict) -> set[str]:
    """Task'in TÜKETTİĞİ runtime token adları (public, additive seam).

    Harici motorlar "bu task runtime değeri istiyor mu?" sorusunu kendi
    regex'leriyle cevaplamamalı — ikinci bir doğruluk kaynağı, sözleşme
    değiştiğinde sessizce ayrışır. Operatörün `uses_context` kararı da
    aynı kümeden üretilir.
    """
    dag_names, airflow_names = _task_template_references(task_config)
    return dag_names | airflow_names


def _build_external_run_contract(
    task_config: dict,
    airflow_ctx: dict,
    *,
    source_conn_id: str,
    target_conn_id: str,
) -> dict:
    dag_names, airflow_names = _task_template_references(task_config)
    contract = {
        "schema_version": 1,
        "source_conn_id": source_conn_id,
        "target_conn_id": target_conn_id,
        "binding_values": {},
        "dag_run_conf": {},
        "airflow_params": {},
        "airflow_variables": {},
    }
    for name in sorted(dag_names):
        for namespace in ("binding_values", "dag_run_conf", "airflow_params"):
            values = airflow_ctx.get(namespace) or {}
            if name in values:
                contract[namespace][name] = values[name]
                break
    variables = airflow_ctx.get("airflow_variables") or {}
    for name in sorted(airflow_names):
        if name not in variables:
            raise ConfigError(f"Airflow Variable '{name}' is unavailable.")
        contract["airflow_variables"][name] = variables[name]
    try:
        encoded = json.dumps(contract, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ConfigError("External engine run context must be JSON-serializable.") from exc
    if len(encoded.encode("utf-8")) > _MAX_RUN_CONTRACT_BYTES:
        raise ConfigError("External engine run context exceeds the 64 KiB limit.")
    return contract


def _select_compiled_binding_values(ti, binding_sources: dict[str, str]) -> dict:
    parameters_by_task: dict[str, list[str]] = {}
    for name, task_id in sorted(binding_sources.items()):
        parameters_by_task.setdefault(task_id, []).append(name)
    if parameters_by_task and ti is None:
        raise ConfigError("Task instance is required to read compiled Binding XCom.")
    selected: dict[str, Any] = {}
    for task_id, names in parameters_by_task.items():
        value = ti.xcom_pull(task_ids=task_id, key="return_value")
        if not isinstance(value, dict):
            raise ConfigError(f"Binding XCom is unavailable for task '{task_id}'.")
        missing = [name for name in names if name not in value]
        if missing:
            raise ConfigError(
                f"DAG parameter '{missing[0]}' is missing from Binding XCom "
                f"task '{task_id}'."
            )
        selected.update({name: value[name] for name in names})
    return selected


def _merge_legacy_binding_values(ti, binding_task_ids: list[str] | None) -> dict:
    merged: dict[str, Any] = {}
    for task_id in list(binding_task_ids or []):
        value = ti.xcom_pull(task_ids=task_id, key="return_value") if ti else None
        if not isinstance(value, dict):
            continue
        duplicate = sorted(set(merged) & set(value))
        if duplicate:
            raise ConfigError(
                "Conflicting compiled Binding XCom values for DAG parameter(s): "
                + ", ".join(duplicate)
            )
        merged.update(value)
    return merged


# F6.0 — operatörün `engine` argümanı bu değerle geldiğinde "verilmedi" ile
# ayırt edilemez (imza `engine: str = "auto"`; sentinel'e geçmek public API
# değişikliği olurdu). Bu durumda YAML otoritedir.
_ENGINE_ARG_AMBIGUOUS = "auto"


def _runtime_guard_context(
    context: dict[str, Any] | None = None, *, task_id: str | None = None
) -> dict[str, Any]:
    """Build the metadata-only context required by runtime guards.

    Mapped TaskFlow helpers receive a normalized binding context rather than
    Airflow's full execution context. Recover the current context lazily while
    running under Airflow; direct library/unit-test calls remain supported.
    """
    runtime_context = dict(context or {})
    metadata_keys = {"dag_run", "ti", "task", "dag_id", "run_id"}
    if not metadata_keys.intersection(runtime_context):
        try:
            from airflow.sdk import get_current_context

            runtime_context = dict(get_current_context())
        except (ImportError, RuntimeError):
            pass

    dag_run = runtime_context.get("dag_run")
    ti = runtime_context.get("ti")
    task = runtime_context.get("task")
    return {
        "dag_id": getattr(dag_run, "dag_id", None)
        or runtime_context.get("dag_id"),
        "task_id": getattr(ti, "task_id", None)
        or getattr(task, "task_id", None)
        or task_id,
        "run_id": getattr(dag_run, "run_id", None)
        or runtime_context.get("run_id"),
        "dag_run_start_date": getattr(dag_run, "start_date", None),
    }


def _resolve_engine_preference(
    task_config: dict[str, Any], operator_arg: str | None = None
) -> str:
    """F6.0 — çalışma-zamanı engine tercihi. Kök YAML tek otoritedir.

    Operatör argümanı legacy/programmatic giriş kanalıdır: varsayılandan
    farklıysa **explicit** sayılır ve YAML ile çelişirse fail-loud olur
    (sessizce yok sayılmaz). ``"auto"`` verildiğinde verilmemişten ayırt
    edilemez → YAML kazanır (bilinen sınır, plan §6 satır 5).
    """
    from ffengine.config.schema import (
        DEFAULT_ENGINE_PREFERENCE,
        ENGINE_PREFERENCE_KEY,
    )

    yaml_pref = task_config.get(ENGINE_PREFERENCE_KEY)
    arg = None if operator_arg is None else str(operator_arg).strip().lower()
    explicit_arg = arg is not None and arg != _ENGINE_ARG_AMBIGUOUS

    if yaml_pref is None:
        return arg if explicit_arg else DEFAULT_ENGINE_PREFERENCE

    yaml_value = str(yaml_pref).strip().lower()
    if explicit_arg and arg != yaml_value:
        raise ConfigError(
            f"Engine tercihi celisiyor: config engine.preference='{yaml_value}', "
            f"operator engine='{arg}'. Kok YAML tek otoritedir; operator "
            "argumani deprecated. Birini kaldirin."
        )
    return yaml_value


def _revalidate_engine_selection(task_config: dict[str, Any], preference: str) -> None:
    """F6.1 — operatör argümanı YAML'da olmayan bir tercih dayattığında motor
    config kurallarını yeniden koşar.

    `ConfigValidator._check_engine` yalnız kök `engine:` bloğunu görür ve o
    blok yoksa erken döner. `FFEngineOperator(engine="spark")` ise tercihi
    çalışma zamanında değiştirebiliyor — bu yolda knob kilidi, `submit_mode`
    zorunluluğu, edition gate ve endpoint matrisi hiç değerlendirilmezdi.
    Kural değişmez, yalnız çözülen tercih enjekte edilip aynı doğrulama
    tekrarlanır; kök YAML tek otorite olmaya devam eder.
    """
    from ffengine.config.schema import (
        DEFAULT_ENGINE_PREFERENCE,
        ENGINE_PREFERENCE_KEY,
    )
    from ffengine.config.validator import ConfigValidator

    if ENGINE_PREFERENCE_KEY in task_config:
        # YAML otoritedir ve loader doğrulamayı zaten çalıştırdı.
        return
    if preference == DEFAULT_ENGINE_PREFERENCE:
        # Argüman verilmemişten ayırt edilemez; davranış değişmedi.
        return
    effective = dict(task_config)
    effective[ENGINE_PREFERENCE_KEY] = preference
    ConfigValidator().validate_engine_selection(effective)


def _governance_event_fields(
    *,
    context: dict | None,
    event_scope: str,
    outcome: str,
    engine_type: str | None,
    task_config: dict | None,
    task_group_id: str,
    source_conn_id: str | None,
    target_conn_id: str | None,
    config_path: str | None,
    raw_file_path: str | None = None,
    raw_target_file_path: str | None = None,
    paths_rendered: bool = True,
    counter_source: str | None = None,
) -> dict[str, Any]:
    """F4.3E — kanonik `ffgov` v1 alanlarını TEK noktadan üretir.

    Mevcut summary/error payload sözlüklerine **additive** eklenir; yeni XCom
    anahtarı açılmaz. Bilinmeyen/çözülememiş bilgi ``null`` yayımlanır —
    uydurma değer yazılmaz. `config_revision` task başına tek ucuz dosya
    okumasıdır (EX-D039.6; sıcak yolda ek sorgu 0).
    """
    fields = _gov.event_header(event_scope=event_scope, outcome=outcome)
    fields.update(_gov.airflow_identity(context))
    if isinstance(task_config, dict):
        fields.update(
            _gov.dataset_identity(
                task_config,
                task_group_id=task_group_id,
                source_conn_id=source_conn_id,
                target_conn_id=target_conn_id,
                raw_file_path=raw_file_path,
                raw_target_file_path=raw_target_file_path,
                paths_rendered=paths_rendered,
            )
        )
    else:
        # Config yüklenemeden hata: dataset kimliği dürüstçe null (yalnız
        # conn_id bilinir — INV-5 gereği zaten yalnız o yayımlanır).
        fields.update(
            {
                "source_conn_id": (
                    None if source_conn_id is None else str(source_conn_id)
                ),
                "target_conn_id": (
                    None if target_conn_id is None else str(target_conn_id)
                ),
                "source_type": None,
                "target_type": None,
                "source_dataset": None,
                "source_dataset_resolved": None,
                "target_dataset": None,
                "target_dataset_resolved": None,
            }
        )
    fields["engine_type"] = engine_type
    fields["counter_source"] = counter_source
    fields["config_revision"] = _gov.config_revision_for(
        config_path, fields.get("dag_id")
    )
    return fields


def _is_standard_engine(engine: Any) -> bool:
    from ffengine.core.flow_manager import StandardEngine

    return isinstance(engine, StandardEngine)


def _engine_type_name(engine: Any, preference: str) -> str:
    """Cozulen motorun rapor adi (`engine_type`).

    `detect()` semantigi geregi deterministiktir: `auto` yalniz `pipeline`
    provider'ini yoklar, Spark'i asla secmez (EX-D021).
    """
    if _is_standard_engine(engine):
        return "standard"
    return "pipeline" if preference == "auto" else preference


def _engine_preflight(
    task_config: dict[str, Any],
    *,
    mapped_path: bool,
    operator_arg: str | None = None,
) -> tuple[Any, str]:
    """F6.0 — motor cozumu; **her turlu I/O'dan once** calisir.

    Mapped zincir (`plan_partitions -> prepare_target -> run_partition`) uc
    ayri task oldugundan, dogrulama `run_partition`'a birakilirsa
    `prepare_target` hedefte TRUNCATE/CREATE calistirdiktan SONRA fail-loud
    olurdu. Bu yuzden cagri `_resolve_task_runtime()` icinde, connection
    adapter'a dokunulmadan once yapilir.

    Returns
    -------
    (engine, engine_type)
    """
    from ffengine.core.base_engine import BaseEngine

    # F6.3 (EX-D036) — kafka+db (Track A) bir motor TERCIHI degil, kaynak
    # sozlesmesidir: dogrudan Enterprise 'cdc' provider'ina gider. Provider
    # yoksa/unavailable ise fail-loud (sessiz StandardEngine dususu YOK —
    # Standard motorda kafka okuyucusu yoktur). kafka+iceberg dali acik
    # `engine.preference: spark` ile normal preflight'tan gecer.
    source_type = str(task_config.get("source_type") or "").strip().lower()
    target_type = str(task_config.get("target_type") or "db").strip().lower()
    if source_type == "kafka" and target_type == "db":
        from ffengine.core import engine_registry

        if mapped_path:
            raise EngineError(
                "Partition'li calisma CDC (kafka) kaynaginda desteklenmiyor: "
                "siralama sozlesmesi partition-icidir ve bounded batch "
                "koordinatoru tek task'ta calisir."
            )
        provider = engine_registry.get_engine_provider("cdc")
        if provider is None:
            raise EngineError(
                "source_type='kafka' requires the Enterprise 'cdc' engine "
                "provider, but none is registered (entry point group "
                f"'{engine_registry.ENTRY_POINT_GROUP}'). Install/enable "
                "FFEngine Enterprise."
            )
        engine = provider()
        if not engine.is_available():
            raise EngineError(
                "Engine provider 'cdc' is registered but reports unavailable "
                "— check the Enterprise edition/license/confluent-kafka state."
            )
        return engine, "cdc"

    preference = _resolve_engine_preference(task_config, operator_arg)
    # F6.1: operator argumani YAML'da olmayan bir tercih dayattiysa motor
    # config kurallari burada yeniden kosar -- provider aramasindan ONCE,
    # cunku config hatasi "provider kayitli degil"den daha aksiyonabildir.
    _revalidate_engine_selection(task_config, preference)
    # Provider yok / is_available()=False -> EngineError (fail-loud).
    engine = BaseEngine.detect(preference)

    if mapped_path and not _is_standard_engine(engine):
        # B8: partition basina harici motor sozlesmesi tanimsiz (F4.1).
        # `engine.run(config_path, task_group_id)` tum task-group'u calistirir;
        # her partition'da cagrilirsa veri cogalir. Varsayim uretmeyiz.
        raise EngineError(
            f"Partition'li calisma '{_engine_type_name(engine, preference)}' "
            "motoruyla desteklenmiyor: non-Standard motorlar icin partition "
            "basina yurutme sozlesmesi tanimli degil (F4.1). "
            "`partitioning.enabled: false` yapin ya da "
            "`engine.preference: standard` kullanin."
        )
    return engine, _engine_type_name(engine, preference)


def _resolve_task_runtime(
    *,
    config_path: str,
    task_group_id: str,
    source_conn_id: str,
    target_conn_id: str,
    airflow_context: dict | None = None,
    binding_sources: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], Any, Any, Any, dict, str]:
    """Config, connection, dialect ve BindingResolver context'ini hazırlar.

    F6.0: config yuklendikten hemen sonra, **connection adapter'dan once**
    engine preflight calisir; uc mapped helper de bu tek noktadan korunur.
    Doner tuple'a 8. eleman olarak `engine_type` eklenmistir (private,
    additive).
    """
    from ffengine.config.loader import ConfigLoader
    from ffengine.config.binding_resolver import BindingResolver
    from ffengine.core.runtime_guard import run_runtime_guards
    from ffengine.db.airflow_adapter import AirflowConnectionAdapter

    run_runtime_guards(
        _runtime_guard_context(airflow_context, task_id=task_group_id)
    )
    task_config = ConfigLoader().load(config_path, task_group_id)
    # --- F6.0 preflight: buradan onceye hicbir I/O girmez ---
    _engine, engine_type = _engine_preflight(task_config, mapped_path=True)
    # F4.3E — sessiz garanti düşürme yasağı: sayaç otoritesi olmayan motor +
    # require_reconciliation=true → kaynağa bağlanmadan fail-loud (T-F4.3E-10).
    _gov.preflight_reconciliation(
        engine_type, require_reconciliation=_require_reconciliation(task_config)
    )
    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
    src_dialect = resolve_dialect(src_params["conn_type"])
    tgt_dialect = resolve_dialect(tgt_params["conn_type"])

    airflow_ctx = build_runtime_binding_context(
        airflow_context, binding_sources=binding_sources
    )
    resolver = BindingResolver()
    task_config = resolver.resolve(task_config, airflow_ctx)
    return (
        task_config,
        src_params,
        tgt_params,
        src_dialect,
        tgt_dialect,
        resolver,
        airflow_ctx,
        engine_type,
    )


def _resolve_sql_bindings_if_needed(
    *,
    task_config: dict[str, Any],
    resolver: Any,
    airflow_ctx: dict,
    source_session: Any,
    target_session: Any,
    source_dialect: Any,
) -> dict[str, Any]:
    if not task_config.get("bindings") and not task_config.get("where"):
        return task_config
    return resolver.resolve_sql_bindings(
        task_config,
        context=airflow_ctx,
        source_session=source_session,
        target_session=target_session,
        where_dialect=source_dialect,
    )


def _attach_mapping_if_needed(
    *,
    task_config: dict[str, Any],
    src_conn: Any,
    src_dialect: Any,
    tgt_dialect: Any,
) -> dict[str, Any]:
    from ffengine.mapping import MappingResolver

    mapping = MappingResolver().resolve(task_config, src_conn, src_dialect, tgt_dialect)
    effective = dict(task_config)
    effective["source_columns"] = mapping.source_columns
    effective["target_columns"] = mapping.target_columns
    effective["target_columns_meta"] = mapping.target_columns_meta
    effective["target_value_exprs"] = mapping.target_value_exprs
    effective["plain_source_by_target"] = mapping.plain_source_by_target
    return effective


def plan_partitions_for_task(
    *,
    config_path: str,
    task_group_id: str,
    source_conn_id: str,
    target_conn_id: str,
    airflow_context: dict | None = None,
    binding_sources: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Dynamic task mapping için partition spec listesini hesaplar.
    """
    from ffengine.db.session import DBSession
    from ffengine.partition import Partitioner

    (
        task_config,
        src_params,
        tgt_params,
        src_dialect,
        tgt_dialect,
        resolver,
        airflow_ctx,
        engine_type,
    ) = _resolve_task_runtime(
        config_path=config_path,
        task_group_id=task_group_id,
        source_conn_id=source_conn_id,
        target_conn_id=target_conn_id,
        airflow_context=airflow_context,
        binding_sources=binding_sources,
    )

    with DBSession(src_params, src_dialect) as src_session:
        with DBSession(tgt_params, tgt_dialect) as tgt_session:
            effective = _resolve_sql_bindings_if_needed(
                task_config=task_config,
                resolver=resolver,
                airflow_ctx=airflow_ctx,
                source_session=src_session,
                target_session=tgt_session,
                source_dialect=src_dialect,
            )
            specs = Partitioner().plan(effective, src_session.conn, src_dialect)

    return specs


def prepare_target_for_task(
    *,
    config_path: str,
    task_group_id: str,
    source_conn_id: str,
    target_conn_id: str,
    airflow_context: dict | None = None,
    binding_sources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Partition koşularından önce hedef hazırlığını bir kez yapar.
    """
    from ffengine.db.session import DBSession
    from ffengine.pipeline.target_writer import TargetWriter

    (
        task_config,
        src_params,
        tgt_params,
        src_dialect,
        tgt_dialect,
        resolver,
        airflow_ctx,
        engine_type,
    ) = _resolve_task_runtime(
        config_path=config_path,
        task_group_id=task_group_id,
        source_conn_id=source_conn_id,
        target_conn_id=target_conn_id,
        airflow_context=airflow_context,
        binding_sources=binding_sources,
    )

    with DBSession(src_params, src_dialect) as src_session:
        with DBSession(tgt_params, tgt_dialect) as tgt_session:
            effective = _resolve_sql_bindings_if_needed(
                task_config=task_config,
                resolver=resolver,
                airflow_ctx=airflow_ctx,
                source_session=src_session,
                target_session=tgt_session,
                source_dialect=src_dialect,
            )
            effective = _attach_mapping_if_needed(
                task_config=effective,
                src_conn=src_session.conn,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
            )
            writer = TargetWriter(tgt_session, tgt_dialect)
            writer.prepare(effective)

    return {"prepared": True, "task_group_id": task_group_id}


def run_partition_for_task(
    *,
    config_path: str,
    task_group_id: str,
    source_conn_id: str,
    target_conn_id: str,
    partition_spec: dict[str, Any] | None,
    airflow_context: dict | None = None,
    binding_sources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Tek bir partition spec için ETL çalıştırır (dynamic mapped task body).
    """
    from ffengine.core.flow_manager import FlowManager
    from ffengine.db.session import DBSession

    (
        task_config,
        src_params,
        tgt_params,
        src_dialect,
        tgt_dialect,
        resolver,
        airflow_ctx,
        engine_type,
    ) = _resolve_task_runtime(
        config_path=config_path,
        task_group_id=task_group_id,
        source_conn_id=source_conn_id,
        target_conn_id=target_conn_id,
        airflow_context=airflow_context,
        binding_sources=binding_sources,
    )

    with DBSession(src_params, src_dialect) as src_session:
        with DBSession(tgt_params, tgt_dialect) as tgt_session:
            effective = _resolve_sql_bindings_if_needed(
                task_config=task_config,
                resolver=resolver,
                airflow_ctx=airflow_ctx,
                source_session=src_session,
                target_session=tgt_session,
                source_dialect=src_dialect,
            )
            effective = _attach_mapping_if_needed(
                task_config=effective,
                src_conn=src_session.conn,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
            )

            base_where = effective.get("_resolved_where")
            partition_where = (partition_spec or {}).get("where")
            effective["_resolved_where"] = combine_where(base_where, partition_where)
            _log_structured(
                level=logging.INFO,
                stage="airflow",
                message="run_partition effective where resolved.",
                task_group_id=task_group_id,
                source_db=str(src_params.get("conn_type") or "unknown"),
                target_db=str(tgt_params.get("conn_type") or "unknown"),
                partition_id=(partition_spec or {}).get("part_id"),
                base_where=base_where,
                partition_where=partition_where,
                effective_where=effective.get("_resolved_where"),
                datetime_timezone="UTC",
                datetime_precision="timestamp(6)",
                datetime_boundary_policy="half_open_[lo,hi)_last_lte",
            )

            manager = FlowManager()
            result = manager.run_flow_task(
                src_session=src_session,
                tgt_session=tgt_session,
                src_dialect=src_dialect,
                tgt_dialect=tgt_dialect,
                task_config=effective,
                partition_spec=None,
                skip_prepare=True,
            )

    payload = {
        "rows": int(result.rows),
        "duration_seconds": float(result.duration_seconds),
        "throughput": float(result.throughput),
        "partitions_completed": int(result.partitions_completed),
        "errors": list(result.errors or []),
        "partition_id": (partition_spec or {}).get("part_id"),
        # F3.3 — mapped-partition yolunun muhasebe kanalı budur; bu yol
        # `rows_transferred` XCom'u push etmez (yeni anahtar da açılmaz).
        "rows_read": int(result.rows_read or 0),
        "rows_written": int(result.rows_written or 0),
        "rows_rejected": int(result.rows_rejected or 0),
        "reconciliation_status": str(result.reconciliation_status),
        # F6.0 — mapped yolda çözülen motor (bu yol Standard-only'dir;
        # non-Standard preflight'ta fail-loud olur).
        "engine_type": engine_type,
    }
    # F4.3E — kanonik `ffgov` v1 alanları mevcut payload sözlüğüne ADDITIVE
    # eklenir; mapped task event'i `event_scope=partition` + `map_index`
    # taşır (T-F4.3E-15). Yeni XCom anahtarı açılmaz.
    payload.update(
        _governance_event_fields(
            context=airflow_context,
            event_scope=_gov.EVENT_SCOPE_PARTITION,
            outcome=_gov.OUTCOME_SUCCEEDED,
            engine_type=engine_type,
            task_config=effective,
            task_group_id=task_group_id,
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            config_path=config_path,
            counter_source=_gov.resolve_counter_source(
                engine_type,
                rows_read=payload["rows_read"],
                rows_written=payload["rows_written"],
            ),
        )
    )
    return payload


def aggregate_partition_payloads(
    results: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """
    Dynamic mapped partition task sonuçlarını tek summary payload'a indirger.
    """
    payloads = list(results or [])
    flow_results: list[FlowResult] = []
    for item in payloads:
        rows = int(item.get("rows", 0))
        flow_results.append(
            FlowResult(
                rows=rows,
                duration_seconds=float(item.get("duration_seconds", 0.0)),
                throughput=float(item.get("throughput", 0.0)),
                partitions_completed=int(item.get("partitions_completed", 1)),
                errors=list(item.get("errors") or []),
                # Eski payload'lar sayaç taşımaz → FlowResult bunları
                # `rows`'tan türetir ve "legacy" olarak işaretler.
                rows_read=item.get("rows_read"),
                rows_written=item.get("rows_written"),
                rows_rejected=int(item.get("rows_rejected", 0) or 0),
                reconciliation_status=str(
                    item.get("reconciliation_status") or "legacy"
                ),
            )
        )
    # F6.0 — motor kimliği partition'lar arasında tek olmalıdır (hepsi aynı
    # task'ın parçası). Farklı değerler sessizce birleştirilmez.
    engine_type = _single_engine_type(
        (
            item.get("engine_type")
            for item in (results or [])
            if isinstance(item, dict)
        ),
        source="Partition payload'lari",
    )

    aggregated = aggregate_results(flow_results)
    summary = {
        "rows": aggregated.rows,
        "duration_seconds": aggregated.duration_seconds,
        "throughput": aggregated.throughput,
        "partitions_completed": aggregated.partitions_completed,
        "errors": aggregated.errors,
        "rows_read": aggregated.rows_read,
        "rows_written": aggregated.rows_written,
        "rows_rejected": aggregated.rows_rejected,
        "reconciliation_status": aggregated.reconciliation_status,
        # Legacy payload'lar taşımaz → None (uydurma değer yazılmaz).
        "engine_type": engine_type,
    }
    # F4.3E — aggregate task-scope event'tir; kimlik alanları partition
    # payload'larından TEK tutarlı değerse taşınır, aksi hâlde null (legacy
    # payload'lar alan taşımaz — uydurma birleştirme yapılmaz).
    summary.update(
        _gov.event_header(
            event_scope=_gov.EVENT_SCOPE_TASK, outcome=_gov.OUTCOME_SUCCEEDED
        )
    )
    for field in (
        "dag_id",
        "run_id",
        "source_conn_id",
        "target_conn_id",
        "source_type",
        "target_type",
        "source_dataset",
        "source_dataset_resolved",
        "target_dataset",
        "target_dataset_resolved",
        "counter_source",
        "config_revision",
    ):
        summary[field] = _gov.uniform_value(payloads, field)
    # Aggregate ayrı bir Airflow task'ıdır; partition'ların task_id/map_index
    # kimliği ona MAL EDİLMEZ (null bir değerdir).
    summary["task_id"] = None
    summary["map_index"] = None
    return summary


# ---------------------------------------------------------------------------
# F1.4/F1.5 — file endpoint helpers
# ---------------------------------------------------------------------------


def _render_file_paths(task_config: dict, airflow_ctx: dict) -> dict:
    """Substitute ``{{ name }}`` tokens in file_path/target_file_path (F1.5)."""
    values: dict = {}
    values.update(airflow_ctx.get("airflow_params") or {})
    values.update(airflow_ctx.get("dag_run_conf") or {})
    values.update(airflow_ctx.get("binding_values") or {})
    for key in ("file_path", "target_file_path"):
        raw = task_config.get(key)
        if isinstance(raw, str) and "{{" in raw:
            task_config[key] = _substitute_tokens(raw, values, key)
    return task_config


def _substitute_tokens(text: str, values: dict, field: str) -> str:
    import re

    def _sub(match):
        name = match.group(1).strip()
        if name not in values or values[name] is None:
            raise ConfigError(
                f"{field} sablonunda cozulemeyen deger: '{{{{ {name} }}}}'."
            )
        return str(values[name])

    return re.sub(r"\{\{\s*([A-Za-z0-9_.]+)\s*\}\}", _sub, text)


def _file_source_context(conn_id: str, params: dict, task_config: dict):
    from ffengine.pipeline.file_transport import FileSourceContext

    return FileSourceContext(
        conn_id=conn_id,
        conn_type=str(params.get("conn_type") or ""),
        file_path=str(task_config.get("file_path") or ""),
        options={
            "format": task_config.get("source_file_format"),
            "delimiter": task_config.get("delimiter"),
            "encoding": task_config.get("encoding"),
            "header": task_config.get("header", True),
            "quotechar": task_config.get("quotechar"),
            "json_mode": task_config.get("json_mode", "flat"),
        },
    )


def _file_target_context(conn_id: str, params: dict, task_config: dict):
    from ffengine.pipeline.file_transport import FileTargetContext

    return FileTargetContext(
        conn_id=conn_id,
        conn_type=str(params.get("conn_type") or ""),
        file_path=str(task_config.get("target_file_path") or ""),
        options={
            "delimiter": task_config.get("target_delimiter"),
            "encoding": task_config.get("target_encoding"),
            "header": task_config.get("target_header", True),
            # Hedef dosya formati (csv|json); verilmezse writer CSV'ye duser.
            "format": task_config.get("target_file_format"),
        },
    )


# ---------------------------------------------------------------------------
# FFEngineOperator
# ---------------------------------------------------------------------------


class FFEngineOperator(BaseOperator):
    """
    FFEngine Flow pipeline'ını Airflow ortamında orkestre eden operatör.

    3-fazlı iç orkestrasyon:
      1. plan   — config yükle, binding çöz, mapping çöz, partition planla
      2. prepare — TargetWriter.prepare() (bir kez)
      3. run    — her partition için FlowManager.run_flow_task(skip_prepare=True)

    Parameters
    ----------
    config_path      : YAML config dosya yolu.
    task_group_id    : Çalıştırılacak task kimliği.
    source_conn_id   : Airflow kaynak Connection ID.
    target_conn_id   : Airflow hedef Connection ID.
    engine           : Engine tercihi — "auto" (varsayılan) | "standard" |
                       "pipeline" | "spark". Legacy "community"/"enterprise"
                       alias'ları `DeprecationWarning` üretir. **Deprecated
                       giriş kanalı:** çalışma-zamanı otoritesi kök YAML
                       `engine.preference`'tır; bu argüman yalnız YAML'da alan
                       yokken kullanılır ve çeliştiğinde fail-loud olur (F6.0).
    airflow_context  : BindingResolver context dict (test/CLI için).
    """

    # Airflow template rendering desteği
    template_fields = (
        "config_path",
        "task_group_id",
        "source_conn_id",
        "target_conn_id",
    )

    def __init__(
        self,
        *,
        config_path: str,
        task_group_id: str,
        source_conn_id: str,
        target_conn_id: str,
        engine: str = "auto",
        airflow_context: dict | None = None,
        binding_task_ids: list[str] | None = None,
        binding_sources: dict[str, str] | None = None,
        task_id: str = "ffengine_etl",
        cdc_deferrable: bool = False,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)

        self.config_path = config_path
        self.task_group_id = task_group_id
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.engine = engine
        # F6.3 (EX-D036) — yalniz `@continuous` CDC akislarinda anlamli:
        # backlog yokken worker slotu tuketmeden deferrable trigger'da bekle.
        self.cdc_deferrable = bool(cdc_deferrable)
        self._airflow_context = airflow_context
        self.binding_sources = dict(binding_sources or {})
        source_task_ids = dict.fromkeys(self.binding_sources.values())
        self.binding_task_ids = list(binding_task_ids or source_task_ids)
        self._active_engine = None

    def execute_complete(self, context: dict | None = None, event=None) -> dict:
        """F6.3 — deferrable CDC beklemesinden donus: yeniden execute.

        Uyandiktan sonra backlog vardir ve normal yol bounded batch'i kosar;
        yaris durumunda (mesaj kaybolmadi ama baska batch tuketti — tek-yazar
        kilidi geregi ancak ayni flow'un onceki run'i olabilir) en kotu sonuc
        yeniden defer'dir, veri etkisi yoktur.
        """
        return self.execute(context)

    def execute(self, context: dict | None = None) -> dict:
        """
        3-fazlı pipeline orkestrasyon.

        Returns
        -------
        dict : Toplam sonuç (rows, duration_seconds, throughput, partitions_completed, errors).
        """
        from contextlib import ExitStack

        from ffengine.config.loader import ConfigLoader
        from ffengine.config.binding_resolver import BindingResolver
        from ffengine.config.schema import is_file_source
        from ffengine.db.airflow_adapter import AirflowConnectionAdapter
        from ffengine.db.session import DBSession
        from ffengine.mapping import MappingResolver
        from ffengine.partition import Partitioner
        from ffengine.pipeline.target_writer import TargetWriter
        from ffengine.core.flow_manager import FlowManager
        from ffengine.core.runtime_guard import run_runtime_guards

        context = context or {}
        # F2.3 runtime guard seam: no-op in Community; Enterprise registers
        # its license guard via the "ffengine.runtime_guards" entry point.
        # Runs before any real work; a raising guard stops the task
        # fail-loud. Metadata only — no DB/LDAP/network on this path.
        run_runtime_guards(
            _runtime_guard_context(context, task_id=getattr(self, "task_id", None))
        )
        airflow_ctx = build_runtime_binding_context(
            context,
            airflow_variables=self._airflow_context,
            binding_task_ids=self.binding_task_ids,
            binding_sources=self.binding_sources or None,
        )
        if "log_level" in airflow_ctx["binding_values"]:
            raise ConfigError(BUILTIN_DAG_PARAM_BINDING_ERROR)
        log_level = (
            airflow_ctx["dag_run_conf"].get("log_level")
            or airflow_ctx["airflow_params"].get("log_level")
            or "default"
        )
        if log_level not in {"default", "DEBUG"}:
            raise ConfigError("log_level must be default or DEBUG.")
        previous_log_level = _log.level
        if log_level == "DEBUG":
            _log.setLevel(logging.DEBUG)

        retry_telemetry = self._retry_telemetry(context)
        ti = context.get("ti")
        source_db = "unknown"
        target_db = "unknown"
        # F4.3E — hata yolunda dürüst (null) governance kimliği üretebilmek
        # için çözüm ilerledikçe doldurulan durum.
        engine_type = None
        gov_task_config = None
        raw_file_path = None
        raw_target_file_path = None
        # Uc host'lari: baglanti cozulmeden hata olursa dürüstce None kalir.
        src_params = None
        tgt_params = None
        try:
            _log_structured(
                level=logging.DEBUG,
                stage="airflow",
                message="Runtime parameters prepared.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                parameter_count=len(airflow_ctx["airflow_params"]),
                binding_parameter_count=len(airflow_ctx["binding_values"]),
            )
            # ---- Phase 1: PLAN ----
            _log_structured(
                level=logging.INFO,
                stage="airflow",
                message="Operator plan phase started.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                retry_telemetry=retry_telemetry,
            )

            # 1. Config yükle
            task_config = ConfigLoader().load(self.config_path, self.task_group_id)
            gov_task_config = task_config
            # F4.3E — şablonlu dosya yolları render EDİLMEDEN yakalanır:
            # `*_dataset` = şablon, `*_dataset_resolved` = render sonrası.
            raw_file_path = task_config.get("file_path")
            raw_target_file_path = task_config.get("target_file_path")

            # 1b. F6.0 — engine preflight: runtime guard'dan SONRA, herhangi
            # bir connection/DB dokunuşundan ÖNCE. Provider yok/unavailable →
            # fail-loud EngineError; hedefe hiçbir şey yazılmadan durur.
            engine, engine_type = _engine_preflight(
                task_config, mapped_path=False, operator_arg=self.engine
            )
            # 1c. F4.3E — sessiz garanti düşürme yasağı: sayaç otoritesi
            # `unavailable` + require_reconciliation=true → task başında,
            # kaynağa hiç bağlanılmadan fail-loud (T-F4.3E-10).
            _gov.preflight_reconciliation(
                engine_type,
                require_reconciliation=_require_reconciliation(task_config),
            )
            if not _is_standard_engine(engine):
                # Harici motor tüm task-group'u kendi çalıştırır (W2 sözleşmesi);
                # Standard hazırlığı (session/dialect/prepare) çalıştırılmaz.
                return self._run_external_engine(
                    engine=engine,
                    engine_type=engine_type,
                    retry_telemetry=retry_telemetry,
                    task_config=task_config,
                    airflow_ctx=airflow_ctx,
                    context=context,
                )

            # 2. Connection parametreleri
            src_params = AirflowConnectionAdapter.get_connection_params(
                self.source_conn_id
            )
            tgt_params = AirflowConnectionAdapter.get_connection_params(
                self.target_conn_id
            )
            source_db = src_params.get("conn_type", "unknown")
            target_db = tgt_params.get("conn_type", "unknown")

            # F1.4/F1.5 — file uçları DB dialect/session çözümünü atlar.
            source_is_file = is_file_source(task_config)
            target_is_file = (
                str(task_config.get("target_type") or "db").strip().lower() == "file"
            )

            # 3. Dialect çöz (yalnız DB uçları)
            src_dialect = (
                None if source_is_file else resolve_dialect(src_params["conn_type"])
            )
            tgt_dialect = (
                None if target_is_file else resolve_dialect(tgt_params["conn_type"])
            )

            # 4. Binding çöz (+ dosya yolu şablon/render)
            resolver = BindingResolver()
            task_config = resolver.resolve(task_config, airflow_ctx)
            task_config = _render_file_paths(task_config, airflow_ctx)
            gov_task_config = task_config

            # 5. Session'lar aç (yalnız DB uçları), mapping/partition/çalıştır
            with ExitStack() as stack:
                src_session = (
                    None
                    if source_is_file
                    else stack.enter_context(DBSession(src_params, src_dialect))
                )
                tgt_session = (
                    None
                    if target_is_file
                    else stack.enter_context(DBSession(tgt_params, tgt_dialect))
                )
                if not source_is_file and (
                    task_config.get("bindings") or task_config.get("where")
                ):
                    task_config = resolver.resolve_sql_bindings(
                        task_config,
                        context=airflow_ctx,
                        source_session=src_session,
                        target_session=tgt_session,
                        where_dialect=src_dialect,
                    )
                # 6. Mapping çöz (C09). Dosya hedefi kaynak kolonlarını aynen taşır.
                src_conn = None if src_session is None else src_session.conn
                mapping = MappingResolver().resolve(
                    task_config,
                    src_conn,
                    src_dialect,
                    tgt_dialect or src_dialect,
                )
                task_config["source_columns"] = mapping.source_columns
                task_config["target_columns"] = mapping.target_columns
                task_config["target_columns_meta"] = mapping.target_columns_meta
                task_config["target_value_exprs"] = mapping.target_value_exprs
                task_config["plain_source_by_target"] = mapping.plain_source_by_target

                # 7. Partition planla (dosya ucu → tek partition, M=1)
                if source_is_file or target_is_file:
                    specs = [{"part_id": 0, "where": None}]
                else:
                    specs = Partitioner().plan(
                        task_config, src_session.conn, src_dialect
                    )

                # Handle'lar: dosya uçları File*Context, DB uçları DBSession.
                src_handle = (
                    _file_source_context(self.source_conn_id, src_params, task_config)
                    if source_is_file
                    else src_session
                )
                tgt_handle = (
                    _file_target_context(self.target_conn_id, tgt_params, task_config)
                    if target_is_file
                    else tgt_session
                )

                # ---- Phase 2: PREPARE (DB hedef; dosya hedef FlowManager içinde) ----
                # Uc kimligi (conn_id + host + dataset/dosya yolu) faz loglarina
                # yazilir: "hangi sunucudan nereye" sorusu tek satirda cevaplanir.
                # Sinir (kullanici karari 2026-08-19): HOST yayimlanir; port,
                # kullanici adi ve parola YAYIMLANMAZ (AGENTS.md "no credentials").
                endpoints = _gov.dataset_identity(
                    task_config,
                    task_group_id=self.task_group_id,
                    source_conn_id=self.source_conn_id,
                    target_conn_id=self.target_conn_id,
                    raw_file_path=raw_file_path,
                    raw_target_file_path=raw_target_file_path,
                )
                endpoints["source_host"] = _endpoint_host(src_params)
                endpoints["target_host"] = _endpoint_host(tgt_params)
                _log_structured(
                    level=logging.INFO,
                    stage="airflow",
                    message="Operator prepare phase.",
                    task_group_id=self.task_group_id,
                    source_db=source_db,
                    target_db=target_db,
                    **endpoints,
                )
                if not target_is_file:
                    TargetWriter(tgt_session, tgt_dialect).prepare(task_config)

                # ---- Phase 3: RUN ----
                _log_structured(
                    level=logging.INFO,
                    stage="airflow",
                    message="Operator run phase.",
                    task_group_id=self.task_group_id,
                    source_db=source_db,
                    target_db=target_db,
                    partition_id=len(specs),
                    **endpoints,
                )
                base_where = task_config.get("_resolved_where")
                results: list[FlowResult] = []
                manager = FlowManager()

                for spec in specs:
                    effective = dict(task_config)
                    effective["_resolved_where"] = combine_where(
                        base_where, spec.get("where")
                    )

                    result = manager.run_flow_task(
                        src_session=src_handle,
                        tgt_session=tgt_handle,
                        src_dialect=src_dialect,
                        tgt_dialect=tgt_dialect,
                        task_config=effective,
                        partition_spec=None,
                        skip_prepare=not target_is_file,
                    )
                    results.append(result)

            # ---- Aggregate + XCom ----
            aggregated = aggregate_results(results)

            # XCom push (Airflow context varsa)
            if ti is not None:
                ti.xcom_push(key="rows_transferred", value=aggregated.rows)
                ti.xcom_push(key="duration_seconds", value=aggregated.duration_seconds)
                ti.xcom_push(key="rows_per_second", value=aggregated.throughput)
                ti.xcom_push(key="retry_telemetry", value=retry_telemetry)

            summary = {
                "rows": aggregated.rows,
                "duration_seconds": aggregated.duration_seconds,
                "throughput": aggregated.throughput,
                "partitions_completed": aggregated.partitions_completed,
                "errors": aggregated.errors,
                "retry_telemetry": retry_telemetry,
                # F3.3 — muhasebe alanları mevcut return_value XCom'uyla
                # taşınır; yeni XCom satırı açılmaz.
                "rows_read": aggregated.rows_read,
                "rows_written": aggregated.rows_written,
                "rows_rejected": aggregated.rows_rejected,
                "reconciliation_status": aggregated.reconciliation_status,
                # F6.0 — çözülen gerçek motor (sabit değil); aynı return_value
                # XCom'uyla taşınır, yeni anahtar açılmaz.
                "engine_type": engine_type,
            }
            # F4.3E — kanonik `ffgov` v1 alanları mevcut return_value
            # sözlüğüne ADDITIVE eklenir (yeni XCom anahtarı yok, T-F4.3E-16).
            counter_source = _gov.resolve_counter_source(
                engine_type,
                rows_read=aggregated.rows_read,
                rows_written=aggregated.rows_written,
            )
            summary.update(
                _governance_event_fields(
                    context=context,
                    event_scope=_gov.EVENT_SCOPE_TASK,
                    outcome=_gov.OUTCOME_SUCCEEDED,
                    engine_type=engine_type,
                    task_config=gov_task_config,
                    task_group_id=self.task_group_id,
                    source_conn_id=self.source_conn_id,
                    target_conn_id=self.target_conn_id,
                    config_path=self.config_path,
                    raw_file_path=raw_file_path,
                    raw_target_file_path=raw_target_file_path,
                    counter_source=counter_source,
                )
            )
            _log_structured(
                level=logging.INFO,
                stage="airflow",
                message="Operator completed.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                rows=aggregated.rows,
                duration_seconds=aggregated.duration_seconds,
                throughput=aggregated.throughput,
                delivery_semantics="best_effort",
                rows_read=aggregated.rows_read,
                rows_written=aggregated.rows_written,
                rows_rejected=aggregated.rows_rejected,
                reconciliation_status=aggregated.reconciliation_status,
                engine_type=engine_type,
                schema_version=summary["schema_version"],
                event_scope=summary["event_scope"],
                outcome=summary["outcome"],
                counter_source=summary["counter_source"],
                config_revision=summary["config_revision"],
                source_dataset=summary["source_dataset"],
                target_dataset=summary["target_dataset"],
            )
            return summary
        except Exception as exc:
            norm = normalize_exception(exc)
            payload = error_payload(norm)
            err_details = dict(payload.get("details") or {})
            payload["retry_telemetry"] = retry_telemetry
            # F4.3E — hata event'i de kanonik şemayı taşır (additive; mevcut
            # error_summary anahtarları aynen durur). ReconciliationError
            # dışında muhasebe İDDİA EDİLMEZ: `reconciliation_status` anahtarı
            # null DEĞERİYLE bulunur (T-F4.3E-12; null bir değerdir).
            from ffengine.errors.exceptions import (
                ReconciliationError,
                ReconciliationUnavailableError,
            )

            payload.update(
                _governance_event_fields(
                    context=context,
                    event_scope=_gov.EVENT_SCOPE_TASK,
                    outcome=_gov.OUTCOME_FAILED,
                    engine_type=engine_type,
                    task_config=gov_task_config,
                    task_group_id=self.task_group_id,
                    source_conn_id=self.source_conn_id,
                    target_conn_id=self.target_conn_id,
                    config_path=self.config_path,
                    raw_file_path=raw_file_path,
                    raw_target_file_path=raw_target_file_path,
                    counter_source=(
                        None
                        if engine_type is None
                        else _gov.expected_counter_source(engine_type)
                    ),
                )
            )
            # "failed" yalnız KOŞMUŞ muhasebenin tutmadığı durumdur; preflight
            # reddi (ReconciliationUnavailableError) aktarım hiç başlamadan
            # durur → dürüst değer null (review-fix MINOR-2).
            payload["reconciliation_status"] = (
                "failed"
                if isinstance(norm, ReconciliationError)
                and not isinstance(norm, ReconciliationUnavailableError)
                else None
            )
            _log_structured(
                level=logging.ERROR,
                stage="airflow",
                message="Operator failed.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                error_type=payload.get("error_type"),
                error_message=payload.get("message"),
                error_details=err_details,
                db_exception_type=err_details.get("db_exception_type"),
                db_error_message=err_details.get("db_error_message"),
                db_sqlstate=err_details.get("db_sqlstate"),
                db_driver=err_details.get("db_driver"),
                db_root_cause=err_details.get("db_root_cause"),
                sql_preview=err_details.get("sql_preview"),
                retry_telemetry=retry_telemetry,
                schema_version=payload.get("schema_version"),
                event_scope=payload.get("event_scope"),
                outcome=payload.get("outcome"),
                error_code=payload.get("error_code"),
                reconciliation_status=payload.get("reconciliation_status"),
                counter_source=payload.get("counter_source"),
                config_revision=payload.get("config_revision"),
                engine_type=engine_type,
                # Hata satirinda da uc kimligi gorunur: "hangi baglantidan
                # nereye" bilgisi olmadan izin/yol hatalarini teshis etmek
                # loglari tek tek okumayi gerektiriyordu.
                source_conn_id=payload.get("source_conn_id"),
                target_conn_id=payload.get("target_conn_id"),
                source_host=_endpoint_host(src_params),
                target_host=_endpoint_host(tgt_params),
                source_dataset=payload.get("source_dataset"),
                source_dataset_resolved=payload.get("source_dataset_resolved"),
                target_dataset=payload.get("target_dataset"),
                target_dataset_resolved=payload.get("target_dataset_resolved"),
            )
            if ti is not None:
                ti.xcom_push(key="error_summary", value=payload)
                ti.xcom_push(key="retry_telemetry", value=retry_telemetry)
            raise norm from exc
        finally:
            _log.setLevel(previous_log_level)

    def _run_external_engine(
        self,
        *,
        engine: Any,
        engine_type: str,
        retry_telemetry: dict,
        task_config: dict,
        airflow_ctx: dict,
        context: dict | None = None,
    ) -> dict:
        """F6.0 — harici (non-Standard) motor dispatch'i.

        W2 sözleşmesi: ``engine.run(config_path, task_group_id)``. Motor tüm
        task-group'u kendi yürütür; Standard hazırlığı (connection/dialect/
        mapping/partition/prepare) **çalıştırılmaz**. Bu yüzden yalnız
        partition'sız (tek task) yolda çağrılır — mapped yolda `_engine_preflight`
        zaten fail-loud olur (B8).
        """
        _log_structured(
            level=logging.INFO,
            stage="airflow",
            message="Operator delegating to external engine.",
            task_group_id=self.task_group_id,
            # Harici motor yolunda FFEngine dialect çözmez (motor kendi
            # bağlantısını kurar) — uydurma değer yazmak yerine "unknown".
            source_db="unknown",
            target_db="unknown",
            retry_telemetry=retry_telemetry,
            engine_type=engine_type,
        )
        runner = getattr(engine, "run_with_context", None)
        uses_context = bool(task_runtime_token_names(task_config))
        run_contract = _build_external_run_contract(
            task_config,
            airflow_ctx,
            source_conn_id=self.source_conn_id,
            target_conn_id=self.target_conn_id,
        )
        # F6.3 (EX-D036) — deferrable CDC beklemesi: backlog yoksa bounded
        # batch HIC baslatilmaz; motoron urettigi (secret-free) wake-up
        # trigger'iyla triggerer'da beklenir. Kancalar duck-typed'dir:
        # Community Enterprise'i import etmez; kanca yoksa davranis degismez.
        if self.cdc_deferrable and engine_type == "cdc":
            has_backlog = getattr(engine, "has_backlog", None)
            build_trigger = getattr(engine, "build_wakeup_trigger", None)
            if callable(has_backlog) and callable(build_trigger):
                if not has_backlog(
                    self.config_path, self.task_group_id, context=run_contract
                ):
                    self.defer(
                        trigger=build_trigger(
                            self.config_path,
                            self.task_group_id,
                            context=run_contract,
                        ),
                        method_name="execute_complete",
                    )
        self._active_engine = engine
        try:
            if callable(runner):
                result = runner(
                    self.config_path,
                    self.task_group_id,
                    context=run_contract,
                )
            elif uses_context:
                raise EngineError(
                    "External engine must implement run_with_context() for a task "
                    "that consumes runtime binding values."
                )
            else:
                result = engine.run(self.config_path, self.task_group_id)
        finally:
            if self._active_engine is engine:
                self._active_engine = None
        # Motor kimlik döndürmezse çözülen motorla deterministik doldurulur;
        # sabit değer yazılmaz.
        # The preflight provider identity remains authoritative for audit.
        reported_engine = getattr(result, "engine", None)
        if reported_engine is not None and reported_engine != engine_type:
            raise EngineError(
                "External engine reported an engine_type inconsistent with "
                f"preflight: resolved='{engine_type}', "
                f"reported='{reported_engine}'."
            )
        resolved = engine_type
        # F4.3E — fiili counter_source sonuç kanıtından türetilir; otoritatif
        # sayı yoksa dürüstlük: `unavailable` → sayaçlar açık null,
        # reconciliation_status = not_applicable (T-F4.3E-11). Bu dala yalnız
        # require_reconciliation=false ile gelinebilir (preflight aksi hâlde
        # task başında durdurur).
        counter_source = _gov.resolve_counter_source(
            resolved,
            rows_read=getattr(result, "rows_read", None),
            rows_written=getattr(result, "rows_written", None),
            snapshot_id=getattr(result, "snapshot_id", None),
            reconciliation_status=getattr(result, "reconciliation_status", None),
        )
        if counter_source == _gov.COUNTER_SOURCE_UNAVAILABLE:
            rows_read = None
            rows_written = None
            rows_rejected = None
            reconciliation_status = _gov.RECONCILIATION_NOT_APPLICABLE
        else:
            rows_read = result.rows_read
            rows_written = result.rows_written
            rows_rejected = result.rows_rejected
            reconciliation_status = result.reconciliation_status
        summary = {
            "rows": result.rows,
            "duration_seconds": result.duration_seconds,
            "throughput": result.throughput,
            "partitions_completed": result.partitions_completed,
            "errors": list(result.errors or []),
            "retry_telemetry": retry_telemetry,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_rejected": rows_rejected,
            "reconciliation_status": reconciliation_status,
            "engine_type": resolved,
            "application_id": result.application_id,
            "snapshot_id": result.snapshot_id,
        }
        summary.update(
            _governance_event_fields(
                context=context,
                event_scope=_gov.EVENT_SCOPE_TASK,
                outcome=_gov.OUTCOME_SUCCEEDED,
                engine_type=resolved,
                task_config=task_config,
                task_group_id=self.task_group_id,
                source_conn_id=self.source_conn_id,
                target_conn_id=self.target_conn_id,
                config_path=self.config_path,
                # Harici motor dispatch'i `_render_file_paths`'ten ÖNCEDİR:
                # dosya-yolu alanları şablon hâlindedir → dataset=şablon,
                # `*_resolved`=null (dürüstlük — review-fix MINOR-3).
                raw_file_path=task_config.get("file_path"),
                raw_target_file_path=task_config.get("target_file_path"),
                paths_rendered=False,
                counter_source=counter_source,
            )
        )
        _log_structured(
            level=logging.INFO,
            stage="airflow",
            message="Operator completed.",
            task_group_id=self.task_group_id,
            source_db="unknown",
            target_db="unknown",
            rows=result.rows,
            duration_seconds=result.duration_seconds,
            throughput=result.throughput,
            delivery_semantics="best_effort",
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            reconciliation_status=reconciliation_status,
            engine_type=resolved,
            application_id=result.application_id,
            snapshot_id=result.snapshot_id,
            schema_version=summary["schema_version"],
            event_scope=summary["event_scope"],
            outcome=summary["outcome"],
            counter_source=counter_source,
            config_revision=summary["config_revision"],
            source_dataset=summary["source_dataset"],
            target_dataset=summary["target_dataset"],
        )
        return summary

    def on_kill(self) -> None:
        """Best-effort cancellation for an active optional external engine."""
        engine = self._active_engine
        self._active_engine = None
        cancel = getattr(engine, "cancel", None)
        if not callable(cancel):
            return
        try:
            cancel()
        except Exception as exc:  # signal path must never replace task failure
            _log.warning(
                "External engine cancellation failed; error_type=%s.",
                type(exc).__name__,
            )

    def _retry_telemetry(self, context: dict) -> dict:
        """Task retry bilgilerini context'ten normalize eder."""
        ti = (context or {}).get("ti")
        if ti is None:
            return {"try_number": None, "max_tries": None}
        try_number = getattr(ti, "try_number", None)
        max_tries = getattr(ti, "max_tries", None)
        return {"try_number": try_number, "max_tries": max_tries}
