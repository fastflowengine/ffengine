"""
Flow Studio MVP service layer.

Phase 1 (T01-T04, T07, T11) and Phase 2 (T05-T10, T08-T09, T12) endpoints use this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import pprint
import re
import shutil
import stat
import threading
import time
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo, available_timezones

import yaml

from ffengine.airflow.operator import resolve_dialect
from ffengine.config.dag_param_flow import (
    compile_dag_parameter_flow,
    validate_binding_target_is_custom,
)
from ffengine.config.schema import (
    ENGINE_PREFERENCE_FIELD,
    ENGINE_PREFERENCE_KEY,
    ENGINE_SPARK_FIELD,
    ENGINE_SPARK_KEY,
    VALID_NOTIFY_TRIGGERS,
    FILE_SOURCE_TYPES,
    VALID_JSON_MODES,
)
from ffengine.config.validator import ConfigValidator
from ffengine.db.airflow_adapter import AirflowConnectionAdapter
from ffengine.db.session import DBSession
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.mapping.generator import MappingGenerator
from ffengine.mapping.resolver import VALID_MAPPING_VERSIONS, _dialect_name
from ffengine.mapping.type_contract import (
    LENGTH_BEARING_TYPES,
    NUMERIC_PARAM_TYPES,
    parse_type,
    validate_mapping_object_strict,
)

STUDIO_METADATA_NAME = ".flow_studio.json"
STUDIO_DAG_MARKER = "# generated_by: flow_studio"
STUDIO_HISTORY_DIR_NAME = ".flow_studio_history"
STUDIO_HISTORY_KEEP_LIMIT = 20
STUDIO_CUSTOM_TAG_MAX_COUNT = 10
STUDIO_CUSTOM_TAG_MAX_LENGTH = 32
STUDIO_DAG_DEPENDENCY_MAX_COUNT = 200
STUDIO_DEFAULT_START_DATE = "2023-01-01T00:00:00"
STUDIO_DEFAULT_ACTIVE = True
STUDIO_FOLDER_PATH_REQUIRED_MESSAGE = "Select a project and DAG path."
REVISION_SOURCE_CREATE_INITIAL = "create_initial"
REVISION_SOURCE_UPDATE = "update"
STUDIO_TASK_TYPE_SOURCE_TARGET = "source_target"
STUDIO_TASK_TYPE_SCRIPT_RUN = "script_run"
STUDIO_TASK_TYPE_DAG = "dag"
STUDIO_TASK_TYPE_BINDING = "binding"
STUDIO_TASK_TYPE_DBT = "dbt"
STUDIO_VALID_TASK_TYPES = {
    STUDIO_TASK_TYPE_SOURCE_TARGET,
    STUDIO_TASK_TYPE_SCRIPT_RUN,
    STUDIO_TASK_TYPE_DAG,
    STUDIO_TASK_TYPE_BINDING,
    STUDIO_TASK_TYPE_DBT,
}
# F3.2 — dbt task contract keys (the runner is Enterprise; Community owns the
# shape). Extracted narrowly so engine-field defaults never leak into the
# dbt contract validation.
_DBT_FIELD_KEYS = (
    "dbt_project_ref",
    "dbt_command",
    "dbt_select",
    "dbt_target",
    "dbt_threads",
    "dbt_vars",
    # F3.2b (Cosmos) — execution mode + cosmos-only knobs
    "dbt_execution",
    "dbt_test_behavior",
    "emit_datasets",
    # F6.4 (EX-D035) — cosmos-mode adapter/profile selector
    "dbt_target_platform",
)
STUDIO_VALID_SCRIPT_RUN_ENVIRONMENTS = {"source", "target"}
_BINDING_PARAM_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_OBSOLETE_BINDING_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")

_REVISION_DIR_RE = re.compile(r"^rev_(\d{6})$")
_DAG_LOCKS: dict[str, threading.Lock] = {}
_DAG_LOCKS_GUARD = threading.Lock()


def _slugify(value: str, default: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_").lower()
    return cleaned or default


def _require_folder_scope(payload: dict[str, Any]) -> dict[str, str]:
    scope = {
        name: str(payload.get(name) or "").strip()
        for name in ("project", "domain", "level", "flow")
    }
    if not all(scope.values()):
        raise ValueError(STUDIO_FOLDER_PATH_REQUIRED_MESSAGE)
    return scope


def _auto_task_group_id(
    source_db: str,
    src_schema: str,
    src_table: str,
    target_db: str,
    load_method: str,
    tgt_schema: str,
    tgt_table: str,
    task_index: int = 1,
) -> str:
    idx = max(1, int(task_index or 1))
    return (
        f"{idx}_{_slugify(source_db, 'source')}_{_slugify(src_schema, 'src')}_{_slugify(src_table, 'table')}"
        f"_to_{_slugify(target_db, 'target')}_{_slugify(load_method, 'method')}_{_slugify(tgt_schema, 'tgt')}_{_slugify(tgt_table, 'table')}"
    )


def _normalize_bindings(raw_bindings: Any) -> list[dict[str, Any]]:
    items = raw_bindings if isinstance(raw_bindings, list) else []
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        binding_source = str(item.get("binding_source") or "").strip()
        normalized_item = {
            "variable_name": str(item.get("variable_name") or "").strip(),
            "binding_source": binding_source,
            "default_value": str(item.get("default_value") or "").strip() or None,
            "sql": str(item.get("sql") or "").strip() or None,
            "airflow_variable_key": str(item.get("airflow_variable_key") or "").strip()
            or None,
        }
        normalized.append(normalized_item)
    return normalized


def _normalize_dag_params(raw_params: Any) -> list[dict[str, Any]]:
    if raw_params is None:
        return []
    if not isinstance(raw_params, list):
        raise ValueError("dag_params must be a list.")
    normalized: list[dict[str, Any]] = []
    for item in raw_params:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name != "log_level" and {"default", "required", "enum"} & set(item):
            raise ValueError(
                "Custom DAG parameters must not define default, required, or enum."
            )
        fields = (
            ("name", "type", "default", "required", "description", "enum")
            if name == "log_level"
            else ("name", "type", "description")
        )
        normalized.append({key: item[key] for key in fields if key in item})
    return normalized


def _normalize_upsert_match_columns(raw_columns: Any) -> list[str]:
    if raw_columns is None:
        return []
    if not isinstance(raw_columns, list):
        raise ValueError("upsert_match_columns must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_columns:
        col = str(raw or "").strip()
        if not col or col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _normalize_task_type(raw_task_type: Any) -> str:
    task_type = str(raw_task_type or STUDIO_TASK_TYPE_SOURCE_TARGET).strip().lower()
    if task_type not in STUDIO_VALID_TASK_TYPES:
        raise ValueError(
            "task_type must be source_target, script_run, dag, binding, "
            "or dbt."
        )
    return task_type


def _extract_dbt_fields(source: dict[str, Any]) -> dict[str, Any]:
    return {
        key: source.get(key)
        for key in _DBT_FIELD_KEYS
        if source.get(key) is not None
    }


def _validated_dbt_fields(source: dict[str, Any]) -> dict[str, Any]:
    """Validate + normalize dbt_* fields; Enterprise-provider gate included.

    The provider check is the backend half of the edition gate: it also
    blocks direct service calls that bypass the API (F2.1/F2.2 precedent).
    """
    from ffengine.airflow.task_type_registry import has_task_type_provider
    from ffengine.config.dbt_contract import validate_dbt_task_fields
    from ffengine.core.edition import is_enterprise_enabled

    if not is_enterprise_enabled() or not has_task_type_provider("dbt"):
        raise ValueError(
            "task_type='dbt' requires Enterprise edition and the dbt "
            "provider; both gates must be enabled."
        )
    if _normalize_bindings(source.get("bindings")):
        raise ValueError(
            "dbt tasks do not support local bindings; assign DAG parameters "
            "with an upstream 'binding' task."
        )
    return validate_dbt_task_fields(_extract_dbt_fields(source))


def _extract_binding_params(expression: Any) -> set[str]:
    text = str(expression or "")
    obsolete = _OBSOLETE_BINDING_PARAM_RE.search(text)
    if obsolete:
        name = obsolete.group(1)
        raise ValueError(
            f"Obsolete parameter syntax; replace :{name} with {{{{ {name} }}}}."
        )
    return set(_BINDING_PARAM_RE.findall(text))


def _validate_binding_contract(
    expression: Any,
    bindings: Any,
    *,
    expression_label: str,
) -> None:
    params = _extract_binding_params(expression)
    items = list(bindings or [])
    if not items:
        return
    binding_names = {
        str(item.get("variable_name") or "").strip()
        for item in items
        if isinstance(item, dict) and str(item.get("variable_name") or "").strip()
    }
    unused = sorted(binding_names - params)
    if unused:
        raise ValueError(
            f"Binding definition exists but parameter(s) are unused in {expression_label}: "
            + ", ".join(unused)
        )


def _derive_tags(project: str, domain: str, level: str, flow: str) -> list[str]:
    return [
        _slugify(project, "default_project"),
        _slugify(domain, "default_domain"),
        _slugify(level, "level1"),
        _slugify(flow, "src_to_stg"),
    ]


def _normalize_custom_tag(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = re.sub(r"[^a-z0-9_-]+", "_", raw)
    normalized = normalized.strip("_-")
    return normalized


def _normalize_custom_tags(raw_tags: Any) -> list[str]:
    if raw_tags is None:
        return []
    if not isinstance(raw_tags, list):
        raise ValueError("custom_tags must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for idx, raw in enumerate(raw_tags, start=1):
        tag = _normalize_custom_tag(raw)
        if not tag:
            continue
        if len(tag) > STUDIO_CUSTOM_TAG_MAX_LENGTH:
            raise ValueError(
                f"custom_tags[{idx-1}] length must be at most {STUDIO_CUSTOM_TAG_MAX_LENGTH}."
            )
        if tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
        if len(out) > STUDIO_CUSTOM_TAG_MAX_COUNT:
            raise ValueError(
                f"custom_tags can contain at most {STUDIO_CUSTOM_TAG_MAX_COUNT} items."
            )
    return out


def _merge_tags(auto_tags: list[str], user_tags: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in [*(auto_tags or []), *(user_tags or [])]:
        tag = str(raw or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        merged.append(tag)
    return merged


def get_airflow_default_timezone_name() -> str:
    try:
        from airflow.settings import TIMEZONE  # type: ignore

        tz_name = str(getattr(TIMEZONE, "name", "") or str(TIMEZONE) or "").strip()
        if tz_name:
            return tz_name
    except Exception:
        pass
    return "UTC"


def discover_timezones(
    search: str | None = None,
    limit: int = 200,
) -> list[str]:
    safe_limit = max(1, min(int(limit or 200), 1000))
    search_val = str(search or "").strip().lower()
    zones = sorted(available_timezones())
    if search_val:
        zones = [item for item in zones if search_val in item.lower()]
    return zones[:safe_limit]


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def _normalize_scheduler_cron(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    cron = " ".join(text.split())
    if len(cron.split()) != 5:
        raise ValueError(
            "scheduler.cron_expression must be a valid 5-field cron expression."
        )
    try:
        from croniter import croniter

        croniter(cron)
    except ImportError:
        allowed = re.compile(r"^[\d\*/,\-]+$")
        for field in cron.split():
            if field == "?":
                raise ValueError(
                    "scheduler.cron_expression must be a valid 5-field cron expression."
                )
            if not allowed.fullmatch(field):
                raise ValueError(
                    "scheduler.cron_expression must be a valid 5-field cron expression."
                )
    except Exception as exc:
        raise ValueError(
            "scheduler.cron_expression must be a valid 5-field cron expression."
        ) from exc
    return cron


def _normalize_scheduler_start_date(raw: Any, *, timezone_name: str) -> str:
    default_start = STUDIO_DEFAULT_START_DATE
    text = str(raw or "").strip()
    if not text:
        return default_start
    candidate = text.replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("scheduler.start_date must be a valid datetime.") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo(timezone_name)).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S")


_VALID_SCHEDULER_TRIGGER_TYPES = {"manual", "cron", "asset", "continuous"}


def _normalize_scheduler_assets(raw: Any) -> list[str]:
    """F3.2b — validate the asset descriptor list (v1: AND-only strings)."""
    if not isinstance(raw, list):
        raise ValueError("scheduler.assets must be a list of asset URIs.")
    assets: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if not text or any(ord(ch) < 32 or ord(ch) == 127 for ch in text):
            raise ValueError(
                "scheduler.assets entries must be non-empty asset URIs "
                "without control characters (fail-loud)."
            )
        if text in assets:
            raise ValueError(
                f"scheduler.assets contains duplicate entry {text!r}; the "
                "v1 semantics are AND over distinct assets (fail-loud)."
            )
        assets.append(text)
    return assets


def _validate_scheduler_trigger(
    trigger_type: str | None,
    assets_raw: Any,
    cron_expression: str | None,
) -> tuple[str | None, list[str] | None]:
    """F3.2b — additive trigger contract; legacy payloads stay untouched.

    Returns (trigger_type, assets) where both are None for legacy shapes so
    persisted YAML remains byte-stable for configs that never opted in.
    """
    if trigger_type is None and assets_raw is None:
        return None, None
    if trigger_type is None:
        raise ValueError(
            "scheduler.assets requires scheduler.trigger_type='asset'."
        )
    if trigger_type not in _VALID_SCHEDULER_TRIGGER_TYPES:
        raise ValueError(
            "scheduler.trigger_type must be manual, cron, asset, or "
            "continuous."
        )
    if trigger_type != "asset":
        if assets_raw not in (None, []):
            raise ValueError(
                "scheduler.assets requires scheduler.trigger_type='asset'."
            )
        if trigger_type == "cron" and not cron_expression:
            raise ValueError(
                "scheduler.trigger_type='cron' requires "
                "scheduler.cron_expression."
            )
        if trigger_type == "manual" and cron_expression:
            raise ValueError(
                "scheduler.trigger_type='manual' contradicts a "
                "cron_expression; clear one of them (fail-loud)."
            )
        if trigger_type == "continuous":
            # F6.3 (EX-D036) — @continuous CDC zinciri. Cift kapi: edition +
            # cdc engine provider (asset kapisiyla ayni disiplin).
            if cron_expression:
                raise ValueError(
                    "scheduler.trigger_type='continuous' cannot be combined "
                    "with a cron_expression (fail-loud)."
                )
            from ffengine.core.edition import is_enterprise_enabled
            from ffengine.core.engine_registry import get_engine_provider

            if not is_enterprise_enabled() or get_engine_provider("cdc") is None:
                raise ValueError(
                    "scheduler.trigger_type='continuous' requires Enterprise "
                    "edition and the CDC engine provider; both gates must be "
                    "enabled (fail-loud)."
                )
        return trigger_type, None

    # Asset-triggered scheduling is an Enterprise capability (consumer half
    # of the F3.2b Asset contract). Edition alone or provider alone is not
    # enough — same double gate as dbt tasks (v0.1.4 review lesson).
    from ffengine.airflow.task_type_registry import has_task_type_provider
    from ffengine.core.edition import is_enterprise_enabled

    if not is_enterprise_enabled() or not has_task_type_provider("dbt"):
        raise ValueError(
            "scheduler.trigger_type='asset' requires Enterprise edition "
            "and the dbt provider; both gates must be enabled."
        )
    if cron_expression:
        raise ValueError(
            "scheduler.trigger_type='asset' cannot be combined with a "
            "cron_expression; pick one trigger (fail-loud)."
        )
    assets = _normalize_scheduler_assets(assets_raw)
    if not assets:
        raise ValueError(
            "scheduler.trigger_type='asset' requires at least one entry "
            "in scheduler.assets."
        )
    return trigger_type, assets


def normalize_scheduler(raw_scheduler: Any) -> dict[str, Any]:
    if raw_scheduler is None:
        payload: dict[str, Any] = {}
    elif isinstance(raw_scheduler, dict):
        payload = dict(raw_scheduler)
    else:
        raise ValueError("scheduler must be an object.")

    timezone_name = (
        str(payload.get("timezone") or "").strip()
        or get_airflow_default_timezone_name()
    )
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError("scheduler.timezone must be a valid IANA timezone.") from exc

    cron_expression = _normalize_scheduler_cron(payload.get("cron_expression"))
    start_date = _normalize_scheduler_start_date(
        payload.get("start_date"), timezone_name=timezone_name
    )
    active = _coerce_bool(payload.get("active"), default=STUDIO_DEFAULT_ACTIVE)

    trigger_raw = payload.get("trigger_type")
    trigger_type = str(trigger_raw).strip().lower() if trigger_raw else None
    trigger_type, assets = _validate_scheduler_trigger(
        trigger_type, payload.get("assets"), cron_expression
    )

    normalized: dict[str, Any] = {
        "cron_expression": cron_expression,
        "timezone": timezone_name,
        "active": active,
        "start_date": start_date,
    }
    if trigger_type is not None:
        normalized["trigger_type"] = trigger_type
    if assets is not None:
        normalized["assets"] = assets
    return normalized


# --- F3.2b Studio slice (EX-D016): dbt Asset catalog + save-time guards ---
# The catalog is derived through the provider CAPABILITY seam (cosmos's
# public URI rule on the Enterprise side); Community only orchestrates.
# All guards run at the Flow Studio save boundary (EX-D010) — never on the
# DAG parse path or in scheduler loops.


def _dbt_asset_capability() -> Any:
    """Return the Asset-catalog capability, or None when unavailable.

    Double gate mirrors dbt tasks: Community edition alone or a registered
    provider alone never opens the catalog.
    """
    from ffengine.airflow.task_type_registry import get_task_type_capability
    from ffengine.core.edition import is_enterprise_enabled

    if not is_enterprise_enabled():
        return None
    return get_task_type_capability("dbt", "list_asset_uris")


def _iter_all_dag_configs() -> list[tuple[str, dict[str, Any]]]:
    """(dag_id, cfg) for every generated Studio DAG across all scopes.

    Unreadable entries are skipped like the DAG Explorer does — a config
    that cannot be parsed cannot be a producer or consumer either (its DAG
    would not parse); the guards stay fail-closed for consumers because a
    skipped producer only shrinks the derivable set.
    """
    out: list[tuple[str, dict[str, Any]]] = []
    gen_root = _generated_dag_root()
    if not gen_root.is_dir():
        return out
    for dag_path in gen_root.rglob("*.py"):
        if not dag_path.is_file():
            continue
        try:
            config_path = _extract_config_path_from_dag_source(dag_path)
            cfg = _load_yaml_root(config_path)
        except Exception:
            continue
        dag_id = str(dag_path.stem or "").strip()
        if dag_id and isinstance(cfg, dict):
            out.append((dag_id, cfg))
    return out


def _dbt_producer_specs(cfg: dict[str, Any]) -> set[tuple[str, str]]:
    """(project_ref, target_conn_id) for every EMITTING cosmos dbt task.

    Only ``dbt_execution: cosmos`` (the default) with ``emit_datasets``
    strictly True produces Assets at runtime (cosmos gates emission on the
    flag — source-verified); everything else contributes nothing.
    """
    from ffengine.config.dbt_contract import DEFAULT_DBT_EXECUTION

    specs: set[tuple[str, str]] = set()
    target_conn = str(cfg.get("target_db_var") or "").strip()
    if not target_conn:
        return specs
    for task in cfg.get("flow_tasks") or []:
        if not isinstance(task, dict):
            continue
        if str(task.get("task_type") or "").strip().lower() != "dbt":
            continue
        execution = (
            str(task.get("dbt_execution") or DEFAULT_DBT_EXECUTION)
            .strip()
            .lower()
        )
        if execution != "cosmos" or task.get("emit_datasets") is not True:
            continue
        ref = str(task.get("dbt_project_ref") or "").strip()
        if ref:
            specs.add((ref, target_conn))
    return specs


def _derivable_asset_uris(
    specs: set[tuple[str, str]], capability: Any
) -> set[str]:
    """Union of derivable URIs; capability errors propagate (fail-loud)."""
    uris: set[str] = set()
    for ref, conn in sorted(specs):
        for row in capability(project_ref=ref, target_conn_id=conn) or []:
            uri = str((row or {}).get("uri") or "").strip()
            if uri:
                uris.add(uri)
    return uris


def _validate_asset_consumer_membership(
    dag_id: str, assets: list[str]
) -> None:
    """Membership + stale-URI guard: every consumed URI must be derivable
    from an EMITTING cosmos dbt producer in ANOTHER DAG right now."""
    capability = _dbt_asset_capability()
    if capability is None:
        raise ValueError(
            "scheduler.assets doğrulanamıyor: dbt provider Asset katalog "
            "capability'sini sunmuyor (list_asset_uris). Eski bir provider "
            "sürümüyle asset tüketicisi kaydedilemez (fail-loud)."
        )
    specs: set[tuple[str, str]] = set()
    for other_id, cfg in _iter_all_dag_configs():
        if other_id == dag_id:
            continue
        specs |= _dbt_producer_specs(cfg)
    derivable = _derivable_asset_uris(specs, capability)
    missing = [uri for uri in assets if uri not in derivable]
    if missing:
        raise ValueError(
            "scheduler.assets, hiçbir emit eden cosmos dbt producer'ından "
            f"türetilemeyen URI'ler içeriyor: {missing}. Asset picker'daki "
            "kayıtlı URI'lerden seçin; producer'ın emit_datasets=true + "
            "cosmos modunda olması ve bundle'ının modeli içermesi gerekir "
            "(fail-loud)."
        )


def _validate_no_orphaned_asset_consumers(
    dag_id: str, new_cfg_like: dict[str, Any]
) -> None:
    """Producer-side guard: this save must not orphan any OTHER DAG's
    asset consumption (covers cosmos→task mode change, emit_datasets
    kapatma, project/target değişimi, dbt task silme ve bundle'dan model
    çıkması — hepsi tek mekanizma)."""
    consumers: list[tuple[str, list[str]]] = []
    all_cfgs = _iter_all_dag_configs()
    for other_id, cfg in all_cfgs:
        if other_id == dag_id:
            continue
        sched = cfg.get("scheduler") or {}
        if str(sched.get("trigger_type") or "").strip().lower() != "asset":
            continue
        uris = [
            str(uri).strip()
            for uri in (sched.get("assets") or [])
            if str(uri).strip()
        ]
        if uris:
            consumers.append((other_id, uris))
    if not consumers:
        return

    capability = _dbt_asset_capability()
    if capability is None:
        raise ValueError(
            "Asset tüketicisi DAG'lar mevcutken dbt Asset katalog "
            "capability'si kullanılamıyor; tüketicileri öksüz bırakma "
            "riski doğrulanamadan kayıt yapılmaz (fail-loud)."
        )
    specs: set[tuple[str, str]] = set()
    for other_id, cfg in all_cfgs:
        if other_id == dag_id:
            continue
        specs |= _dbt_producer_specs(cfg)
    specs |= _dbt_producer_specs(new_cfg_like)
    derivable = _derivable_asset_uris(specs, capability)
    for consumer_id, uris in consumers:
        orphaned = [uri for uri in uris if uri not in derivable]
        if orphaned:
            raise ValueError(
                f"Bu kayıt, '{consumer_id}' DAG'ının asset tetiklerini "
                f"öksüz bırakırdı: {orphaned}. Üretici görev cosmos "
                "modunda + emit_datasets=true kalmalı ve bundle bu "
                "modelleri içermeli; önce tüketiciyi güncelleyin "
                "(fail-loud)."
            )


def list_dbt_asset_options() -> dict[str, Any]:
    """Flow Studio Asset picker source: derivable URIs across producers.

    Broken producer bundles do not kill the picker; they surface as error
    rows in the response (visible, not silent)."""
    from ffengine.airflow.task_type_registry import has_task_type_provider
    from ffengine.core.edition import is_enterprise_enabled

    if not is_enterprise_enabled() or not has_task_type_provider("dbt"):
        raise ValueError(
            "dbt Asset kataloğu Enterprise edition ve kayıtlı dbt provider "
            "gerektirir; iki kapı birlikte açık olmalı."
        )
    capability = _dbt_asset_capability()
    if capability is None:
        raise ValueError(
            "dbt provider Asset katalog capability'sini sunmuyor "
            "(list_asset_uris)."
        )
    options: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for dag_id, cfg in _iter_all_dag_configs():
        for ref, conn in sorted(_dbt_producer_specs(cfg)):
            try:
                rows = capability(project_ref=ref, target_conn_id=conn) or []
            except Exception as exc:
                errors.append(
                    {
                        "producer_dag_id": dag_id,
                        "project_ref": ref,
                        "error": str(exc),
                    }
                )
                continue
            for row in rows:
                uri = str((row or {}).get("uri") or "").strip()
                if not uri:
                    continue
                options.append(
                    {
                        "uri": uri,
                        "model": str((row or {}).get("model") or ""),
                        "project_ref": ref,
                        "producer_dag_id": dag_id,
                    }
                )
    options.sort(key=lambda o: (o["uri"], o["producer_dag_id"]))
    return {"options": options, "errors": errors}


def _normalize_notify_triggers(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise ValueError("notifications.notify_on must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        value = str(item or "").strip().lower()
        if not value:
            continue
        if value not in VALID_NOTIFY_TRIGGERS:
            raise ValueError(
                f"Invalid notify_on trigger: '{value}'. "
                f"Valid values: {sorted(VALID_NOTIFY_TRIGGERS)}"
            )
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _normalize_notify_emails(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        candidates: list[Any] = re.split(r"[,;\s]+", raw)
    elif isinstance(raw, (list, tuple)):
        candidates = list(raw)
    else:
        raise ValueError("notifications.notify_emails must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        addr = str(item or "").strip()
        if not addr:
            continue
        if addr.count("@") != 1 or any(c.isspace() for c in addr) or "." not in addr:
            raise ValueError(f"Invalid email address: '{addr}'.")
        low = addr.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(addr)
    return out


def normalize_notifications(raw: Any) -> dict[str, Any] | None:
    """F1.3 — flow seviyesi operasyonel bildirim politikasini normalize eder.

    Bos/tanimsiz -> None (bildirim yok, geriye donuk uyumlu). Bildirim
    etkinse notify_on (failure/success), notify_emails ve notify_conn_id
    (SMTP Airflow Connection) zorunludur; gecersiz deger fail-loud.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("notifications must be an object.")

    triggers = _normalize_notify_triggers(raw.get("notify_on"))
    emails = _normalize_notify_emails(raw.get("notify_emails"))
    conn_id = str(raw.get("notify_conn_id") or "").strip()
    template = str(raw.get("notify_template") or "").strip()
    deadline_raw = raw.get("notify_deadline_minutes")
    try:
        deadline_minutes = (
            int(deadline_raw) if deadline_raw not in (None, "") else 0
        )
    except (TypeError, ValueError):
        raise ValueError(
            "notifications.notify_deadline_minutes must be an integer (minutes)."
        )

    if not triggers and not emails and not conn_id:
        return None

    if not triggers:
        raise ValueError(
            "notifications.notify_on must include at least one of: "
            f"{sorted(VALID_NOTIFY_TRIGGERS)}."
        )
    if not emails:
        raise ValueError(
            "notifications.notify_emails must include at least one recipient."
        )
    if not conn_id:
        raise ValueError(
            "notifications.notify_conn_id (SMTP Airflow Connection) is required."
        )
    if "deadline" in triggers and deadline_minutes <= 0:
        raise ValueError(
            "notifications.notify_deadline_minutes must be a positive integer "
            "(minutes) when the 'deadline' trigger is selected."
        )

    result: dict[str, Any] = {
        "notify_on": triggers,
        "notify_emails": emails,
        "notify_conn_id": conn_id,
    }
    # Default template is implicit at runtime; persist only a non-default choice
    # so existing configs stay byte-identical (backward compatible).
    if template and template != "Default":
        result["notify_template"] = template
    # Deadline minutes persisted only when the deadline trigger is on.
    if "deadline" in triggers and deadline_minutes > 0:
        result["notify_deadline_minutes"] = deadline_minutes
    return result


def normalize_file_source(item: dict[str, Any], source_type: str) -> dict[str, Any] | None:
    """F1.4/F1.5 — extract + validate csv/json file-source fields.

    Returns None for non-file sources (byte-identical DB configs). File sources
    require an explicit ``file_path``; ``json`` accepts only ``flat`` this slice.
    """
    if source_type not in FILE_SOURCE_TYPES:
        return None
    file_path = str(item.get("file_path") or "").strip()
    if not file_path:
        raise ValueError("file_path is required for a csv/json file source.")
    result: dict[str, Any] = {"file_path": file_path}
    for key in ("delimiter", "encoding", "quotechar"):
        value = str(item.get(key) or "").strip()
        if value:
            result[key] = value
    if "header" in item:
        result["header"] = bool(item.get("header"))
    if source_type == "json":
        json_mode = str(item.get("json_mode") or "flat").strip().lower()
        if json_mode not in VALID_JSON_MODES:
            raise ValueError(
                f"json_mode must be one of {sorted(VALID_JSON_MODES)} "
                "('raw' is not supported yet — F1.4b)."
            )
        result["json_mode"] = json_mode
    return result


def normalize_spark_endpoint(item: dict[str, Any]) -> dict[str, Any]:
    """F6.2 — Iceberg/parquet uc noktasinin Studio alanlarini gecirir.

    Bu alanlar `ConfigValidator`in **zorunlu** kildigi alanlardir; Studio
    tasiyicisina eklenmezlerse kullanici formu doldursa bile validator onlari
    HIC gormez ve "catalog_type zorunludur" hatasi kullanicinin duzeltemeyecegi
    bir 422'ye donusurdu.
    """
    result: dict[str, Any] = {}
    source_type = str(item.get("source_type") or "").strip().lower()
    target_type = str(item.get("target_type") or "db").strip().lower()
    if "iceberg" in (source_type, target_type):
        catalog_type = str(item.get("catalog_type") or "").strip()
        if catalog_type:
            result["catalog_type"] = catalog_type
        publish_mode = str(item.get("publish_mode") or "").strip()
        if publish_mode:
            result["publish_mode"] = publish_mode
    if source_type == "parquet":
        file_path = str(item.get("file_path") or "").strip()
        if not file_path:
            raise ValueError("file_path is required for a parquet source.")
        result["file_path"] = file_path
    if target_type == "iceberg":
        result["target_type"] = "iceberg"
    return result


def normalize_kafka_cdc(item: dict[str, Any]) -> dict[str, Any]:
    """F6.3 — kafka/CDC alanlarini gecirir (EX-D036).

    `normalize_spark_endpoint` deseni: alanlar tasiyiciya eklenmezse
    `ConfigValidator._check_kafka` onlari HIC gormez ve zorunlu-alan hatasi
    kullanicinin duzeltemeyecegi bir 422'ye donusur. Deger dogrulamasi
    ConfigValidator'da kalir (tek kanonik kaynak); burasi yalniz tasima +
    tip normalizasyonu yapar.
    """
    result: dict[str, Any] = {}
    source_type = str(item.get("source_type") or "").strip().lower()
    if source_type != "kafka":
        return result
    topic = str(item.get("kafka_topic") or "").strip()
    if topic:
        result["kafka_topic"] = topic
    policy = str(item.get("cdc_start_policy") or "").strip().lower()
    if policy:
        result["cdc_start_policy"] = policy
    offsets = item.get("cdc_start_offsets")
    if isinstance(offsets, dict) and offsets:
        try:
            result["cdc_start_offsets"] = {
                int(part): int(off) for part, off in offsets.items()
            }
        except (TypeError, ValueError):
            raise ValueError(
                "cdc_start_offsets keys/values must be integers "
                "({partition: offset})."
            )
    budget = item.get("max_batch_records")
    if isinstance(budget, int) and not isinstance(budget, bool):
        result["max_batch_records"] = budget
    return result


def normalize_file_target(item: dict[str, Any]) -> dict[str, Any] | None:
    """F1.5 — extract + validate file-target fields (target_type='file')."""
    target_type = str(item.get("target_type") or "db").strip().lower()
    if target_type != "file":
        return None
    path = str(item.get("target_file_path") or "").strip()
    if not path:
        raise ValueError("target_file_path is required when target_type='file'.")
    result: dict[str, Any] = {"target_type": "file", "target_file_path": path}
    for src_key, dst_key in (
        ("target_delimiter", "target_delimiter"),
        ("target_encoding", "target_encoding"),
    ):
        value = str(item.get(src_key) or "").strip()
        if value:
            result[dst_key] = value
    if "target_header" in item:
        result["target_header"] = bool(item.get("target_header"))
    return result


def _extract_flow_target(flow: str) -> str:
    """src_to_stg -> stg, stg_to_dwh -> dwh, fallback -> flow slug."""
    raw = _slugify(flow, "flow")
    if "_to_" in raw:
        right = raw.split("_to_")[-1].strip("_")
        if right:
            return right
    return raw


def _build_dag_filename(
    project: str,
    domain: str,
    level: str,
    flow: str,
    group_no: int,
) -> str:
    project_slug = _slugify(project, "default_project")
    domain_slug = _slugify(domain, "domain")
    level_slug = _slugify(level, "level1")
    flow_slug = _slugify(flow, "src_to_stg")
    return (
        f"{project_slug}_{domain_slug}_{level_slug}_{flow_slug}_{int(group_no)}_dag.py"
    )


def _build_yaml_filename(
    project: str,
    domain: str,
    level: str,
    flow: str,
    group_no: int,
) -> str:
    return f"{project}_{domain}_{level}_{flow}_{int(group_no)}.yaml"


def _extract_group_no_from_name(name: str) -> int | None:
    match = re.search(r"(?:_group_)?(\d+)(?:_dag)?(?:\.py|\.ya?ml)?$", name or "")
    if not match:
        return None
    try:
        value = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _next_group_no(flow_dir: Path, flow_dag_dir: Path) -> int:
    groups: set[int] = set()

    if flow_dir.is_dir():
        for item in flow_dir.glob("*.yaml"):
            g = _extract_group_no_from_name(item.name)
            if g is not None:
                groups.add(g)

    if flow_dag_dir.is_dir():
        for item in flow_dag_dir.glob("*_dag.py"):
            g = _extract_group_no_from_name(item.name)
            if g is not None:
                groups.add(g)

    return (max(groups) + 1) if groups else 1


def _projects_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_PROJECTS_ROOT", "/opt/airflow/projects")
    return Path(root)


def _generated_dag_root() -> Path:
    return Path(os.getenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags"))


def _bulk_backfill_legacy_task_types_once() -> None:
    marker = "_ffengine_task_type_backfilled_once"
    if getattr(_bulk_backfill_legacy_task_types_once, marker, False):
        return
    root = _projects_root()
    if not root.is_dir():
        setattr(_bulk_backfill_legacy_task_types_once, marker, True)
        return
    for config_path in root.rglob("*.yaml"):
        try:
            raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        tasks = raw.get("flow_tasks")
        if not isinstance(tasks, list) or not tasks:
            continue
        changed = False
        for task in tasks:
            if not isinstance(task, dict):
                continue
            if str(task.get("task_type") or "").strip():
                continue
            task["task_type"] = STUDIO_TASK_TYPE_SOURCE_TARGET
            changed = True
        if not changed:
            continue
        config_path.write_text(
            yaml.safe_dump(raw, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
    setattr(_bulk_backfill_legacy_task_types_once, marker, True)


def resolve_task_dependencies(task_defs: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    Build dependency edges for flow_tasks.
    - Uses explicit depends_on only.
    - Missing/empty depends_on means parallel execution (no implicit chain).
    """
    if not isinstance(task_defs, list):
        raise ValueError("flow_tasks must be a list.")

    task_ids: list[str] = []
    id_set: set[str] = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Each flow_task must be a dict.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("task_group_id is required for each flow_task.")
        if task_id in id_set:
            raise ValueError(
                f"Ayni task_group_id birden fazla kez kullanildi: {task_id}"
            )
        task_ids.append(task_id)
        id_set.add(task_id)

    edges: list[tuple[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for idx, task in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task.get("depends_on")
        if depends_on is None:
            depends_on = []
        if not isinstance(depends_on, list):
            raise ValueError(f"depends_on must be a list: task_group_id={task_id}")
        for dep in depends_on:
            dep_id = str(dep or "").strip()
            if not dep_id:
                continue
            if dep_id == task_id:
                raise ValueError(f"depends_on cannot reference itself: {task_id}")
            if dep_id not in id_set:
                raise ValueError(f"depends_on contains invalid task_group_id: {dep_id}")
            edge = (dep_id, task_id)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            edges.append(edge)

    # cycle kontrolu
    graph: dict[str, list[str]] = {task_id: [] for task_id in task_ids}
    for upstream, downstream in edges:
        graph[upstream].append(downstream)
    state: dict[str, int] = {}

    def _dfs(node: str) -> None:
        st = state.get(node, 0)
        if st == 1:
            raise ValueError("depends_on cycle tespit edildi.")
        if st == 2:
            return
        state[node] = 1
        for nxt in graph[node]:
            _dfs(nxt)
        state[node] = 2

    for node in task_ids:
        _dfs(node)

    return edges


def _normalize_dag_dependency_ids(raw_ids: Any) -> list[str]:
    if raw_ids is None:
        return []
    if not isinstance(raw_ids, list):
        raise ValueError("dag_dependencies.upstream_dag_ids must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for idx, raw in enumerate(raw_ids, start=1):
        dag_id = str(raw or "").strip()
        if not dag_id:
            continue
        if dag_id in seen:
            continue
        seen.add(dag_id)
        out.append(dag_id)
        if len(out) > STUDIO_DAG_DEPENDENCY_MAX_COUNT:
            raise ValueError(
                "dag_dependencies.upstream_dag_ids can contain at most "
                f"{STUDIO_DAG_DEPENDENCY_MAX_COUNT} items."
            )
    return out


def _normalize_dag_dependencies(raw_dependencies: Any) -> dict[str, Any]:
    if raw_dependencies is None:
        payload: dict[str, Any] = {}
    elif isinstance(raw_dependencies, dict):
        payload = dict(raw_dependencies)
    else:
        raise ValueError("dag_dependencies must be an object.")
    upstream_dag_ids = _normalize_dag_dependency_ids(payload.get("upstream_dag_ids"))
    return {"upstream_dag_ids": upstream_dag_ids}


def _extract_scope_from_config_path(config_path: Path) -> tuple[str, str, str, str]:
    projects_root = _projects_root().resolve()
    config_resolved = config_path.resolve()
    try:
        rel = config_resolved.relative_to(projects_root)
    except ValueError as exc:
        raise ValueError("YAML path is outside Flow Studio projects root.") from exc
    if len(rel.parts) < 5:
        raise ValueError("YAML path hierarchy is invalid.")
    project, domain, level, flow = rel.parts[:4]
    return (
        _slugify(project, "default_project"),
        _slugify(domain, "default_domain"),
        _slugify(level, "level1"),
        _slugify(flow, "src_to_stg"),
    )


def _load_yaml_root(config_path: Path) -> dict[str, Any]:
    if not config_path.is_file():
        raise FileNotFoundError(f"YAML file not found: {config_path.as_posix()}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("YAML root must be a dict.")
    return raw


def _read_dag_dependencies_from_yaml(config_path: Path) -> list[str]:
    try:
        raw = _load_yaml_root(config_path)
    except Exception:
        return []
    try:
        normalized = _normalize_dag_dependencies(raw.get("dag_dependencies"))
    except ValueError:
        return []
    return list(normalized.get("upstream_dag_ids") or [])


def _collect_scope_studio_dag_entries(
    project: str, domain: str
) -> dict[str, dict[str, Any]]:
    scope_project = _slugify(project, "default_project")
    dag_root = _generated_dag_root()
    scope_root = dag_root / scope_project
    if not scope_root.is_dir():
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for dag_path in scope_root.rglob("*.py"):
        if not dag_path.is_file():
            continue
        try:
            config_path = _extract_config_path_from_dag_source(dag_path)
        except Exception:
            continue
        if not config_path.is_file():
            continue
        try:
            cfg_project, cfg_domain, cfg_level, cfg_flow = (
                _extract_scope_from_config_path(config_path)
            )
        except Exception:
            continue
        if cfg_project != scope_project:
            continue
        dag_id = str(dag_path.stem or "").strip()
        if not dag_id:
            continue
        try:
            group_no = _extract_group_no(dag_id, config_path)
        except Exception:
            group_no = 0
        entries[dag_id] = {
            "dag_id": dag_id,
            "dag_path": dag_path,
            "config_path": config_path,
            "project": cfg_project,
            "domain": cfg_domain,
            "level": cfg_level,
            "flow": cfg_flow,
            "group_no": group_no,
            "upstream_dag_ids": _read_dag_dependencies_from_yaml(config_path),
        }
    return entries


def _build_scope_dag_graph(
    scope_entries: dict[str, dict[str, Any]],
    *,
    override_dag_id: str | None = None,
    override_upstreams: list[str] | None = None,
) -> dict[str, list[str]]:
    dag_ids = set(scope_entries.keys())
    if override_dag_id:
        dag_ids.add(str(override_dag_id).strip())
    graph: dict[str, list[str]] = {dag_id: [] for dag_id in dag_ids if dag_id}

    for dag_id in graph:
        if override_dag_id and dag_id == override_dag_id:
            upstreams = list(override_upstreams or [])
        else:
            upstreams = list(
                (scope_entries.get(dag_id) or {}).get("upstream_dag_ids") or []
            )
        for upstream in upstreams:
            if upstream not in graph:
                continue
            graph[upstream].append(dag_id)

    for upstream in list(graph.keys()):
        graph[upstream] = list(dict.fromkeys(graph[upstream]))
    return graph


def _validate_scope_dag_graph(graph: dict[str, list[str]]) -> None:
    state: dict[str, int] = {}

    def _dfs(node: str) -> None:
        marker = state.get(node, 0)
        if marker == 1:
            raise ValueError("dag_dependencies cycle detected.")
        if marker == 2:
            return
        state[node] = 1
        for nxt in graph.get(node, []):
            _dfs(nxt)
        state[node] = 2

    for dag_id in graph:
        _dfs(dag_id)


def _validate_dag_dependencies_for_scope(
    *,
    project: str,
    domain: str,
    dag_id: str,
    upstream_dag_ids: list[str],
    scope_entries: dict[str, dict[str, Any]],
) -> list[str]:
    did = str(dag_id or "").strip()
    if not did:
        raise ValueError("dag_id is required.")
    normalized = _normalize_dag_dependency_ids(upstream_dag_ids)
    if did in normalized:
        raise ValueError("dag_dependencies cannot reference itself.")

    for dep_dag_id in normalized:
        upstream_entry = scope_entries.get(dep_dag_id)
        if upstream_entry is None:
            raise ValueError(f"dag_dependencies contains invalid dag_id: {dep_dag_id}")
        if str(upstream_entry.get("project") or "") != project:
            raise ValueError(
                "dag_dependencies can only reference DAGs in the same project scope."
            )

    graph = _build_scope_dag_graph(
        scope_entries,
        override_dag_id=did,
        override_upstreams=normalized,
    )
    _validate_scope_dag_graph(graph)
    return normalized


def discover_dag_dependency_options(
    *,
    project: str,
    domain: str,
    level: str,
    flow: str,
    dag_id: str | None = None,
) -> dict[str, Any]:
    scope_project = _slugify(project, "default_project")
    scope_domain = _slugify(domain, "default_domain")
    scope_level = _slugify(level, "level1")
    scope_flow = _slugify(flow, "src_to_stg")
    current_dag_id = str(dag_id or "").strip()

    scope_entries = _collect_scope_studio_dag_entries(scope_project, scope_domain)
    current_entry = scope_entries.get(current_dag_id) if current_dag_id else None

    if current_entry is not None:
        current_group_no = int(current_entry.get("group_no") or 1)
        current_upstream_dag_ids = list(current_entry.get("upstream_dag_ids") or [])
    else:
        flow_dir = (
            _projects_root() / scope_project / scope_domain / scope_level / scope_flow
        )
        flow_dag_dir = (
            _generated_dag_root()
            / scope_project
            / scope_domain
            / scope_level
            / scope_flow
        )
        current_group_no = _next_group_no(flow_dir, flow_dag_dir)
        current_upstream_dag_ids = []

    items: list[dict[str, Any]] = []
    for entry in scope_entries.values():
        candidate_id = str(entry.get("dag_id") or "").strip()
        if not candidate_id or candidate_id == current_dag_id:
            continue
        items.append(
            {
                "dag_id": candidate_id,
                "project": str(entry.get("project") or ""),
                "domain": str(entry.get("domain") or ""),
                "level": str(entry.get("level") or ""),
                "flow": str(entry.get("flow") or ""),
                "group_no": int(entry.get("group_no") or 0),
            }
        )
    items.sort(
        key=lambda row: (
            str(row.get("dag_id") or "").strip().lower(),
            str(row.get("dag_id") or "").strip(),
        )
    )

    referenced_by = sorted(
        [
            str(entry.get("dag_id") or "")
            for entry in scope_entries.values()
            if current_dag_id
            and current_dag_id in list(entry.get("upstream_dag_ids") or [])
        ]
    )

    return {
        "project": scope_project,
        "domain": scope_domain,
        "level": scope_level,
        "flow": scope_flow,
        "dag_id": current_dag_id,
        "group_no": int(current_group_no),
        "current_upstream_dag_ids": current_upstream_dag_ids,
        "referenced_by": referenced_by,
        "items": items,
        "count": len(items),
    }


def _render_single_studio_dag_entry(entry: dict[str, Any]) -> None:
    dag_id = str(entry.get("dag_id") or "").strip()
    dag_path = entry.get("dag_path")
    config_path = entry.get("config_path")
    if (
        not dag_id
        or not isinstance(dag_path, Path)
        or not isinstance(config_path, Path)
    ):
        raise ValueError("Invalid studio DAG entry.")
    cfg = _load_yaml_root(config_path)
    user_tags = _normalize_custom_tags(cfg.get("custom_tags"))
    tags = _merge_tags(
        _derive_tags(
            str(entry.get("project") or ""),
            str(entry.get("domain") or ""),
            str(entry.get("level") or ""),
            str(entry.get("flow") or ""),
        ),
        user_tags,
    )
    dag_source = _render_group_dag_source(
        dag_id=dag_id,
        config_path=config_path,
        tags=tags,
        upstream_dag_ids=list(entry.get("upstream_dag_ids") or []),
        raw_config=cfg,
    )
    if not dag_path.is_file() or dag_path.read_text(encoding="utf-8") != dag_source:
        dag_path.write_text(dag_source, encoding="utf-8")


def _render_group_dag_source(
    *,
    dag_id: str,
    config_path: Path,
    tags: list[str],
    upstream_dag_ids: list[str] | None = None,
    raw_config: dict[str, Any] | None = None,
) -> str:
    cfg = json.dumps(config_path.as_posix())
    did = json.dumps(dag_id)
    dtags = json.dumps(tags)
    upstream_ids = json.dumps(list(dict.fromkeys(upstream_dag_ids or [])))
    snapshot = dict(raw_config or {})
    snapshot["__config_path"] = config_path.as_posix()
    raw_literal = pprint.pformat(snapshot, width=100, sort_dicts=False)
    return f"""{STUDIO_DAG_MARKER}
from pathlib import Path

from ffengine.airflow.generated_factory import build_generated_dag

CONFIG_PATH = Path({cfg})
DAG_ID = {did}
DAG_TAGS = {dtags}
UPSTREAM_DAG_IDS = {upstream_ids}
RAW_CONFIG = {raw_literal}

dag = build_generated_dag(
    dag_id=DAG_ID,
    dag_tags=DAG_TAGS,
    upstream_dag_ids=UPSTREAM_DAG_IDS,
    raw_config_snapshot=RAW_CONFIG,
)
"""


def _ensure_path_under_root(path: Path, root: Path) -> Path:
    """Path traversal guard: path must stay under root."""
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"Invalid path: {path!s}") from exc
    return resolved


def _best_effort_unlink(
    path: Path, *, retries: int = 80, wait_seconds: float = 0.1
) -> bool:
    for _ in range(max(1, retries)):
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return True
        except PermissionError:
            try:
                path.chmod(stat.S_IWRITE | stat.S_IREAD)
            except OSError:
                pass
            time.sleep(max(0.0, wait_seconds))
        except OSError:
            time.sleep(max(0.0, wait_seconds))
    for idx in range(1, max(2, retries + 1)):
        tomb = path.with_name(f"{path.name}.stale_{idx}")
        if tomb.exists():
            continue
        try:
            path.replace(tomb)
            return True
        except OSError:
            continue
    return False


def _best_effort_rmtree(path: Path) -> bool:
    if not path.exists():
        return True
    if not path.is_dir():
        return False

    def _onerror(func, raw_path, _exc_info):
        try:
            os.chmod(raw_path, stat.S_IWRITE | stat.S_IREAD)
        except OSError:
            pass
        try:
            func(raw_path)
        except OSError:
            pass

    try:
        shutil.rmtree(path, onerror=_onerror)
        return True
    except OSError:
        try:
            shutil.rmtree(path, ignore_errors=True)
        except OSError:
            return False
    return not path.exists()


def _normalize_relative_mapping_file(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    raw = re.sub(r"/{2,}", "/", raw).lstrip("/")
    if not raw:
        raise ValueError("mapping_file cannot be empty.")
    path = Path(raw)
    if path.is_absolute():
        raise ValueError("mapping_file must be a relative path.")
    return Path(raw).as_posix()


def _auto_mapping_relative_file(task_no: int, task_group_id: str) -> str:
    safe_task_no = max(1, int(task_no))
    tg = str(task_group_id or "").strip()
    if not tg:
        raise ValueError("task_group_id cannot be empty.")
    if "/" in tg or "\\" in tg or ".." in tg:
        raise ValueError(f"Invalid task_group_id (for mapping path): {tg!r}")
    return f"mapping/{safe_task_no}_{tg}.yaml"


def _is_auto_mapping_relative_file(value: str) -> bool:
    rel = str(value or "").strip().replace("\\", "/")
    return bool(re.fullmatch(r"mapping/\d+_[^/\\]+\.ya?ml", rel))


def _resolve_mapping_file_path(flow_dir: Path, mapping_file: str) -> Path:
    rel = _normalize_relative_mapping_file(mapping_file)
    target = flow_dir / rel
    return _ensure_path_under_root(target, flow_dir)


def _mapping_yaml_to_source_columns(mapping_obj: dict[str, Any]) -> list[str]:
    from ffengine.mapping import expression as _expr

    validated = validate_mapping_object_strict(
        mapping_obj,
        valid_versions=VALID_MAPPING_VERSIONS,
        error_cls=ValueError,
        context="flow_studio",
    )
    entries = validated.get("columns") or []
    out: list[str] = []
    for idx, item in enumerate(entries, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Mapping columns[{idx-1}] must be a dict.")
        expr_text = str(item.get("expression") or "").strip()
        if expr_text:
            # v1.1 derived column: source columns come from expression refs;
            # parsing also validates expression syntax at save time.
            try:
                ast = _expr.parse(expr_text)
            except Exception as exc:
                raise ValueError(
                    f"Mapping columns[{idx-1}] expression invalid: {exc}"
                ) from exc
            out.extend(_expr.column_refs(ast))
            continue
        src = str(item.get("source_name") or "").strip()
        if not src:
            raise ValueError(f"Mapping columns[{idx-1}] source_name cannot be empty.")
        out.append(src)
    return list(dict.fromkeys(out))


def _parse_yaml_mapping_text(mapping_content: str, *, label: str) -> dict[str, Any]:
    try:
        parsed = yaml.safe_load(mapping_content)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid mapping YAML ({label}): {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid mapping YAML ({label} ): root must be a dict.")
    validate_mapping_object_strict(
        parsed,
        valid_versions=VALID_MAPPING_VERSIONS,
        error_cls=ValueError,
        context=label,
    )
    return parsed


def parse_mapping_columns(mapping_content: str) -> dict[str, Any]:
    """
    Lenient parse of a mapping YAML into structured columns for the Flow Studio
    row editor. No strict validation here (that runs on save) so the user can
    load and repair any mapping in the structured editor.
    """
    try:
        parsed = yaml.safe_load(mapping_content or "")
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid mapping YAML: {exc}") from exc
    if parsed is None:
        return {"version": "v1.1", "columns": []}
    if not isinstance(parsed, dict):
        raise ValueError("Invalid mapping YAML: root must be a dict.")
    cols = parsed.get("columns")
    return {
        "version": str(parsed.get("version") or "v1.1"),
        "columns": cols if isinstance(cols, list) else [],
    }


def _read_mapping_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Mapping file not found: {path.as_posix()}")
    return _parse_yaml_mapping_text(
        path.read_text(encoding="utf-8"), label=path.as_posix()
    )


_POSTGRES_OID_TYPE_NAMES: dict[str, str] = {
    "16": "BOOLEAN",
    "20": "BIGINT",
    "21": "SMALLINT",
    "23": "INTEGER",
    "25": "TEXT",
    "700": "REAL",
    "701": "DOUBLE PRECISION",
    "1042": "CHAR",
    "1043": "VARCHAR",
    "1082": "DATE",
    "1083": "TIME",
    "1114": "TIMESTAMP",
    "1184": "TIMESTAMP WITH TIME ZONE",
    "1700": "NUMERIC",
    "2950": "UUID",
    "3802": "JSONB",
}


def _normalize_description_type(
    type_code: Any,
    type_display: Any | None = None,
) -> str:
    if type_display is not None:
        display = str(type_display or "").strip().upper()
        if display:
            return display
    if type_code is None:
        return "TEXT"
    if isinstance(type_code, str):
        raw = type_code
    elif hasattr(type_code, "__name__"):
        raw = str(getattr(type_code, "__name__", ""))
    else:
        raw = str(type_code)
    mapped = _POSTGRES_OID_TYPE_NAMES.get(raw.strip())
    if mapped:
        return mapped
    cleaned = re.sub(r"[^A-Za-z0-9_ ]+", "_", raw).strip("_ ").upper()
    return cleaned or "TEXT"


def _description_value(col: Any, attr_name: str, index: int) -> Any:
    if hasattr(col, attr_name):
        return getattr(col, attr_name)
    try:
        return col[index] if len(col) > index else None
    except TypeError:
        return None


def _as_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _as_non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _parse_param_pair(params: str | None) -> tuple[int | None, int | None]:
    if not params:
        return None, None
    parts = [p.strip() for p in str(params).split(",")]
    precision = _as_positive_int(parts[0]) if parts else None
    scale = _as_non_negative_int(parts[1]) if len(parts) > 1 else None
    return precision, scale


def _normalize_target_type_for_strict(
    *,
    target_type: str,
    source_type: str,
    source_meta: dict[str, Any],
    same_dialect: bool = False,
) -> str:
    target_base, target_params = parse_type(target_type)
    if target_params is not None:
        return str(target_type or "").strip().upper()

    source_base, source_params = parse_type(source_type)
    src_param_precision, src_param_scale = _parse_param_pair(source_params)
    src_meta_length = _as_positive_int(source_meta.get("source_length"))
    src_meta_precision = _as_positive_int(source_meta.get("source_precision"))
    src_meta_scale = _as_non_negative_int(source_meta.get("source_scale"))

    if target_base in LENGTH_BEARING_TYPES:
        explicit_length = src_meta_length or src_meta_precision
        if explicit_length is None and source_base in LENGTH_BEARING_TYPES:
            explicit_length = src_param_precision
        if explicit_length is not None and explicit_length > 0:
            return f"{target_base}({explicit_length})"
        # No resolvable length. "No size" is a deliberate "max size" choice:
        # same dialect -> lossless bare passthrough; different dialect -> leave
        # blank for the developer to fill (Apply/Save block on empty).
        return target_base if same_dialect else ""

    if target_base in NUMERIC_PARAM_TYPES:
        precision = src_meta_precision or src_param_precision
        scale = src_meta_scale if src_meta_scale is not None else src_param_scale
        if precision is not None and precision > 0:
            if scale is not None and scale >= 0:
                return f"{target_base}({precision},{scale})"
            return f"{target_base}({precision})"
        # Unsized numeric: same dialect -> bare passthrough (max size, lossless);
        # different dialect -> blank (no guessed precision/scale).
        return target_base if same_dialect else ""

    return target_base


# CASE/fonksiyon iceren ifade kolonlarinda surucu uzunluk bildirmeyebilir;
# taslak mapping bu genislikle uretilir, kullanici Mapping Editor'de degistirir.
DEFAULT_EXPRESSION_VARCHAR_LENGTH = 1000


def _wrap_zero_row_sql_for_dialect(inline_sql: str, dialect_name: str) -> str:
    base = str(inline_sql or "").strip().rstrip(";")
    if not base:
        raise ValueError("inline_sql is required when source_type='sql'.")
    if dialect_name == "mssql":
        return f"SELECT TOP 0 * FROM ({base}) AS ffengine_inline_sql"
    if dialect_name == "oracle":
        return f"SELECT * FROM ({base}) ffengine_inline_sql WHERE 1=0"
    return f"SELECT * FROM ({base}) AS ffengine_inline_sql LIMIT 0"


def _normalize_mssql_system_type(
    *,
    system_type_name: Any,
    max_length: Any,
    precision: Any,
    scale: Any,
    column_name: str,
) -> str:
    base, params = parse_type(str(system_type_name or ""))
    if not base:
        raise ValueError(
            f"SQL metadata extraction failed: '{column_name}' column type is empty."
        )

    if base in LENGTH_BEARING_TYPES:
        if params:
            normalized_param = str(params).strip().upper()
            if normalized_param == "MAX":
                return base
            parsed_len = _as_positive_int(normalized_param)
            if parsed_len is not None:
                return f"{base}({parsed_len})"
        length = _as_positive_int(max_length) or _as_positive_int(precision)
        if length is not None:
            return f"{base}({length})"
        raise ValueError(
            f"SQL metadata extraction failed: '{column_name}' length could not be resolved."
        )

    if base in NUMERIC_PARAM_TYPES:
        if params:
            p, s = _parse_param_pair(params)
            if p is not None:
                if s is not None:
                    return f"{base}({p},{s})"
                return f"{base}({p})"
        p = _as_positive_int(precision)
        s = _as_non_negative_int(scale)
        if p is not None:
            if s is not None:
                return f"{base}({p},{s})"
            return f"{base}({p})"
        raise ValueError(
            f"SQL metadata extraction failed: '{column_name}' precision could not be resolved."
        )

    return base


def _extract_sql_select_columns_mssql(
    src_session: DBSession, inline_sql: str
) -> list[dict[str, Any]]:
    cursor = src_session.cursor(server_side=False)
    query = """
        SELECT column_ordinal,
               name,
               system_type_name,
               is_nullable,
               max_length,
               precision,
               scale,
               error_number,
               error_message
        FROM sys.dm_exec_describe_first_result_set(?, NULL, 0)
        ORDER BY column_ordinal
    """
    try:
        cursor.execute(query, (inline_sql,))
        rows = list(cursor.fetchall() or [])
    except Exception as exc:
        raise ValueError(f"SQL metadata extraction failed: {exc}") from exc
    finally:
        cursor.close()

    if not rows:
        raise ValueError("No columns found during SQL metadata extraction.")

    out: list[dict[str, Any]] = []
    for row in rows:
        error_number = row[7] if len(row) > 7 else None
        error_message = row[8] if len(row) > 8 else None
        if error_number not in (None, 0):
            raise ValueError(
                "SQL metadata extraction failed: "
                f"{error_message or f'error_number={error_number}'}"
            )

        name = str(row[1] if len(row) > 1 else "").strip()
        if not name:
            continue
        source_type = _normalize_mssql_system_type(
            system_type_name=(row[2] if len(row) > 2 else None),
            max_length=(row[4] if len(row) > 4 else None),
            precision=(row[5] if len(row) > 5 else None),
            scale=(row[6] if len(row) > 6 else None),
            column_name=name,
        )
        out.append(
            {
                "name": name,
                "source_type": source_type,
                "nullable": bool(row[3]) if len(row) > 3 else True,
                "source_length": _as_positive_int(row[4] if len(row) > 4 else None),
                "source_precision": _as_positive_int(row[5] if len(row) > 5 else None),
                "source_scale": _as_non_negative_int(row[6] if len(row) > 6 else None),
            }
        )

    if not out:
        raise ValueError("No columns found during SQL metadata extraction.")
    return out


def extract_sql_select_columns(
    src_session: DBSession, src_dialect, inline_sql: str
) -> list[dict[str, Any]]:
    """Extract column names and normalized type names from SQL query metadata."""
    dialect_name = _dialect_name(src_dialect)
    if dialect_name == "mssql":
        return _extract_sql_select_columns_mssql(src_session, inline_sql)

    query = _wrap_zero_row_sql_for_dialect(inline_sql, dialect_name)
    cursor = src_session.cursor(server_side=False)
    try:
        cursor.execute(query)
        desc = list(cursor.description or [])
    except Exception as exc:
        raise ValueError(f"SQL metadata extraction failed: {exc}") from exc
    finally:
        cursor.close()
    cols: list[dict[str, str]] = []
    for col in desc:
        name = str(_description_value(col, "name", 0) or "").strip()
        if not name:
            continue
        type_code = _description_value(col, "type_code", 1)
        type_display = getattr(col, "type_display", None)
        source_type = _normalize_description_type(type_code, type_display)
        source_precision = _as_positive_int(_description_value(col, "precision", 4))
        source_scale = _as_non_negative_int(_description_value(col, "scale", 5))
        source_length = _as_positive_int(_description_value(col, "display_size", 2))

        if source_type in {"STR", "STRING", "UNICODE"}:
            if source_length is None:
                # CASE/fonksiyon gibi ifade kolonlarinda surucu uzunluk
                # bildirmez; taslakta guvenli genislikte VARCHAR verilir,
                # kullanici Mapping Editor'de daraltabilir.
                source_length = DEFAULT_EXPRESSION_VARCHAR_LENGTH
            source_type = f"VARCHAR({source_length})"
        elif source_type in {"BYTES", "BYTEARRAY", "MEMORYVIEW"}:
            if source_length is None:
                raise ValueError(
                    f"SQL metadata extraction failed: '{name}' length could not be resolved."
                )
            source_type = f"VARBINARY({source_length})"
        elif source_type in NUMERIC_PARAM_TYPES:
            # Lenient draft: an unparameterized numeric (e.g. Postgres bare
            # `numeric`) keeps its bare base type here. The developer sets an
            # explicit precision/scale in the Mapping Editor; strict validation
            # enforces it at Apply/Save, not at scaffold (generate) time.
            if source_precision is not None:
                if source_scale is None:
                    source_type = f"{source_type}({source_precision})"
                else:
                    source_type = f"{source_type}({source_precision},{source_scale})"
        elif source_type in LENGTH_BEARING_TYPES:
            if source_length is None:
                # Uzunluk tasiyan string tipte de ayni taslak varsayilani.
                source_length = DEFAULT_EXPRESSION_VARCHAR_LENGTH
            source_type = f"{source_type}({source_length})"
        elif source_type == "INT":
            source_type = "INTEGER"

        cols.append(
            {
                "name": name,
                "source_type": source_type,
                "nullable": True,
                "source_length": source_length,
                "source_precision": source_precision,
                "source_scale": source_scale,
            }
        )
    if not cols:
        raise ValueError("No columns found during SQL metadata extraction.")
    return cols


def extract_sql_select_columns_for_conn(
    source_conn_id: str, inline_sql: str
) -> list[dict[str, str]]:
    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    src_dialect = resolve_dialect(src_params["conn_type"])
    with DBSession(src_params, src_dialect) as src_session:
        return extract_sql_select_columns(src_session, src_dialect, inline_sql)


def _collect_existing_auto_mapping_paths(
    config_path: Path, flow_dir: Path
) -> set[Path]:
    if not config_path.is_file():
        return set()
    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return set()
    tasks = cfg.get("flow_tasks")
    if not isinstance(tasks, list):
        return set()
    out: set[Path] = set()
    for task in tasks:
        if not isinstance(task, dict):
            continue
        rel = str(task.get("mapping_file") or "").strip()
        if not _is_auto_mapping_relative_file(rel):
            continue
        try:
            out.add(_resolve_mapping_file_path(flow_dir, rel))
        except Exception:
            continue
    return out


def _build_mapping_from_columns(
    *,
    columns: list[dict[str, Any]],
    src_dialect_name: str,
    tgt_dialect_name: str,
    version: str = "v1",
    strict: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    if version not in VALID_MAPPING_VERSIONS:
        raise ValueError(
            f"Invalid mapping version: {version!r}. "
            f"Gecerli: {sorted(VALID_MAPPING_VERSIONS)}"
        )
    same_dialect = bool(src_dialect_name) and (
        str(src_dialect_name).strip().lower() == str(tgt_dialect_name).strip().lower()
    )
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    for col in columns:
        src_name = str(col.get("name") or "").strip()
        src_type = str(col.get("source_type") or "").strip().upper()
        if not src_name:
            continue
        if not src_type:
            raise ValueError(
                "SQL metadata extraction failed: "
                f"column '{src_name}' has empty source_type."
            )
        try:
            tgt_type = TypeMapper.map_type(src_type, src_dialect_name, tgt_dialect_name)
        except UnsupportedTypeError as exc:
            if strict:
                raise ValueError(
                    "Mapping generation failed: "
                    f"column '{src_name}' source_type '{src_type}' cannot be mapped "
                    f"from '{src_dialect_name}' to '{tgt_dialect_name}'. "
                    "Regenerate mapping with explicit source metadata."
                ) from exc
            # Scaffold: same dialect -> lossless identity copy of the source
            # type; different dialect -> leave blank for the developer to fill.
            tgt_type = src_type if same_dialect else ""
            if not same_dialect:
                warnings.append(
                    f"column '{src_name}' (source '{src_type}') could not be "
                    "auto-mapped - set a target Data Type before Apply."
                )
            rows.append(
                {
                    "source_name": src_name,
                    "target_name": src_name,
                    "source_type": src_type,
                    "target_type": tgt_type,
                    "nullable": bool(col.get("nullable", True)),
                }
            )
            continue
        tgt_type = _normalize_target_type_for_strict(
            target_type=tgt_type,
            source_type=src_type,
            source_meta=col,
            same_dialect=same_dialect,
        )
        if not tgt_type:
            # Cross-dialect unsized type: blanked for the developer to fill.
            warnings.append(
                f"column '{src_name}' (source '{src_type}') has no explicit size; "
                "set an explicit target Data Type before Apply."
            )
        rows.append(
            {
                "source_name": src_name,
                "target_name": src_name,
                "source_type": src_type,
                "target_type": tgt_type,
                "nullable": bool(col.get("nullable", True)),
            }
        )
    if not rows:
        raise ValueError("No usable columns found for mapping generation.")
    mapping_obj = {
        "version": version,
        "source_dialect": src_dialect_name,
        "target_dialect": tgt_dialect_name,
        "columns": rows,
    }
    if strict:
        validate_mapping_object_strict(
            mapping_obj,
            valid_versions=VALID_MAPPING_VERSIONS,
            error_cls=ValueError,
            context="mapping/generate",
        )
    return mapping_obj, warnings


def _incomplete_type_warnings(columns: list[dict[str, Any]]) -> list[str]:
    """Draft columns FFEngine could not fill (blank target_type).

    A blank target_type means the source type had no explicit size and the
    source/target Connection Types differ (or the type is not cross-mappable),
    so FFEngine left it for the developer to complete. Not an error at scaffold
    (generate) time — strict validation enforces a non-empty type at Apply/Save.
    A bare numeric/length target on a same-dialect mapping is a valid "max size"
    passthrough and is intentionally NOT flagged.
    """
    out: list[str] = []
    for col in columns or []:
        tgt_name = str(col.get("target_name") or col.get("name") or "").strip()
        tgt_type = str(col.get("target_type") or "").strip()
        if tgt_type:
            continue
        src_type = str(col.get("source_type") or "").strip()
        detail = f" (source '{src_type}')" if src_type else ""
        out.append(
            f"column '{tgt_name}'{detail} could not be auto-mapped - "
            "set a target Data Type before Apply."
        )
    return out


def _mapping_dump_text(mapping_obj: dict[str, Any]) -> str:
    return yaml.safe_dump(mapping_obj, sort_keys=False, allow_unicode=True)


def _resolve_save_dialect_names(
    payload: dict[str, Any]
) -> tuple[str | None, str | None]:
    """Resolve the authoritative source/target dialect names from the DAG's
    connections. These drive the same-vs-cross "max size" rule at Save. Any
    failure -> (None, None) so strict validation is preserved unchanged."""
    try:
        source_conn_id = str(payload.get("source_conn_id") or "").strip()
        target_conn_id = str(payload.get("target_conn_id") or "").strip()
        if not source_conn_id or not target_conn_id:
            return None, None
        src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
        tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
        src_name = _dialect_name(resolve_dialect(src_params["conn_type"]))
        tgt_name = _dialect_name(resolve_dialect(tgt_params["conn_type"]))
        return src_name, tgt_name
    except Exception:
        return None, None


def _stamp_mapping_dialects(
    mapping_content: str, *, source_dialect: str, target_dialect: str
) -> str:
    """Stamp the authoritative source/target dialects into a mapping YAML.

    The Flow Studio row editor serializes mappings without top-level
    source_dialect/target_dialect, so the same-dialect "max size" waiver in
    validate_mapping_object_strict cannot fire (and the persisted file would not
    be self-describing at runtime). Here the backend injects the real Connection
    Types (from the DAG's connections) - overwriting any client value - before
    validation and before the file is written. Malformed/non-dict content is
    returned unchanged so the normal gate raises the proper shape/YAML error.
    """
    try:
        parsed = yaml.safe_load(mapping_content)
    except yaml.YAMLError:
        return mapping_content
    if not isinstance(parsed, dict):
        return mapping_content
    ordered: dict[str, Any] = {"version": parsed.get("version")}
    ordered["source_dialect"] = source_dialect
    ordered["target_dialect"] = target_dialect
    for key, value in parsed.items():
        if key not in ("version", "source_dialect", "target_dialect"):
            ordered[key] = value
    return _mapping_dump_text(ordered)


def _generate_mapping_content_for_task(
    *,
    source_conn_id: str,
    target_conn_id: str,
    task: dict[str, Any],
    task_no: int,
) -> str:
    source_type = str(task.get("source_type") or "table").strip() or "table"
    task_group_id = (
        str(task.get("task_group_id") or "").strip() or f"task_{max(1, int(task_no))}"
    )

    preview_payload: dict[str, Any] = {
        "source_conn_id": str(source_conn_id or "").strip(),
        "target_conn_id": str(target_conn_id or "").strip(),
        "source_type": source_type,
        "task_no": max(1, int(task_no)),
        "task_group_id": task_group_id,
        "version": "v1.1",
    }
    if source_type in {"table", "view"}:
        preview_payload["source_schema"] = str(task.get("source_schema") or "").strip()
        preview_payload["source_table"] = str(task.get("source_table") or "").strip()
    elif source_type == "sql":
        preview_payload["inline_sql"] = str(task.get("inline_sql") or "").strip()
    elif source_type in FILE_SOURCE_TYPES:
        preview_payload["file_path"] = str(task.get("file_path") or "").strip()
        for key in ("delimiter", "encoding", "quotechar", "header", "json_mode"):
            if task.get(key) is not None:
                preview_payload[key] = task.get(key)

    preview = generate_mapping_preview(preview_payload)
    mapping_content = str(preview.get("mapping_content") or "")
    if not mapping_content.strip():
        raise ValueError("Generated mapping_content is empty.")
    _parse_yaml_mapping_text(mapping_content, label=f"task_group_id={task_group_id}")
    return mapping_content if mapping_content.endswith("\n") else f"{mapping_content}\n"


def _semantic_yaml_equal(left_text: str, right_text: str) -> bool:
    try:
        left_obj = yaml.safe_load(left_text) if left_text.strip() else None
        right_obj = yaml.safe_load(right_text) if right_text.strip() else None
    except yaml.YAMLError:
        return False
    return left_obj == right_obj


def _load_studio_metadata(flow_dir: Path) -> dict[str, Any] | None:
    meta_path = flow_dir / STUDIO_METADATA_NAME
    if not meta_path.is_file():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw not in {"", "0", "false", "no", "off"}


def _history_keep_limit() -> int:
    raw = str(
        os.getenv("FFENGINE_STUDIO_HISTORY_KEEP_LIMIT", str(STUDIO_HISTORY_KEEP_LIMIT))
    ).strip()
    try:
        value = int(raw)
    except ValueError:
        value = STUDIO_HISTORY_KEEP_LIMIT
    return max(1, value)


def _revision_history_root(flow_dir: Path, dag_id: str) -> Path:
    return flow_dir / STUDIO_HISTORY_DIR_NAME / str(dag_id or "").strip()


def _revision_dirs_sorted(history_root: Path) -> list[Path]:
    if not history_root.is_dir():
        return []
    items: list[tuple[int, Path]] = []
    for item in history_root.iterdir():
        if not item.is_dir():
            continue
        m = _REVISION_DIR_RE.fullmatch(item.name)
        if not m:
            continue
        try:
            seq = int(m.group(1))
        except ValueError:
            continue
        items.append((seq, item))
    items.sort(key=lambda x: x[0])
    return [x[1] for x in items]


def _next_revision_id(history_root: Path) -> str:
    dirs = _revision_dirs_sorted(history_root)
    if not dirs:
        return "rev_000001"
    last = dirs[-1].name
    m = _REVISION_DIR_RE.fullmatch(last)
    if not m:
        return "rev_000001"
    return f"rev_{(int(m.group(1)) + 1):06d}"


def _prune_revision_history(history_root: Path, keep_limit: int) -> None:
    dirs = _revision_dirs_sorted(history_root)
    stale = dirs[: -max(1, keep_limit)]
    for item in stale:

        def _onerror(func, path, _exc_info):
            try:
                os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            except OSError:
                pass
            try:
                func(path)
            except OSError:
                pass

        try:
            shutil.rmtree(item, onerror=_onerror)
        except OSError:
            shutil.rmtree(item, ignore_errors=True)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _bundle_hash_from_texts(
    *,
    dag_text: str,
    config_text: str,
    mapping_texts: dict[str, str] | None = None,
) -> str:
    file_hashes: dict[str, str] = {
        "dag.py": _sha256_text(dag_text),
        "config.yaml": _sha256_text(config_text),
    }
    for rel, text in sorted(dict(mapping_texts or {}).items()):
        file_hashes[str(rel)] = _sha256_text(str(text or ""))
    return _sha256_text(json.dumps(file_hashes, sort_keys=True))


def _bundle_hash_from_loaded_bundle(bundle: dict[str, Any]) -> str:
    return _bundle_hash_from_texts(
        dag_text=str(bundle.get("dag_text") or ""),
        config_text=str(bundle.get("config_text") or ""),
        mapping_texts=dict(bundle.get("mapping_texts") or {}),
    )


def _auto_mapping_rel_paths_from_config_obj(config_obj: dict[str, Any]) -> list[str]:
    out: list[str] = []
    tasks = config_obj.get("flow_tasks") if isinstance(config_obj, dict) else None
    if not isinstance(tasks, list):
        return out
    for task in tasks:
        if not isinstance(task, dict):
            continue
        rel = str(task.get("mapping_file") or "").strip()
        if not _is_auto_mapping_relative_file(rel):
            continue
        out.append(_normalize_relative_mapping_file(rel))
    return sorted(set(out))


def _read_active_bundle(
    dag_path: Path, config_path: Path, flow_dir: Path
) -> dict[str, Any]:
    if not dag_path.is_file():
        raise FileNotFoundError(f"DAG file not found: {dag_path.as_posix()}")
    if not config_path.is_file():
        raise FileNotFoundError(f"YAML file not found: {config_path.as_posix()}")

    dag_text = dag_path.read_text(encoding="utf-8")
    config_text = config_path.read_text(encoding="utf-8")
    config_obj = yaml.safe_load(config_text) or {}
    if not isinstance(config_obj, dict):
        raise ValueError("YAML root must be a dict.")

    mapping_texts: dict[str, str] = {}
    for rel in _auto_mapping_rel_paths_from_config_obj(config_obj):
        path = _resolve_mapping_file_path(flow_dir, rel)
        if not path.is_file():
            continue
        mapping_texts[rel] = path.read_text(encoding="utf-8")

    file_hashes: dict[str, str] = {
        "dag.py": _sha256_text(dag_text),
        "config.yaml": _sha256_text(config_text),
    }
    for rel in sorted(mapping_texts):
        file_hashes[rel] = _sha256_text(mapping_texts[rel])
    bundle_hash = _bundle_hash_from_texts(
        dag_text=dag_text,
        config_text=config_text,
        mapping_texts=mapping_texts,
    )
    file_hashes["bundle"] = bundle_hash

    return {
        "dag_text": dag_text,
        "config_text": config_text,
        "config_obj": config_obj,
        "mapping_texts": mapping_texts,
        "hashes": file_hashes,
    }


def _active_bundle_hash_or_empty(
    dag_path: Path, config_path: Path, flow_dir: Path
) -> str:
    try:
        bundle = _read_active_bundle(dag_path, config_path, flow_dir)
    except Exception:
        return ""
    return str((bundle.get("hashes") or {}).get("bundle") or "")


def _save_bundle_as_revision(
    *,
    flow_dir: Path,
    dag_id: str,
    dag_path: Path,
    config_path: Path,
    source: str,
    actor: str,
) -> dict[str, Any]:
    bundle = _read_active_bundle(dag_path, config_path, flow_dir)
    history_root = _revision_history_root(flow_dir, dag_id)
    history_root.mkdir(parents=True, exist_ok=True)
    revision_id = _next_revision_id(history_root)
    revision_dir = history_root / revision_id
    revision_dir.mkdir(parents=True, exist_ok=True)

    (revision_dir / "dag.py").write_text(bundle["dag_text"], encoding="utf-8")
    (revision_dir / "config.yaml").write_text(bundle["config_text"], encoding="utf-8")
    for rel, text in bundle["mapping_texts"].items():
        target = revision_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    manifest = {
        "revision_id": revision_id,
        "dag_id": dag_id,
        "created_at": _utc_now_iso(),
        "source": source,
        "actor": actor,
        "hashes": bundle["hashes"],
        "mapping_files": sorted(bundle["mapping_texts"].keys()),
    }
    (revision_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _prune_revision_history(history_root, _history_keep_limit())
    return manifest


def _load_bundle_from_revision(revision_dir: Path) -> dict[str, Any]:
    manifest_path = revision_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Revision manifest not found: {manifest_path.as_posix()}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dag_file = revision_dir / "dag.py"
    cfg_file = revision_dir / "config.yaml"
    if not dag_file.is_file() or not cfg_file.is_file():
        raise FileNotFoundError("dag.py or config.yaml is missing in revision.")
    mapping_texts: dict[str, str] = {}
    for rel in manifest.get("mapping_files") or []:
        rel_path = _normalize_relative_mapping_file(str(rel or ""))
        src = revision_dir / rel_path
        if not src.is_file():
            continue
        mapping_texts[rel_path] = src.read_text(encoding="utf-8")
    return {
        "manifest": manifest,
        "dag_text": dag_file.read_text(encoding="utf-8"),
        "config_text": cfg_file.read_text(encoding="utf-8"),
        "mapping_texts": mapping_texts,
    }


def _write_studio_metadata(flow_dir: Path, metadata: dict[str, Any]) -> None:
    (flow_dir / STUDIO_METADATA_NAME).write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )


def _list_revision_items(
    history_root: Path, *, limit: int | None = None
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for revision_dir in reversed(_revision_dirs_sorted(history_root)):
        manifest_path = revision_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        out.append(
            {
                "revision_id": str(manifest.get("revision_id") or revision_dir.name),
                "created_at": str(manifest.get("created_at") or ""),
                "source": str(manifest.get("source") or ""),
                "actor": str(manifest.get("actor") or ""),
                "bundle_hash": str((manifest.get("hashes") or {}).get("bundle") or ""),
            }
        )
        if isinstance(limit, int) and limit > 0 and len(out) >= limit:
            break
    return out


def _resolve_active_revision_id(
    *,
    history_root: Path,
    dag_path: Path,
    config_path: Path,
    flow_dir: Path,
) -> str | None:
    if not history_root.is_dir():
        return None
    try:
        active = _read_active_bundle(dag_path, config_path, flow_dir)
    except Exception:
        return None
    bundle_hash = str((active.get("hashes") or {}).get("bundle") or "")
    if not bundle_hash:
        return None
    for revision_dir in reversed(_revision_dirs_sorted(history_root)):
        manifest_path = revision_dir / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        revision_id = (
            str(manifest.get("revision_id") or revision_dir.name).strip()
            or revision_dir.name
        )
        manifest_bundle_hash = str((manifest.get("hashes") or {}).get("bundle") or "")
        if manifest_bundle_hash and manifest_bundle_hash == bundle_hash:
            return revision_id
        try:
            bundle = _load_bundle_from_revision(revision_dir)
            recalculated_bundle_hash = _bundle_hash_from_loaded_bundle(bundle)
        except Exception:
            continue
        if recalculated_bundle_hash and recalculated_bundle_hash == bundle_hash:
            return revision_id
    return None


@contextmanager
def _dag_operation_lock(dag_id: str):
    did = str(dag_id or "").strip()
    if not did:
        yield
        return
    with _DAG_LOCKS_GUARD:
        lock = _DAG_LOCKS.get(did)
        if lock is None:
            lock = threading.Lock()
            _DAG_LOCKS[did] = lock
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _extract_group_no(dag_id: str, config_path: Path) -> int:
    match = re.search(r"(?:_group_)?(\d+)_dag$", dag_id)
    if match:
        return int(match.group(1))
    cfg_match = re.search(r"(?:_group_)?(\d+)\.ya?ml$", config_path.name)
    if cfg_match:
        return int(cfg_match.group(1))
    raise ValueError("group_no could not be resolved from dag_id/config name.")


def _extract_config_path_from_dag_source(dag_path: Path) -> Path:
    source = dag_path.read_text(encoding="utf-8")
    if STUDIO_DAG_MARKER not in source:
        raise ValueError("Bu DAG Flow Studio tarafindan uretilmemis.")
    match = re.search(
        r"CONFIG_PATH\s*=\s*Path\((['\"])(?P<path>.+?)\1\)",
        source,
    )
    if not match:
        raise ValueError("CONFIG_PATH could not be resolved inside DAG.")
    return Path(match.group("path"))


def _find_studio_dag_file_by_id(dag_id: str) -> Path | None:
    gen_root = _generated_dag_root()
    candidate_name = f"{dag_id}.py"
    for path in gen_root.rglob(candidate_name):
        if path.is_file():
            return path
    return None


def _load_mapping_content_for_task(flow_dir: Path, task: dict[str, Any]) -> str | None:
    mode = str(task.get("column_mapping_mode") or "source").strip()
    mapping_file = str(task.get("mapping_file") or "").strip()
    if mode != "mapping_file" or not mapping_file:
        return None
    mapping_path = _resolve_mapping_file_path(flow_dir, mapping_file)
    if not mapping_path.is_file():
        return None
    return mapping_path.read_text(encoding="utf-8")


def resolve_dag_config_for_update(dag_id: str) -> dict[str, Any]:
    _bulk_backfill_legacy_task_types_once()
    did = (dag_id or "").strip()
    if not did:
        raise ValueError("dag_id is required.")

    dag_path = _find_studio_dag_file_by_id(did)
    if dag_path is None:
        raise FileNotFoundError(f"DAG not found: {did}")

    config_path = _extract_config_path_from_dag_source(dag_path)
    if not config_path.is_file():
        raise ValueError("DAG was found but linked YAML file was not found.")

    projects_root = _projects_root().resolve()
    config_resolved = config_path.resolve()
    try:
        rel = config_resolved.relative_to(projects_root)
    except ValueError as exc:
        raise ValueError("YAML path Flow Studio projects root altinda degil.") from exc
    if len(rel.parts) < 5:
        raise ValueError("YAML path hierarchy is invalid.")
    project, domain, level, flow = rel.parts[:4]

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("YAML root must be a dict.")
    tasks = raw.get("flow_tasks") or []
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("YAML flow_tasks list is empty or invalid.")
    normalized_tasks: list[dict[str, Any]] = []
    for idx, task in enumerate(tasks, start=1):
        if not isinstance(task, dict):
            raise ValueError(f"flow_tasks[{idx-1}] must be a dict.")
        partitioning = task.get("partitioning") or {}
        if not isinstance(partitioning, dict):
            partitioning = {}
        normalized_tasks.append(
            {
                "task_group_id": str(task.get("task_group_id") or "").strip() or None,
                "depends_on": [
                    str(dep or "").strip()
                    for dep in list(task.get("depends_on") or [])
                    if str(dep or "").strip()
                ],
                "task_type": _normalize_task_type(task.get("task_type")),
                "source_schema": str(task.get("source_schema") or "").strip(),
                "source_table": str(task.get("source_table") or "").strip(),
                "source_type": str(task.get("source_type") or "table").strip()
                or "table",
                "inline_sql": str(task.get("inline_sql") or "").strip() or None,
                "script_run_environment": str(
                    task.get("script_run_environment") or ""
                ).strip()
                or None,
                "script_sql": str(task.get("script_sql") or "").strip() or None,
                "dag_task_dag_id": str(task.get("dag_task_dag_id") or "").strip()
                or None,
                "target_schema": str(task.get("target_schema") or "").strip(),
                "target_table": str(task.get("target_table") or "").strip(),
                "load_method": (
                    str(
                        task.get("load_method") or "create_if_not_exists_or_truncate"
                    ).strip()
                    or "create_if_not_exists_or_truncate"
                ),
                "upsert_match_columns": _normalize_upsert_match_columns(
                    task.get("upsert_match_columns")
                )
                or None,
                "column_mapping_mode": (
                    str(task.get("column_mapping_mode") or "source").strip() or "source"
                ),
                "mapping_file": str(task.get("mapping_file") or "").strip() or None,
                "mapping_content": _load_mapping_content_for_task(
                    config_resolved.parent, task
                ),
                "where": str(task.get("where") or "").strip() or None,
                "batch_size": int(task.get("batch_size") or 10000),
                "use_bulk_api": _coerce_bool(task.get("use_bulk_api"), default=False),
                "bulk_api_method": str(task.get("bulk_api_method") or "").strip()
                or None,
                "partitioning_enabled": bool(partitioning.get("enabled", False)),
                "partitioning_mode": str(
                    partitioning.get("mode") or "auto_numeric"
                ).strip()
                or "auto_numeric",
                "partitioning_column": str(partitioning.get("column") or "").strip()
                or None,
                "partitioning_parts": int(partitioning.get("parts") or 2),
                "partitioning_distinct_limit": int(
                    partitioning.get("distinct_limit") or 16
                ),
                "partitioning_ranges": partitioning.get("ranges") or [],
                "bindings": _normalize_bindings(task.get("bindings")),
                # F1.4/F1.5 — file source/target fields (preload for edit).
                "file_path": str(task.get("file_path") or "").strip() or None,
                "delimiter": task.get("delimiter"),
                "encoding": task.get("encoding"),
                "quotechar": task.get("quotechar"),
                "header": task.get("header"),
                "json_mode": task.get("json_mode"),
                "target_type": str(task.get("target_type") or "db").strip() or "db",
                "target_file_path": str(task.get("target_file_path") or "").strip()
                or None,
                "target_delimiter": task.get("target_delimiter"),
                "target_encoding": task.get("target_encoding"),
                "target_header": task.get("target_header"),
                # F3.2 — dbt fields (preload for edit). This dict is an
                # EXPLICIT key list: any persisted dbt key missing here is
                # silently dropped on the next resave (round-trip trap).
                "dbt_project_ref": str(task.get("dbt_project_ref") or "").strip()
                or None,
                "dbt_command": str(task.get("dbt_command") or "").strip() or None,
                "dbt_select": str(task.get("dbt_select") or "").strip() or None,
                "dbt_target": str(task.get("dbt_target") or "").strip() or None,
                "dbt_threads": (
                    task.get("dbt_threads")
                    if isinstance(task.get("dbt_threads"), int)
                    and not isinstance(task.get("dbt_threads"), bool)
                    else None
                ),
                "dbt_vars": (
                    dict(task.get("dbt_vars"))
                    if isinstance(task.get("dbt_vars"), dict)
                    and task.get("dbt_vars")
                    else None
                ),
                # F3.2b (Cosmos) — same explicit-key round-trip contract.
                "dbt_execution": str(task.get("dbt_execution") or "").strip()
                or None,
                "dbt_test_behavior": str(
                    task.get("dbt_test_behavior") or ""
                ).strip()
                or None,
                "emit_datasets": (
                    task.get("emit_datasets")
                    if isinstance(task.get("emit_datasets"), bool)
                    else None
                ),
                # F6.3 — kafka/CDC alanlari (ayni explicit-key round-trip
                # sozlesmesi: burada olmayan anahtar resave'de sessizce duser).
                "kafka_topic": str(task.get("kafka_topic") or "").strip() or None,
                "cdc_start_policy": str(
                    task.get("cdc_start_policy") or ""
                ).strip()
                or None,
                "cdc_start_offsets": (
                    dict(task.get("cdc_start_offsets"))
                    if isinstance(task.get("cdc_start_offsets"), dict)
                    and task.get("cdc_start_offsets")
                    else None
                ),
                "max_batch_records": (
                    task.get("max_batch_records")
                    if isinstance(task.get("max_batch_records"), int)
                    and not isinstance(task.get("max_batch_records"), bool)
                    else None
                ),
            }
        )

    first_task = normalized_tasks[0]
    custom_tags = _normalize_custom_tags(raw.get("custom_tags"))
    scheduler = normalize_scheduler(raw.get("scheduler"))
    dag_dependencies = _normalize_dag_dependencies(raw.get("dag_dependencies"))
    dag_params = _normalize_dag_params(raw.get("dag_params"))
    notifications = normalize_notifications(raw.get("notifications"))

    payload = {
        "project": project,
        "domain": domain,
        "level": level,
        "flow": flow,
        "custom_tags": custom_tags,
        "scheduler": scheduler,
        "dag_dependencies": dag_dependencies,
        "dag_params": dag_params,
        "notifications": notifications,
        "group_no": _extract_group_no(did, config_path),
        "task_group_id": first_task["task_group_id"],
        "task_type": first_task["task_type"],
        "source_conn_id": str(raw.get("source_db_var") or "").strip(),
        "target_conn_id": str(raw.get("target_db_var") or "").strip(),
        "source_schema": first_task["source_schema"],
        "source_table": first_task["source_table"],
        "source_type": first_task["source_type"],
        "inline_sql": first_task["inline_sql"],
        "script_run_environment": first_task["script_run_environment"],
        "script_sql": first_task["script_sql"],
        "dag_task_dag_id": first_task["dag_task_dag_id"],
        "target_schema": first_task["target_schema"],
        "target_table": first_task["target_table"],
        "load_method": first_task["load_method"],
        "upsert_match_columns": first_task["upsert_match_columns"],
        "column_mapping_mode": first_task["column_mapping_mode"],
        "mapping_file": first_task["mapping_file"],
        "mapping_content": first_task["mapping_content"],
        "where": first_task["where"],
        "batch_size": first_task["batch_size"],
        "use_bulk_api": first_task["use_bulk_api"],
        "bulk_api_method": first_task["bulk_api_method"],
        "partitioning_enabled": first_task["partitioning_enabled"],
        "partitioning_mode": first_task["partitioning_mode"],
        "partitioning_column": first_task["partitioning_column"],
        "partitioning_parts": first_task["partitioning_parts"],
        "partitioning_distinct_limit": first_task["partitioning_distinct_limit"],
        "partitioning_ranges": first_task["partitioning_ranges"],
        "bindings": first_task["bindings"],
        "file_path": first_task["file_path"],
        "delimiter": first_task["delimiter"],
        "encoding": first_task["encoding"],
        "quotechar": first_task["quotechar"],
        "header": first_task["header"],
        "json_mode": first_task["json_mode"],
        "target_type": first_task["target_type"],
        "target_file_path": first_task["target_file_path"],
        "target_delimiter": first_task["target_delimiter"],
        "target_encoding": first_task["target_encoding"],
        "target_header": first_task["target_header"],
        "kafka_topic": first_task["kafka_topic"],
        "cdc_start_policy": first_task["cdc_start_policy"],
        "cdc_start_offsets": first_task["cdc_start_offsets"],
        "max_batch_records": first_task["max_batch_records"],
        "flow_tasks": normalized_tasks,
    }
    if isinstance(raw.get("engine"), dict):
        payload["engine"] = dict(raw["engine"])

    return {
        "dag_id": did,
        "payload": payload,
        "dag_path": dag_path.as_posix(),
        "config_path": config_path.as_posix(),
        "active_revision_id": _resolve_active_revision_id(
            history_root=_revision_history_root(config_resolved.parent, did),
            dag_path=dag_path,
            config_path=config_resolved,
            flow_dir=config_resolved.parent,
        ),
        "revision_count": len(
            _list_revision_items(
                _revision_history_root(config_resolved.parent, did),
                limit=_history_keep_limit(),
            )
        ),
    }


def _sync_dag_paused_state(dag_id: str, *, active: bool) -> str | None:
    did = str(dag_id or "").strip()
    if not did:
        return "dag_id is empty; pause state sync skipped."
    try:
        from airflow.utils.session import create_session
    except Exception:
        return "Airflow session is unavailable; pause state sync skipped."

    DagModel = None
    for module_name in ("airflow.models.dag", "airflow.models.dagmodel"):
        try:
            module = __import__(module_name, fromlist=["DagModel"])
            DagModel = getattr(module, "DagModel", None)
            if DagModel is not None:
                break
        except Exception:
            continue
    if DagModel is None:
        return "DagModel is unavailable; pause state sync skipped."

    target_paused = not bool(active)
    try:
        with create_session() as session:
            row = session.query(DagModel).filter(DagModel.dag_id == did).one_or_none()
            if row is None:
                return f"DagModel not found for dag_id={did}; pause state sync skipped."
            setattr(row, "is_paused", target_paused)
            try:
                if hasattr(row, "set_is_paused"):
                    row.set_is_paused(target_paused)
            except Exception:
                # attribute assignment above is enough for most Airflow versions
                pass
            session.flush()
            session.commit()
    except Exception as exc:
        return f"DagModel pause state sync failed: {exc}"
    return None


def _airflow_parse_state(dag_id: str) -> dict[str, Any] | None:
    """Airflow metadata uzerinden parse/version durumunu best-effort okur."""
    try:
        from airflow.models.dag_version import DagVersion
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session
    except Exception:
        return None

    try:
        with create_session() as session:
            dag_ver = (
                session.query(DagVersion)
                .filter(DagVersion.dag_id == dag_id)
                .order_by(DagVersion.created_at.desc())
                .first()
            )
            ser = (
                session.query(SerializedDagModel)
                .filter(SerializedDagModel.dag_id == dag_id)
                .order_by(SerializedDagModel.created_at.desc())
                .first()
            )
    except Exception:
        return None

    if dag_ver is None and ser is None:
        return None

    return {
        "dag_version_id": str(getattr(dag_ver, "id", "") or ""),
        "version_number": int(getattr(dag_ver, "version_number", 0) or 0),
        "dag_hash": str(getattr(ser, "dag_hash", "") or ""),
        "serialized_last_updated": str(getattr(ser, "last_updated", "") or ""),
    }


def _parse_state_changed(
    before: dict[str, Any] | None, after: dict[str, Any] | None
) -> bool:
    if before is None:
        return after is not None
    if after is None:
        return False
    if str(after.get("dag_version_id") or "") != str(
        before.get("dag_version_id") or ""
    ):
        return True
    if str(after.get("dag_hash") or "") != str(before.get("dag_hash") or ""):
        return True
    if str(after.get("serialized_last_updated") or "") != str(
        before.get("serialized_last_updated") or ""
    ):
        return True
    if int(after.get("version_number") or 0) > int(before.get("version_number") or 0):
        return True
    return False


def _wait_for_parse_refresh(dag_id: str, before_state: dict[str, Any] | None) -> bool:
    if not _env_bool("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", True):
        return True
    timeout_seconds_raw = str(
        os.getenv("FFENGINE_STUDIO_PROMOTE_VERIFY_TIMEOUT_SECONDS", "60")
    ).strip()
    interval_seconds_raw = str(
        os.getenv("FFENGINE_STUDIO_PROMOTE_VERIFY_INTERVAL_SECONDS", "1")
    ).strip()
    try:
        timeout_seconds = max(2.0, float(timeout_seconds_raw))
    except ValueError:
        timeout_seconds = 60.0
    try:
        interval_seconds = max(0.2, float(interval_seconds_raw))
    except ValueError:
        interval_seconds = 1.0
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = _airflow_parse_state(dag_id)
        if _parse_state_changed(before_state, current):
            return True
        time.sleep(interval_seconds)
    return False


def _import_airflow_model(candidates: list[tuple[str, str]]) -> type | None:
    for module_name, class_name in candidates:
        try:
            module = __import__(module_name, fromlist=[class_name])
            model = getattr(module, class_name, None)
            if model is not None:
                return model
        except Exception:
            continue
    return None


def _cleanup_airflow_dag_metadata(dag_id: str) -> dict[str, Any]:
    did = str(dag_id or "").strip()
    if not did:
        return {
            "ok": False,
            "details": {},
            "warnings": ["Metadata cleanup skipped because dag_id is empty."],
        }

    try:
        from airflow.utils.session import create_session
    except Exception as exc:
        return {
            "ok": False,
            "details": {},
            "warnings": [f"Airflow DB session acilamadi: {exc}"],
        }

    model_specs: list[tuple[str, list[tuple[str, str]]]] = [
        ("task_instances", [("airflow.models.taskinstance", "TaskInstance")]),
        ("task_reschedules", [("airflow.models.taskreschedule", "TaskReschedule")]),
        ("task_fails", [("airflow.models.taskfail", "TaskFail")]),
        # In Airflow 3, airflow.models.xcom.XCom can alias BaseXCom.
        # For metadata cleanup we only use the ORM model XComModel.
        ("xcom", [("airflow.models.xcom", "XComModel")]),
        ("dag_runs", [("airflow.models.dagrun", "DagRun")]),
        ("dag_versions", [("airflow.models.dag_version", "DagVersion")]),
        ("serialized_dags", [("airflow.models.serialized_dag", "SerializedDagModel")]),
        (
            "dag_tags",
            [("airflow.models.dag", "DagTag"), ("airflow.models.dagtag", "DagTag")],
        ),
        ("dag_code", [("airflow.models.dagcode", "DagCode")]),
        (
            "dag_models",
            [
                ("airflow.models.dag", "DagModel"),
                ("airflow.models.dagmodel", "DagModel"),
            ],
        ),
        (
            "parse_import_errors",
            [
                ("airflow.models.errors", "ParseImportError"),
                ("airflow.models.errors", "ImportError"),
            ],
        ),
    ]

    details: dict[str, int] = {}
    warnings: list[str] = []

    try:
        with create_session() as session:
            for label, candidates in model_specs:
                model = _import_airflow_model(candidates)
                if model is None:
                    continue
                # ORM model olmayan siniflarda (ornegin BaseXCom) query kurmaya calismayiz.
                if not hasattr(model, "__mapper__"):
                    continue
                try:
                    query = session.query(model)
                    if hasattr(model, "dag_id"):
                        query = query.filter(getattr(model, "dag_id") == did)
                    elif hasattr(model, "filename"):
                        query = query.filter(
                            getattr(model, "filename").like(f"%{did}%")
                        )
                    else:
                        continue
                    details[label] = int(query.delete(synchronize_session=False) or 0)
                except Exception as exc:
                    warnings.append(f"{label} cleanup failed: {exc}")
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                warnings.append(f"Airflow metadata commit failed: {exc}")
    except Exception as exc:
        warnings.append(f"Airflow metadata cleanup calisamadi: {exc}")

    return {
        "ok": len(warnings) == 0,
        "details": details,
        "warnings": warnings,
    }


def _apply_bundle_to_active(
    *,
    flow_dir: Path,
    dag_path: Path,
    config_path: Path,
    bundle: dict[str, Any],
) -> None:
    existing_auto_mapping_paths = _collect_existing_auto_mapping_paths(
        config_path, flow_dir
    )

    dag_path.write_text(str(bundle.get("dag_text") or ""), encoding="utf-8")
    config_text = str(bundle.get("config_text") or "")
    config_path.write_text(config_text, encoding="utf-8")

    parsed_cfg = yaml.safe_load(config_text) or {}
    if not isinstance(parsed_cfg, dict):
        raise ValueError("Promoted config root must be a dict.")
    required_rels = _auto_mapping_rel_paths_from_config_obj(parsed_cfg)
    mapping_texts = dict(bundle.get("mapping_texts") or {})
    source_conn_id = str(parsed_cfg.get("source_db_var") or "").strip()
    target_conn_id = str(parsed_cfg.get("target_db_var") or "").strip()
    flow_tasks = parsed_cfg.get("flow_tasks")
    rel_task_context: dict[str, tuple[int, dict[str, Any]]] = {}
    if isinstance(flow_tasks, list):
        for idx, task in enumerate(flow_tasks, start=1):
            if not isinstance(task, dict):
                continue
            mode = str(task.get("column_mapping_mode") or "source").strip()
            rel = str(task.get("mapping_file") or "").strip()
            if mode != "mapping_file" or not _is_auto_mapping_relative_file(rel):
                continue
            rel_task_context[_normalize_relative_mapping_file(rel)] = (idx, task)

    new_auto_mapping_paths: set[Path] = set()
    for rel in required_rels:
        rel_norm = _normalize_relative_mapping_file(rel)
        mapping_text = str(mapping_texts.get(rel_norm) or "")
        if not mapping_text.strip():
            existing_path = _resolve_mapping_file_path(flow_dir, rel_norm)
            if existing_path.is_file():
                mapping_text = existing_path.read_text(encoding="utf-8")
        if not mapping_text.strip():
            task_context = rel_task_context.get(rel_norm)
            if task_context is not None:
                task_no, task_obj = task_context
                if not source_conn_id or not target_conn_id:
                    raise ValueError(f"Revision mapping file is missing: {rel_norm}")
                try:
                    mapping_text = _generate_mapping_content_for_task(
                        source_conn_id=source_conn_id,
                        target_conn_id=target_conn_id,
                        task=task_obj,
                        task_no=task_no,
                    )
                except Exception as exc:
                    raise ValueError(
                        f"Revision mapping file is missing and could not be regenerated: {rel_norm}"
                    ) from exc
        if not mapping_text.strip():
            raise ValueError(f"Revision mapping file is missing: {rel_norm}")

        target = _resolve_mapping_file_path(flow_dir, rel_norm)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(mapping_text, encoding="utf-8")
        mapping_texts[rel_norm] = mapping_text
        new_auto_mapping_paths.add(target)

    stale_auto_paths = existing_auto_mapping_paths - new_auto_mapping_paths
    for stale_path in sorted(stale_auto_paths):
        if stale_path.is_file():
            _best_effort_unlink(stale_path)


def get_dag_revisions(dag_id: str) -> dict[str, Any]:
    did = (dag_id or "").strip()
    if not did:
        raise ValueError("dag_id is required.")

    dag_path = _find_studio_dag_file_by_id(did)
    if dag_path is None:
        raise FileNotFoundError(f"DAG not found: {did}")
    config_path = _extract_config_path_from_dag_source(dag_path)
    if not config_path.is_file():
        raise ValueError("DAG was found but linked YAML file was not found.")

    flow_dir = config_path.resolve().parent
    history_root = _revision_history_root(flow_dir, did)
    _prune_revision_history(history_root, _history_keep_limit())
    items = _list_revision_items(history_root, limit=_history_keep_limit())
    active_revision_id = _resolve_active_revision_id(
        history_root=history_root,
        dag_path=dag_path,
        config_path=config_path,
        flow_dir=flow_dir,
    )
    return {
        "dag_id": did,
        "dag_path": dag_path.as_posix(),
        "config_path": config_path.as_posix(),
        "items": items,
        "count": len(items),
        "active_revision_id": active_revision_id,
    }


def promote_dag_revision(
    *,
    dag_id: str,
    revision_id: str,
    actor: str = "flow_studio",
) -> dict[str, Any]:
    did = (dag_id or "").strip()
    rid = (revision_id or "").strip()
    if not did:
        raise ValueError("dag_id is required.")
    if not rid:
        raise ValueError("revision_id is required.")
    if not _REVISION_DIR_RE.fullmatch(rid):
        raise ValueError("revision_id format is invalid.")

    with _dag_operation_lock(did):
        dag_path = _find_studio_dag_file_by_id(did)
        if dag_path is None:
            raise FileNotFoundError(f"DAG not found: {did}")
        config_path = _extract_config_path_from_dag_source(dag_path)
        if not config_path.is_file():
            raise ValueError("DAG was found but linked YAML file was not found.")

        flow_dir = config_path.resolve().parent
        history_root = _revision_history_root(flow_dir, did)
        revision_dir = history_root / rid
        if not revision_dir.is_dir():
            raise FileNotFoundError(f"Revision not found: {rid}")

        rollback_bundle = _read_active_bundle(dag_path, config_path, flow_dir)
        target_bundle = _load_bundle_from_revision(revision_dir)

        def _finalize_promote_response(
            *,
            no_op: bool = False,
            warnings: list[str] | None = None,
        ) -> dict[str, Any]:
            revision_state = get_dag_revisions(did)
            auto_tags: list[str] = []
            try:
                rel = config_path.resolve().relative_to(_projects_root().resolve())
                if len(rel.parts) >= 4:
                    auto_tags = _derive_tags(
                        rel.parts[0], rel.parts[1], rel.parts[2], rel.parts[3]
                    )
            except ValueError:
                auto_tags = []
            raw_cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            user_tags = _normalize_custom_tags(
                raw_cfg.get("custom_tags") if isinstance(raw_cfg, dict) else []
            )
            dag_dependencies = _normalize_dag_dependencies(
                raw_cfg.get("dag_dependencies") if isinstance(raw_cfg, dict) else None
            )
            tags = _merge_tags(auto_tags, user_tags)
            metadata = _load_studio_metadata(flow_dir) or {}
            metadata.update(
                {
                    "flow_dir": flow_dir.as_posix(),
                    "config_path": config_path.as_posix(),
                    "dag_path": dag_path.as_posix(),
                    "dag_id": did,
                    "tags": tags,
                    "auto_tags": auto_tags,
                    "user_tags": user_tags,
                    "dag_dependencies": dag_dependencies,
                    "active_revision_id": revision_state.get("active_revision_id"),
                    "revision_count": revision_state.get("count", 0),
                }
            )
            _write_studio_metadata(flow_dir, metadata)
            response = {
                "dag_id": did,
                "dag_path": dag_path.as_posix(),
                "config_path": config_path.as_posix(),
                "active_revision_id": revision_state.get("active_revision_id"),
                "revision_count": revision_state.get("count", 0),
                "promoted_revision_id": rid,
                "dag_dependencies": dag_dependencies,
                "no_op": no_op,
            }
            warning_items = [
                str(item).strip() for item in list(warnings or []) if str(item).strip()
            ]
            if warning_items:
                response["warnings"] = warning_items
            return response

        current_bundle_hash = str(
            (rollback_bundle.get("hashes") or {}).get("bundle") or ""
        )
        target_manifest_bundle_hash = str(
            (
                ((target_bundle.get("manifest") or {}).get("hashes") or {}).get(
                    "bundle"
                )
                or ""
            )
        )
        target_bundle_hash = _bundle_hash_from_loaded_bundle(target_bundle)
        if not target_bundle_hash:
            target_bundle_hash = target_manifest_bundle_hash
        promote_warnings: list[str] = []
        if (
            target_manifest_bundle_hash
            and target_bundle_hash
            and target_manifest_bundle_hash != target_bundle_hash
        ):
            promote_warnings.append(
                "Revision manifest hash mismatch detected; promote used recalculated revision content hash."
            )
        if (
            current_bundle_hash
            and target_bundle_hash
            and current_bundle_hash == target_bundle_hash
        ):
            return _finalize_promote_response(no_op=True, warnings=promote_warnings)

        before_state = _airflow_parse_state(did)
        try:
            _apply_bundle_to_active(
                flow_dir=flow_dir,
                dag_path=dag_path,
                config_path=config_path,
                bundle=target_bundle,
            )
            if not _wait_for_parse_refresh(did, before_state):
                active_bundle_hash = _active_bundle_hash_or_empty(
                    dag_path, config_path, flow_dir
                )
                if target_bundle_hash and active_bundle_hash == target_bundle_hash:
                    promote_warnings.append(
                        "Airflow parse refresh timeout; revision files are active and promote completed."
                    )
                else:
                    raise TimeoutError(
                        "Airflow parse dogrulamasi zaman asimina ugradi."
                    )
        except Exception as exc:
            _apply_bundle_to_active(
                flow_dir=flow_dir,
                dag_path=dag_path,
                config_path=config_path,
                bundle=rollback_bundle,
            )
            raise ValueError(
                "Revision promote failed; rolled back to the previous active revision."
            ) from exc

        return _finalize_promote_response(no_op=False, warnings=promote_warnings)


def delete_dag_bundle(
    *,
    dag_id: str,
    actor: str = "flow_studio",
    cleanup_references: bool = False,
) -> dict[str, Any]:
    _ = str(actor or "").strip() or "flow_studio"
    did = str(dag_id or "").strip()
    if not did:
        raise ValueError("dag_id is required.")

    with _dag_operation_lock(did):
        dag_path = _find_studio_dag_file_by_id(did)
        if dag_path is None:
            raise FileNotFoundError(f"DAG not found: {did}")
        dag_path = _ensure_path_under_root(dag_path, _generated_dag_root())

        config_path = _extract_config_path_from_dag_source(dag_path)
        if not config_path.is_file():
            raise ValueError("DAG was found but linked YAML file was not found.")
        config_path = _ensure_path_under_root(config_path, _projects_root())
        project, domain, _level, _flow = _extract_scope_from_config_path(config_path)

        flow_dir = config_path.resolve().parent
        auto_mapping_paths = _collect_existing_auto_mapping_paths(config_path, flow_dir)
        history_root = _revision_history_root(flow_dir, did)
        metadata_path = flow_dir / STUDIO_METADATA_NAME

        deleted_paths: list[str] = []
        warnings: list[str] = []
        cleaned_reference_dags: list[str] = []

        scope_entries = _collect_scope_studio_dag_entries(project, domain)
        referenced_by = sorted(
            [
                str(entry.get("dag_id") or "")
                for entry in scope_entries.values()
                if str(entry.get("dag_id") or "") != did
                and did in list(entry.get("upstream_dag_ids") or [])
            ]
        )
        if referenced_by and not cleanup_references:
            raise ValueError(
                "This DAG is referenced by other DAGs. Retry delete with cleanup_references=true."
            )
        if referenced_by and cleanup_references:
            for ref_dag_id in referenced_by:
                ref_entry = scope_entries.get(ref_dag_id) or {}
                ref_config_path = ref_entry.get("config_path")
                if (
                    not isinstance(ref_config_path, Path)
                    or not ref_config_path.is_file()
                ):
                    warnings.append(
                        f"Reference cleanup skipped (YAML missing): {ref_dag_id}"
                    )
                    continue
                try:
                    ref_cfg = _load_yaml_root(ref_config_path)
                    ref_deps = _normalize_dag_dependencies(
                        ref_cfg.get("dag_dependencies")
                    )
                    filtered = [
                        dep
                        for dep in list(ref_deps.get("upstream_dag_ids") or [])
                        if dep != did
                    ]
                    if filtered == list(ref_deps.get("upstream_dag_ids") or []):
                        continue
                    ref_cfg["dag_dependencies"] = {"upstream_dag_ids": filtered}
                    ref_config_path.write_text(
                        yaml.safe_dump(ref_cfg, sort_keys=False, allow_unicode=False),
                        encoding="utf-8",
                    )
                    ref_entry["upstream_dag_ids"] = filtered
                    cleaned_reference_dags.append(ref_dag_id)
                except Exception as exc:
                    warnings.append(f"Reference cleanup failed for {ref_dag_id}: {exc}")

        try:
            airflow_cleanup = _cleanup_airflow_dag_metadata(did)
        except Exception as exc:
            airflow_cleanup = {
                "ok": False,
                "details": {},
                "warnings": [f"Airflow metadata cleanup exception: {exc}"],
            }
        warnings.extend(list(airflow_cleanup.get("warnings") or []))

        for mapping_path in sorted(auto_mapping_paths):
            if not mapping_path.is_file():
                continue
            if _best_effort_unlink(mapping_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(mapping_path.as_posix())
            else:
                warnings.append(
                    f"Mapping file could not be deleted: {mapping_path.as_posix()}"
                )

        if config_path.is_file():
            if _best_effort_unlink(config_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(config_path.as_posix())
            else:
                warnings.append(
                    f"YAML file could not be deleted: {config_path.as_posix()}"
                )

        if dag_path.is_file():
            if _best_effort_unlink(dag_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(dag_path.as_posix())
            else:
                warnings.append(f"DAG file could not be deleted: {dag_path.as_posix()}")

        if history_root.exists():
            if _best_effort_rmtree(history_root):
                deleted_paths.append(history_root.as_posix())
            else:
                warnings.append(
                    f"History directory could not be deleted: {history_root.as_posix()}"
                )

        history_parent = flow_dir / STUDIO_HISTORY_DIR_NAME
        if history_parent.is_dir() and not any(history_parent.iterdir()):
            try:
                history_parent.rmdir()
                deleted_paths.append(history_parent.as_posix())
            except OSError:
                pass

        if metadata_path.is_file():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception:
                metadata = {}
            if str((metadata or {}).get("dag_id") or "").strip() == did:
                if _best_effort_unlink(metadata_path, retries=6, wait_seconds=0.05):
                    deleted_paths.append(metadata_path.as_posix())
                else:
                    warnings.append(
                        f"Metadata file could not be deleted: {metadata_path.as_posix()}"
                    )

        if cleaned_reference_dags:
            for ref_dag_id in cleaned_reference_dags:
                ref_entry = scope_entries.get(ref_dag_id) or {}
                try:
                    _render_single_studio_dag_entry(ref_entry)
                except Exception as exc:
                    warnings.append(
                        f"DAG render refresh failed for {ref_dag_id}: {exc}"
                    )

        return {
            "dag_id": did,
            "deleted_paths": sorted(set(deleted_paths)),
            "airflow_cleanup": airflow_cleanup,
            "warnings": warnings,
            "cleanup_references": bool(cleanup_references),
            "referenced_by": referenced_by,
            "cleaned_reference_dags": cleaned_reference_dags,
        }


def build_task_dict_for_validation(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Pipeline formundan (T06) ConfigValidator ile uyumlu task dict uretir.
    """
    task_type = _normalize_task_type(payload.get("task_type"))
    source_type = payload.get("source_type", "table")
    source_schema = payload.get("source_schema")
    source_table = payload.get("source_table")
    source_conn_id = str(payload.get("source_conn_id") or "").strip()
    target_conn_id = str(payload.get("target_conn_id") or "").strip()
    target_schema = str(payload.get("target_schema") or "").strip()
    target_table = str(payload.get("target_table") or "").strip()
    load_method = payload.get("load_method", "create_if_not_exists_or_truncate")
    upsert_match_columns = _normalize_upsert_match_columns(
        payload.get("upsert_match_columns")
    )
    normalized_source_schema = str(source_schema or "").strip() or (
        "sql" if source_type == "sql" else ""
    )
    normalized_source_table = str(source_table or "").strip() or (
        "query" if source_type == "sql" else ""
    )

    task_group_id = payload.get("task_group_id") or _auto_task_group_id(
        source_db=source_conn_id,
        src_schema=normalized_source_schema,
        src_table=normalized_source_table,
        target_db=target_conn_id,
        load_method=str(load_method),
        tgt_schema=target_schema,
        tgt_table=target_table,
        task_index=1,
    )

    task: dict[str, Any] = {
        "task_type": task_type,
        "task_group_id": task_group_id,
        "source_schema": normalized_source_schema,
        "source_table": normalized_source_table,
        "source_type": source_type,
        "inline_sql": payload.get("inline_sql"),
        "script_run_environment": str(
            payload.get("script_run_environment") or ""
        ).strip()
        or None,
        "script_sql": str(payload.get("script_sql") or "").strip() or None,
        "dag_task_dag_id": str(payload.get("dag_task_dag_id") or "").strip() or None,
        "column_mapping_mode": payload.get("column_mapping_mode", "source"),
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": payload.get("where"),
        "batch_size": int(payload.get("batch_size", 10000)),
        # F2.1 — bulk fields flow into ConfigValidator._check_bulk_api so studio
        # save rejects use_bulk_api in Community / unregistered methods (fail-loud).
        "use_bulk_api": _coerce_bool(payload.get("use_bulk_api"), default=False),
        "bulk_api_method": str(payload.get("bulk_api_method") or "").strip() or None,
        "partitioning": {
            "enabled": bool(payload.get("partitioning_enabled", False)),
            "mode": payload.get("partitioning_mode", "auto_numeric"),
            "column": payload.get("partitioning_column"),
            "parts": int(payload.get("partitioning_parts", 2)),
            "distinct_limit": int(payload.get("partitioning_distinct_limit") or 16),
            "ranges": payload.get("partitioning_ranges") or [],
        },
    }
    bindings = _normalize_bindings(payload.get("bindings"))
    if bindings:
        task["bindings"] = bindings
    if (
        task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
        and source_type == "sql"
        and task["column_mapping_mode"] != "mapping_file"
    ):
        raise ValueError(
            "column_mapping_mode='mapping_file' is required when source_type='sql'."
        )
    if (
        task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
        and payload.get("column_mapping_mode") == "mapping_file"
    ):
        task["mapping_file"] = _auto_mapping_relative_file(1, str(task_group_id))
    if upsert_match_columns:
        task["upsert_match_columns"] = upsert_match_columns
    if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and str(load_method) == "upsert":
        raise ValueError("load_method='upsert' is only valid for source_target tasks.")
    if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and upsert_match_columns:
        raise ValueError(
            "upsert_match_columns is only supported when task_type='source_target'."
        )
    if task_type == STUDIO_TASK_TYPE_DBT:
        task.update(_validated_dbt_fields(payload))
    _apply_file_endpoints(task, payload, source_type, str(task_group_id), 1)
    return task


def _attach_root_engine(payload: dict[str, Any], task: dict[str, Any]) -> None:
    block = payload.get("engine")
    if not isinstance(block, dict):
        return
    if ENGINE_PREFERENCE_FIELD in block:
        task[ENGINE_PREFERENCE_KEY] = block[ENGINE_PREFERENCE_FIELD]
    if ENGINE_SPARK_FIELD in block:
        task[ENGINE_SPARK_KEY] = dict(block[ENGINE_SPARK_FIELD] or {})


def _apply_file_endpoints(
    task: dict[str, Any],
    item: dict[str, Any],
    source_type: str,
    task_group_id: str,
    task_index: int,
) -> None:
    """F1.4/F1.5 — merge validated file source/target fields into a task dict."""
    if task.get("task_type") != STUDIO_TASK_TYPE_SOURCE_TARGET:
        return
    file_source = normalize_file_source(item, source_type)
    if file_source is not None:
        task["column_mapping_mode"] = "mapping_file"
        task["mapping_file"] = _auto_mapping_relative_file(task_index, task_group_id)
        task["source_schema"] = ""
        task["source_table"] = ""
        task.update(file_source)
    file_target = normalize_file_target(item)
    if file_target is not None:
        task.update(file_target)
    # F6.2 — Iceberg/parquet uc noktasi alanlari (EX-D030/EX-D033).
    spark_endpoint = normalize_spark_endpoint(item)
    if spark_endpoint:
        if "file_path" in spark_endpoint:
            task["source_schema"] = ""
            task["source_table"] = ""
        task.update(spark_endpoint)
    # F6.3 — kafka/CDC alanlari (EX-D036); topic schema.table cifti degildir.
    if str(source_type or "").strip().lower() == "kafka":
        task["source_schema"] = ""
        task["source_table"] = ""
        task.update(normalize_kafka_cdc(item))


def build_task_dict_for_validation_from_task(
    task_payload: dict[str, Any],
    *,
    source_conn_id: str,
    target_conn_id: str,
    task_index: int,
) -> dict[str, Any]:
    task_type = _normalize_task_type(task_payload.get("task_type"))
    source_schema = str(task_payload.get("source_schema") or "").strip()
    source_table = str(task_payload.get("source_table") or "").strip()
    target_schema = str(task_payload.get("target_schema") or "").strip()
    target_table = str(task_payload.get("target_table") or "").strip()
    source_type = str(task_payload.get("source_type") or "table").strip() or "table"
    normalized_source_schema = source_schema or ("sql" if source_type == "sql" else "")
    normalized_source_table = source_table or ("query" if source_type == "sql" else "")
    load_method = (
        str(
            task_payload.get("load_method") or "create_if_not_exists_or_truncate"
        ).strip()
        or "create_if_not_exists_or_truncate"
    )
    upsert_match_columns = _normalize_upsert_match_columns(
        task_payload.get("upsert_match_columns")
    )
    task_group_id = str(
        task_payload.get("task_group_id") or ""
    ).strip() or _auto_task_group_id(
        source_db=source_conn_id,
        src_schema=normalized_source_schema,
        src_table=normalized_source_table,
        target_db=target_conn_id,
        load_method=load_method,
        tgt_schema=target_schema,
        tgt_table=target_table,
        task_index=task_index,
    )

    task: dict[str, Any] = {
        "task_type": task_type,
        "task_group_id": task_group_id,
        "source_schema": normalized_source_schema,
        "source_table": normalized_source_table,
        "source_type": source_type,
        "inline_sql": task_payload.get("inline_sql"),
        "script_run_environment": str(
            task_payload.get("script_run_environment") or ""
        ).strip()
        or None,
        "script_sql": str(task_payload.get("script_sql") or "").strip() or None,
        "dag_task_dag_id": str(task_payload.get("dag_task_dag_id") or "").strip()
        or None,
        "column_mapping_mode": str(
            task_payload.get("column_mapping_mode") or "source"
        ).strip()
        or "source",
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": task_payload.get("where"),
        "batch_size": int(task_payload.get("batch_size", 10000)),
        # F2.1 — bulk fields flow into ConfigValidator._check_bulk_api (fail-loud).
        "use_bulk_api": _coerce_bool(task_payload.get("use_bulk_api"), default=False),
        "bulk_api_method": str(task_payload.get("bulk_api_method") or "").strip()
        or None,
        "partitioning": {
            "enabled": bool(task_payload.get("partitioning_enabled", False)),
            "mode": task_payload.get("partitioning_mode", "auto_numeric"),
            "column": task_payload.get("partitioning_column"),
            "parts": int(task_payload.get("partitioning_parts", 2)),
            "distinct_limit": int(
                task_payload.get("partitioning_distinct_limit") or 16
            ),
            "ranges": task_payload.get("partitioning_ranges") or [],
        },
    }
    bindings = _normalize_bindings(task_payload.get("bindings"))
    if bindings:
        task["bindings"] = bindings
    if (
        task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
        and source_type == "sql"
        and task["column_mapping_mode"] != "mapping_file"
    ):
        raise ValueError(
            "column_mapping_mode='mapping_file' is required when source_type='sql'."
        )
    if (
        task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
        and task["column_mapping_mode"] == "mapping_file"
    ):
        task["mapping_file"] = _auto_mapping_relative_file(task_index, task_group_id)
    if upsert_match_columns:
        task["upsert_match_columns"] = upsert_match_columns
    if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and load_method == "upsert":
        raise ValueError("load_method='upsert' is only valid for source_target tasks.")
    if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and upsert_match_columns:
        raise ValueError(
            "upsert_match_columns is only supported when task_type='source_target'."
        )
    if task_type == STUDIO_TASK_TYPE_DBT:
        task.update(_validated_dbt_fields(task_payload))
    _apply_file_endpoints(
        task, task_payload, source_type, str(task_group_id), task_index
    )
    return task


def _validate_non_source_target_task(task: dict[str, Any]) -> None:
    task_type = str(task.get("task_type") or STUDIO_TASK_TYPE_SOURCE_TARGET).strip()
    if str(task.get("load_method") or "").strip() == "upsert":
        raise ValueError("load_method='upsert' is only valid for source_target tasks.")
    if task_type == STUDIO_TASK_TYPE_SCRIPT_RUN:
        environment = str(task.get("script_run_environment") or "").strip()
        if environment not in STUDIO_VALID_SCRIPT_RUN_ENVIRONMENTS:
            raise ValueError(
                "script_run_environment must be one of: 'source' or 'target'."
            )
        script_sql = str(task.get("script_sql") or "").strip()
        if not script_sql:
            raise ValueError("script_sql is required when task_type='script_run'.")
        _validate_binding_contract(
            script_sql,
            task.get("bindings"),
            expression_label="Script SQL / Stored Procedure",
        )
        return
    if task_type == STUDIO_TASK_TYPE_DAG:
        dag_task_dag_id = str(task.get("dag_task_dag_id") or "").strip()
        if not dag_task_dag_id:
            raise ValueError("dag_task_dag_id is required when task_type='dag'.")
        return
    if task_type == STUDIO_TASK_TYPE_BINDING:
        if not _normalize_bindings(task.get("bindings")):
            raise ValueError("bindings is required when task_type='binding'.")
        return
    if task_type == STUDIO_TASK_TYPE_DBT:
        _validated_dbt_fields(task)
        return
    raise ValueError(f"Unsupported task_type: {task_type}")


def _validate_binding_task_targets(
    payload: dict[str, Any], tasks: list[dict[str, Any]]
) -> None:
    declared = {
        str(item.get("name") or "").strip()
        for item in _normalize_dag_params(payload.get("dag_params"))
    }
    for task in tasks:
        if task.get("task_type") != STUDIO_TASK_TYPE_BINDING:
            continue
        targets = {
            str(item.get("variable_name") or "").strip()
            for item in _normalize_bindings(task.get("bindings"))
        }
        for name in targets:
            validate_binding_target_is_custom(name)
        missing = sorted(targets - declared)
        if missing:
            raise ValueError(
                "Binding target must be a declared DAG parameter: "
                + ", ".join(missing)
            )


def validate_pipeline_payload(payload: dict[str, Any]) -> None:
    """Pipeline form (T06): validates task rules with ConfigValidator."""
    validator = ConfigValidator()
    task_items = payload.get("flow_tasks")
    if isinstance(task_items, list) and task_items:
        normalized_tasks: list[dict[str, Any]] = []
        source_conn_id = str(payload.get("source_conn_id") or "").strip()
        target_conn_id = str(payload.get("target_conn_id") or "").strip()
        for idx, task_payload in enumerate(task_items, start=1):
            task = build_task_dict_for_validation_from_task(
                dict(task_payload or {}),
                source_conn_id=source_conn_id,
                target_conn_id=target_conn_id,
                task_index=idx,
            )
            _attach_root_engine(payload, task)
            if (
                str(task.get("task_type") or STUDIO_TASK_TYPE_SOURCE_TARGET)
                == STUDIO_TASK_TYPE_SOURCE_TARGET
            ):
                validator.validate(task)
            else:
                _validate_non_source_target_task(task)
            normalized_tasks.append(task)
        resolve_task_dependencies(normalized_tasks)
        _validate_binding_task_targets(payload, normalized_tasks)
        return

    task = build_task_dict_for_validation(payload)
    _attach_root_engine(payload, task)
    if (
        str(task.get("task_type") or STUDIO_TASK_TYPE_SOURCE_TARGET)
        == STUDIO_TASK_TYPE_SOURCE_TARGET
    ):
        validator.validate(task)
    else:
        _validate_non_source_target_task(task)
    resolve_task_dependencies([task])
    _validate_binding_task_targets(payload, [task])


def fetch_timeline_runs(
    limit: int = 50,
    dag_id: str | None = None,
    state: str | None = None,
) -> list[dict[str, Any]]:
    """DagRun list (T10): filters are optional."""
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    items: list[dict[str, Any]] = []
    with create_session() as session:
        q = session.query(DagRun).order_by(DagRun.start_date.desc())
        if dag_id:
            q = q.filter(DagRun.dag_id == dag_id)
        if state:
            q = q.filter(DagRun.state == state)
        runs = q.limit(limit).all()
        for run in runs:
            items.append(
                {
                    "dag_id": run.dag_id,
                    "run_id": run.run_id,
                    "state": run.state,
                    "start_date": (
                        run.start_date.isoformat() if run.start_date else None
                    ),
                    "end_date": run.end_date.isoformat() if run.end_date else None,
                }
            )
    return items


def _parse_dag_owners(raw_owners: Any) -> list[str]:
    owners_text = str(raw_owners or "").strip()
    if not owners_text:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in owners_text.split(","):
        owner = str(raw or "").strip()
        if not owner or owner in seen:
            continue
        seen.add(owner)
        out.append(owner)
    return out


def _normalize_explorer_path(raw_path: str) -> str:
    text = str(raw_path or "").strip().replace("\\", "/")
    while "//" in text:
        text = text.replace("//", "/")
    if len(text) > 1:
        text = text.rstrip("/")
    return text


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    text = str(value).strip()
    return text or None


def _dag_file_creation_fallback(fileloc: str) -> str | None:
    path_text = str(fileloc or "").strip()
    if not path_text:
        return None
    try:
        stat_obj = Path(path_text).stat()
    except Exception:
        return None

    candidates: list[float] = []
    birth_ts = getattr(stat_obj, "st_birthtime", None)
    if isinstance(birth_ts, (int, float)) and birth_ts > 0:
        candidates.append(float(birth_ts))
    mtime = getattr(stat_obj, "st_mtime", None)
    if isinstance(mtime, (int, float)) and mtime > 0:
        candidates.append(float(mtime))
    ctime = getattr(stat_obj, "st_ctime", None)
    if isinstance(ctime, (int, float)) and ctime > 0:
        candidates.append(float(ctime))
    if not candidates:
        return None

    earliest_ts = min(candidates)
    try:
        return datetime.fromtimestamp(earliest_ts, tz=UTC).isoformat()
    except Exception:
        return None


def _build_dag_explorer_items(
    rows: list[tuple[Any, Any, Any, Any, Any, Any]],
    root: Path,
) -> list[dict[str, Any]]:
    root_norm = _normalize_explorer_path(str(root))
    root_prefix = f"{root_norm}/" if root_norm else "/"
    items: list[dict[str, Any]] = []

    for dag_id, is_paused, fileloc, owners, latest_run, create_date in rows:
        did = str(dag_id or "").strip()
        if not did:
            continue

        fileloc_text = str(fileloc or "").strip()
        fileloc_norm = _normalize_explorer_path(fileloc_text)
        create_date_iso = _iso_or_none(create_date) or _dag_file_creation_fallback(
            fileloc_text
        )
        bucket = "external"
        relative_path: str | None = None
        folder_parts: list[str] = []

        is_root_file = fileloc_norm == root_norm
        is_under_root = bool(fileloc_norm) and fileloc_norm.startswith(root_prefix)
        if is_root_file or is_under_root:
            bucket = "dags_root"
            if is_under_root:
                rel = fileloc_norm[len(root_prefix):]
            else:
                rel = ""
            relative_path = rel or None
            if rel:
                dir_part = rel.rsplit("/", 1)[0] if "/" in rel else ""
                if dir_part:
                    folder_parts = [part for part in dir_part.split("/") if part]

        items.append(
            {
                "dag_id": did,
                "is_paused": bool(is_paused),
                "owners": _parse_dag_owners(owners),
                "fileloc": fileloc_text,
                "latest_run": _iso_or_none(latest_run),
                "create_date": create_date_iso,
                "relative_path": relative_path,
                "folder_parts": folder_parts,
                "bucket": bucket,
                "dag_url": f"/dags/{quote(did, safe='')}",
            }
        )

    items.sort(
        key=lambda item: (
            0 if item["bucket"] == "dags_root" else 1,
            tuple(str(part).lower() for part in item["folder_parts"]),
            str(item["dag_id"]).lower(),
        )
    )
    return items


def _read_dag_explorer_rows() -> list[tuple[Any, Any, Any, Any, Any, Any]]:
    DagModel = None
    for module_name in ("airflow.models.dag", "airflow.models.dagmodel"):
        try:
            module = __import__(module_name, fromlist=["DagModel"])
            DagModel = getattr(module, "DagModel", None)
            if DagModel is not None:
                break
        except Exception:
            continue
    if DagModel is None:
        raise RuntimeError("DagModel is unavailable.")

    DagRun = None
    for module_name in ("airflow.models.dagrun",):
        try:
            module = __import__(module_name, fromlist=["DagRun"])
            DagRun = getattr(module, "DagRun", None)
            if DagRun is not None:
                break
        except Exception:
            continue

    DagVersion = None
    for module_name in ("airflow.models.dag_version",):
        try:
            module = __import__(module_name, fromlist=["DagVersion"])
            DagVersion = getattr(module, "DagVersion", None)
            if DagVersion is not None:
                break
        except Exception:
            continue

    SerializedDagModel = None
    for module_name in ("airflow.models.serialized_dag",):
        try:
            module = __import__(module_name, fromlist=["SerializedDagModel"])
            SerializedDagModel = getattr(module, "SerializedDagModel", None)
            if SerializedDagModel is not None:
                break
        except Exception:
            continue

    from airflow.utils.session import create_session

    with create_session() as session:
        from sqlalchemy import func

        base_rows = (
            session.query(
                DagModel.dag_id,
                DagModel.is_paused,
                DagModel.fileloc,
                DagModel.owners,
            )
            .order_by(DagModel.dag_id.asc())
            .all()
        )

        latest_run_by_dag: dict[str, Any] = {}
        if DagRun is not None:
            latest_rows = (
                session.query(
                    DagRun.dag_id,
                    func.max(DagRun.run_after).label("latest_run"),
                )
                .group_by(DagRun.dag_id)
                .all()
            )
            for dag_id, latest_run in latest_rows:
                latest_run_by_dag[str(dag_id or "")] = latest_run

        creation_by_dag: dict[str, Any] = {}
        if DagVersion is not None:
            creation_rows = (
                session.query(
                    DagVersion.dag_id,
                    func.min(DagVersion.created_at).label("creation_date"),
                )
                .group_by(DagVersion.dag_id)
                .all()
            )
            for dag_id, creation_date in creation_rows:
                creation_by_dag[str(dag_id or "")] = creation_date

        if SerializedDagModel is not None:
            serialized_rows = (
                session.query(
                    SerializedDagModel.dag_id,
                    func.min(SerializedDagModel.created_at).label("creation_date"),
                )
                .group_by(SerializedDagModel.dag_id)
                .all()
            )
            for dag_id, creation_date in serialized_rows:
                key = str(dag_id or "")
                existing = creation_by_dag.get(key)
                if existing is None:
                    creation_by_dag[key] = creation_date
                elif creation_date is not None and creation_date < existing:
                    creation_by_dag[key] = creation_date

        rows: list[tuple[Any, Any, Any, Any, Any, Any]] = []
        for dag_id, is_paused, fileloc, owners in base_rows:
            key = str(dag_id or "")
            rows.append(
                (
                    dag_id,
                    is_paused,
                    fileloc,
                    owners,
                    latest_run_by_dag.get(key),
                    creation_by_dag.get(key),
                )
            )
    return list(rows)


def discover_dag_explorer_items() -> dict[str, Any]:
    root = _generated_dag_root()
    rows = _read_dag_explorer_rows()
    items = _build_dag_explorer_items(rows, root)
    return {
        "root": _normalize_explorer_path(str(root)),
        "items": items,
        "count": len(items),
    }


def search_dag_explorer_items(query: str) -> dict[str, Any]:
    """DAG Explorer content search.

    Case-insensitive keyword match over each DAG's on-disk content — the
    generated `.py` plus its `config.yaml` and mapping YAML (where source_type/
    file_path/tables/where/etc. live). Returns the same item shape as
    `discover_dag_explorer_items` so the right panel renders identically.
    Empty query ⇒ all items (unfiltered).
    """
    base = discover_dag_explorer_items()
    needle = str(query or "").strip().lower()
    if not needle:
        return base
    matched = [
        item for item in base.get("items", []) if _dag_item_matches(item, needle)
    ]
    return {"root": base.get("root"), "items": matched, "count": len(matched)}


def _dag_item_matches(item: dict[str, Any], needle: str) -> bool:
    """A cheap dag_id match, then a content scan for studio DAGs only."""
    if needle in str(item.get("dag_id") or "").lower():
        return True
    if str(item.get("bucket") or "") != "dags_root":
        return False  # external DAGs carry no config → dag_id match only
    return needle in _dag_searchable_text(item).lower()


def _dag_searchable_text(item: dict[str, Any]) -> str:
    """Best-effort full text of one studio DAG (.py + config.yaml + mapping).

    Never raises: a malformed/non-studio/unreadable DAG falls back to the `.py`
    text alone (or ""), so it simply won't content-match. Path reads are guarded
    under the DAG root / projects root (traversal-safe).
    """
    fileloc = str(item.get("fileloc") or "").strip()
    if not fileloc:
        return ""
    try:
        dag_path = _ensure_path_under_root(Path(fileloc), _generated_dag_root())
        dag_text = dag_path.read_text(encoding="utf-8")
    except (ValueError, OSError):
        return ""
    try:
        config_path = _extract_config_path_from_dag_source(dag_path)
        config_path = _ensure_path_under_root(config_path, _projects_root())
        bundle = _read_active_bundle(dag_path, config_path, config_path.parent)
        parts = [bundle["dag_text"], bundle["config_text"]]
        parts.extend(bundle["mapping_texts"].values())
        return "\n".join(parts)
    except Exception:
        return dag_text


def discover_connections() -> list[dict[str, str]]:
    """Returns the configured connection list from Airflow metadata."""
    from airflow.models.connection import Connection
    from airflow.utils.session import create_session

    items: list[dict[str, str]] = []
    with create_session() as session:
        rows = (
            session.query(Connection.conn_id, Connection.conn_type)
            .order_by(Connection.conn_id.asc())
            .all()
        )
    for conn_id, conn_type in rows:
        items.append(
            {
                "conn_id": str(conn_id or ""),
                "conn_type": str(conn_type or ""),
            }
        )
    return items


def discover_airflow_variables(
    search: str | None = None,
    limit: int = 200,
    exact: bool = False,
) -> list[str]:
    """Returns the Variable key list from Airflow metadata."""
    from airflow.models import Variable
    from airflow.utils.session import create_session

    safe_limit = max(1, min(int(limit or 200), 1000))
    search_val = (search or "").strip()

    with create_session() as session:
        q = session.query(Variable.key).order_by(Variable.key.asc())
        if exact and search_val:
            q = q.filter(Variable.key == search_val)
        elif exact:
            return []
        elif search_val:
            q = q.filter(Variable.key.ilike(f"%{search_val.lower()}%"))
        rows = q.limit(safe_limit).all()

    keys = [str(row[0] or "") for row in rows if str(row[0] or "").strip()]
    if exact:
        keys = [key for key in keys if key == search_val]
    return sorted(set(keys))


def _list_child_dirs(path: Path) -> list[str]:
    if not path.is_dir():
        return []
    items: list[str] = []
    try:
        for entry in path.iterdir():
            name = entry.name
            if not entry.is_dir():
                continue
            if name.startswith(".") or name.startswith("__"):
                continue
            items.append(entry.name)
    except OSError:
        return []
    return sorted(set(items))


def discover_hierarchy_options(
    project: str | None = None,
    domain: str | None = None,
    level: str | None = None,
    source: str | None = None,
) -> dict[str, list[str]]:
    """
    Returns available folder options for Flow Studio hierarchy.
    Hem projects root hem dag root taranir ve union alinir.
    """
    project_val = (project or "").strip()
    domain_val = (domain or "").strip()
    level_val = (level or "").strip()
    source_val = (source or "union").strip().lower()

    if source_val == "dag":
        roots = [_generated_dag_root()]
    elif source_val == "projects":
        roots = [_projects_root()]
    elif source_val == "union":
        roots = [_projects_root(), _generated_dag_root()]
    else:
        raise ValueError("source must be one of: 'dag', 'projects', or 'union'.")

    projects: set[str] = set()
    domains: set[str] = set()
    levels: set[str] = set()
    flows: set[str] = set()

    for root in roots:
        projects.update(_list_child_dirs(root))
        if project_val:
            project_dir = root / project_val
            domains.update(_list_child_dirs(project_dir))
            if domain_val:
                domain_dir = project_dir / domain_val
                levels.update(_list_child_dirs(domain_dir))
                if level_val:
                    level_dir = domain_dir / level_val
                    flows.update(_list_child_dirs(level_dir))

    return {
        "projects": sorted(projects),
        "domains": sorted(domains),
        "levels": sorted(levels),
        "flows": sorted(flows),
    }


def discover_schemas(
    conn_id: str,
    search: str | None = None,
    limit: int = 200,
) -> list[str]:
    params = AirflowConnectionAdapter.get_connection_params(conn_id)
    dialect = resolve_dialect(params["conn_type"])
    with DBSession(params, dialect) as session:
        schemas = dialect.list_schemas(session.conn)

    search_val = (search or "").strip().lower()
    if search_val:
        schemas = [name for name in schemas if search_val in str(name or "").lower()]

    safe_limit = max(1, min(int(limit or 200), 1000))
    return list(schemas[:safe_limit])


def _resolve_schema_name(available_schemas: list[str], requested_schema: str) -> str:
    requested = str(requested_schema or "").strip()
    if not requested:
        raise ValueError("Schema value cannot be empty.")
    if requested in available_schemas:
        return requested

    requested_lower = requested.lower()
    case_insensitive_exact = [
        s for s in available_schemas if str(s or "").lower() == requested_lower
    ]
    if len(case_insensitive_exact) == 1:
        return case_insensitive_exact[0]

    prefix_matches = [
        s for s in available_schemas if str(s or "").lower().startswith(requested_lower)
    ]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    if len(prefix_matches) > 1:
        raise ValueError(
            f"Schema '{requested}' birden fazla eslesme verdi: {', '.join(prefix_matches[:5])}"
        )

    raise ValueError(f"Schema not found: {requested}")


def _resolve_table_name(available_tables: list[str], requested_table: str) -> str:
    requested = str(requested_table or "").strip()
    if "." in requested:
        requested = requested.rsplit(".", 1)[-1].strip()
    requested = requested.strip('"').strip("'").strip()
    if not requested:
        raise ValueError("Table value cannot be empty.")
    if requested in available_tables:
        return requested

    requested_lower = requested.lower()
    case_insensitive_exact = [
        t for t in available_tables if str(t or "").lower() == requested_lower
    ]
    if len(case_insensitive_exact) == 1:
        return case_insensitive_exact[0]

    # Accept common UI/manual entry variations such as Event_Logs vs EventLogs.
    def _canon(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())

    requested_canon = _canon(requested)
    if requested_canon:
        canon_matches = [
            t for t in available_tables if _canon(str(t or "")) == requested_canon
        ]
        if len(canon_matches) == 1:
            return canon_matches[0]
        if len(canon_matches) > 1:
            raise ValueError(
                f"Table '{requested}' birden fazla kanonik eslesme verdi: {', '.join(canon_matches[:5])}"
            )

    prefix_matches = [
        t for t in available_tables if str(t or "").lower().startswith(requested_lower)
    ]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    if len(prefix_matches) > 1:
        raise ValueError(
            f"Table '{requested}' birden fazla eslesme verdi: {', '.join(prefix_matches[:5])}"
        )

    raise ValueError(f"Table not found: {requested}")


def discover_tables(
    conn_id: str,
    schema: str,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    params = AirflowConnectionAdapter.get_connection_params(conn_id)
    dialect = resolve_dialect(params["conn_type"])
    with DBSession(params, dialect) as session:
        available_schemas = dialect.list_schemas(session.conn)
        resolved_schema = _resolve_schema_name(available_schemas, schema)
        tables = dialect.list_tables(session.conn, resolved_schema)

    search_val = (search or "").strip().lower()
    if search_val:
        tables = [tbl for tbl in tables if search_val in tbl.lower()]

    safe_limit = max(1, min(int(limit or 50), 50))
    safe_offset = max(0, int(offset or 0))
    total = len(tables)
    items = tables[safe_offset:safe_offset + safe_limit]

    return {
        "schema": resolved_schema,
        "schema_input": schema,
        "total": total,
        "limit": safe_limit,
        "offset": safe_offset,
        "items": items,
    }


def discover_columns(conn_id: str, schema: str, table: str) -> list[dict[str, Any]]:
    params = AirflowConnectionAdapter.get_connection_params(conn_id)
    dialect = resolve_dialect(params["conn_type"])
    with DBSession(params, dialect) as session:
        available_schemas = dialect.list_schemas(session.conn)
        resolved_schema = _resolve_schema_name(available_schemas, schema)
        available_tables = dialect.list_tables(session.conn, resolved_schema)
        resolved_table = _resolve_table_name(available_tables, table)
        columns = dialect.get_table_schema(
            session.conn, resolved_schema, resolved_table
        )

    return [
        {
            "name": c.name,
            "data_type": c.data_type,
            "nullable": c.nullable,
            "precision": c.precision,
            "scale": c.scale,
        }
        for c in columns
    ]


def _detect_file_columns(
    conn_id: str,
    conn_type: str,
    file_path: str,
    source_type: str,
    options: dict[str, Any],
    sample_limit: int = 5,
) -> tuple[list[str], list[list[Any]]]:
    """Read a file's header/first-object keys + a few sample rows (preview)."""
    import csv as _csv
    import json as _json

    from ffengine.pipeline.file_transport import open_read, resolve_read_paths

    paths = resolve_read_paths(conn_id, conn_type, file_path)
    handle = open_read(conn_id, conn_type, paths[0])
    encoding = str(options.get("encoding") or "utf-8")
    names: list[str] = []
    sample_rows: list[list[Any]] = []
    try:
        lines = (raw.decode(encoding) for raw in handle.stream)
        if source_type == "csv":
            reader = _csv.reader(
                lines,
                delimiter=str(options.get("delimiter") or ","),
                quotechar=str(options.get("quotechar") or '"'),
            )
            header = next(reader, [])
            if options.get("header", True):
                names = [str(h) for h in header]
            else:
                names = [str(i + 1) for i in range(len(header))]
                sample_rows.append(list(header))
            for _, row in zip(range(sample_limit), reader):
                sample_rows.append(list(row))
        else:
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                obj = _json.loads(stripped)
                if not isinstance(obj, dict):
                    raise ValueError(
                        "JSON flat preview bir JSON obje bekler (JSONL)."
                    )
                if not names:
                    names = list(obj.keys())
                sample_rows.append([obj.get(n) for n in names])
                if len(sample_rows) >= sample_limit:
                    break
    finally:
        handle.close()
    if not names:
        raise ValueError(f"Dosyadan kolon algilanamadi: {file_path}")
    return names, sample_rows


def _file_source_mapping_preview(
    payload: dict[str, Any], source_type: str, source_conn_id: str
) -> dict[str, Any]:
    """F1.4 — draft mapping from a file's detected columns (untyped → editable)."""
    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    conn_type = str(src_params.get("conn_type") or "")
    file_path = str(payload.get("file_path") or "").strip()
    if not file_path:
        raise ValueError("file_path is required for a csv/json source preview.")
    options = {
        "delimiter": payload.get("delimiter"),
        "encoding": payload.get("encoding"),
        "header": payload.get("header", True),
        "quotechar": payload.get("quotechar"),
        "json_mode": payload.get("json_mode", "flat"),
    }
    names, sample_rows = _detect_file_columns(
        source_conn_id, conn_type, file_path, source_type, options
    )
    version = str(payload.get("version") or "v1.1").strip() or "v1.1"
    task_no = max(1, int(payload.get("task_no") or 1))
    task_group_id = str(payload.get("task_group_id") or "").strip() or f"task_{task_no}"
    columns = [
        {
            "source_name": name,
            "target_name": name,
            "target_type": "varchar(255)",
            "nullable": True,
        }
        for name in names
    ]
    mapping_obj = {"version": version, "columns": columns}
    return {
        "mapping_content": _mapping_dump_text(mapping_obj),
        "generated_mapping_file": _auto_mapping_relative_file(task_no, task_group_id),
        "warnings": [
            "Dosya kaynaklarinda tip algilanamaz; target_type varsayilani "
            "'varchar(255)' — mapping editorunden duzenleyin."
        ],
        "column_count": len(columns),
        "version": version,
        "columns": columns,
        "sample_rows": sample_rows,
    }


def generate_mapping_preview(payload: dict[str, Any]) -> dict[str, Any]:
    source_type = str(payload.get("source_type") or "table").strip() or "table"
    source_conn_id = str(payload.get("source_conn_id") or "").strip()
    target_conn_id = str(payload.get("target_conn_id") or "").strip()
    if not source_conn_id or not target_conn_id:
        raise ValueError("source_conn_id and target_conn_id are required.")

    if source_type in FILE_SOURCE_TYPES:
        return _file_source_mapping_preview(payload, source_type, source_conn_id)

    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
    src_dialect = resolve_dialect(src_params["conn_type"])
    tgt_dialect = resolve_dialect(tgt_params["conn_type"])

    src_name = _dialect_name(src_dialect)
    tgt_name = _dialect_name(tgt_dialect)
    version = str(payload.get("version") or "v1.1").strip() or "v1.1"
    task_no = max(1, int(payload.get("task_no") or 1))
    task_group_id = str(payload.get("task_group_id") or "").strip()
    if not task_group_id:
        task_group_id = f"task_{task_no}"
    generated_mapping_file = _auto_mapping_relative_file(task_no, task_group_id)

    if source_type in {"table", "view"}:
        source_schema = str(payload.get("source_schema") or "").strip()
        source_table = str(payload.get("source_table") or "").strip()
        if not source_schema or not source_table:
            raise ValueError(
                "source_schema and source_table are required when source_type=table|view."
            )
        with DBSession(src_params, src_dialect) as src_session:
            # Lenient scaffold: produce a best-effort draft even when a source
            # column lacks precision/length (e.g. bare `numeric`). Strict type
            # validation runs at Apply (client) and Save (server), not here.
            mapping_obj = MappingGenerator().generate(
                src_session.conn,
                src_dialect,
                tgt_dialect,
                source_schema,
                source_table,
                version=version,
                strict=False,
            )
    elif source_type == "sql":
        inline_sql = str(payload.get("inline_sql") or "").strip()
        if not inline_sql:
            raise ValueError("inline_sql is required when source_type='sql'.")
        sql_cols = extract_sql_select_columns_for_conn(source_conn_id, inline_sql)
        mapping_obj, _ = _build_mapping_from_columns(
            columns=sql_cols,
            src_dialect_name=src_name,
            tgt_dialect_name=tgt_name,
            version=version,
            strict=False,
        )
    else:
        raise ValueError("source_type can only be table|view|sql.")

    mapping_text = _mapping_dump_text(mapping_obj)
    columns = mapping_obj.get("columns") or []
    warnings = _incomplete_type_warnings(columns)
    return {
        "mapping_content": mapping_text,
        "generated_mapping_file": generated_mapping_file,
        "warnings": warnings,
        "column_count": len(columns),
        "version": str(mapping_obj.get("version") or version),
        "columns": columns,
    }


def create_or_update_dag(
    payload: dict[str, Any],
    *,
    update: bool = False,
    dag_id: str | None = None,
) -> dict[str, Any]:
    folder_scope = _require_folder_scope(payload)
    _bulk_backfill_legacy_task_types_once()
    validate_pipeline_payload(payload)

    project = _slugify(folder_scope["project"], "default_project")
    domain = _slugify(folder_scope["domain"], "default_domain")
    level = _slugify(folder_scope["level"], "level1")
    flow = _slugify(folder_scope["flow"], "src_to_stg")

    task_payloads = payload.get("flow_tasks")
    if isinstance(task_payloads, list) and task_payloads:
        tasks_input = [dict(item or {}) for item in task_payloads]
    else:
        tasks_input = [dict(payload)]

    lock_ctx = (
        _dag_operation_lock(str(dag_id or "").strip()) if update else nullcontext()
    )
    with lock_ctx:
        root = _projects_root()
        flow_dir = root / project / domain / level / flow
        flow_dir.mkdir(parents=True, exist_ok=True)
        _ensure_path_under_root(flow_dir, root)
        (flow_dir / "mapping").mkdir(parents=True, exist_ok=True)

        gen_root = _generated_dag_root()
        flow_dag_dir = gen_root / project / domain / level / flow
        flow_dag_dir.mkdir(parents=True, exist_ok=True)
        _ensure_path_under_root(flow_dag_dir, gen_root)

        dag_path: Path
        config_path: Path
        if update:
            update_dag_id = str(dag_id or "").strip()
            if not update_dag_id:
                raise ValueError("dag_id query param is required for update-dag.")
            existing_studio_dag = _find_studio_dag_file_by_id(update_dag_id)
            if existing_studio_dag is None:
                raise ValueError(f"DAG to update not found: dag_id={update_dag_id}")
            dag_path = existing_studio_dag
            _ensure_path_under_root(dag_path, gen_root)
            config_path = _extract_config_path_from_dag_source(dag_path)
            if not config_path.is_file():
                raise ValueError("YAML file to update was not found.")
            _ensure_path_under_root(config_path, root)

            config_resolved = config_path.resolve()
            rel = config_resolved.relative_to(root.resolve())
            if len(rel.parts) < 5:
                raise ValueError("Linked YAML path hierarchy in DAG is invalid.")
            cfg_project, cfg_domain, cfg_level, cfg_flow = rel.parts[:4]
            if (cfg_project, cfg_domain, cfg_level, cfg_flow) != (
                project,
                domain,
                level,
                flow,
            ):
                raise ValueError(
                    "dag_id and payload hierarchy do not match: "
                    f"dag=({cfg_project}/{cfg_domain}/{cfg_level}/{cfg_flow}) "
                    f"payload=({project}/{domain}/{level}/{flow})"
                )

            group_no = _extract_group_no(dag_path.stem, config_path)
        else:
            group_no = _next_group_no(flow_dir, flow_dag_dir)
            dag_path = flow_dag_dir / _build_dag_filename(
                project,
                domain,
                level,
                flow,
                group_no,
            )
            _ensure_path_under_root(dag_path, gen_root)
            config_path = flow_dir / _build_yaml_filename(
                project, domain, level, flow, group_no
            )

        existing_auto_mapping_paths = _collect_existing_auto_mapping_paths(
            config_path, flow_dir
        )
        auto_tags = _derive_tags(project, domain, level, flow)
        user_tags = _normalize_custom_tags(payload.get("custom_tags"))
        tags = _merge_tags(auto_tags, user_tags)
        scheduler = normalize_scheduler(payload.get("scheduler"))
        existing_cfg = _load_yaml_root(config_path) if update else {}
        if update and "dag_params" not in payload:
            dag_params = _normalize_dag_params(existing_cfg.get("dag_params"))
        else:
            dag_params = _normalize_dag_params(payload.get("dag_params"))
        if update and "dag_dependencies" not in payload:
            dag_dependencies = _normalize_dag_dependencies(
                existing_cfg.get("dag_dependencies")
            )
        else:
            dag_dependencies = _normalize_dag_dependencies(
                payload.get("dag_dependencies")
            )
        if update and "notifications" not in payload:
            notifications = normalize_notifications(existing_cfg.get("notifications"))
        else:
            notifications = normalize_notifications(payload.get("notifications"))
        scope_entries = _collect_scope_studio_dag_entries(project, domain)
        dag_upstream_dag_ids = _validate_dag_dependencies_for_scope(
            project=project,
            domain=domain,
            dag_id=dag_path.stem,
            upstream_dag_ids=list(dag_dependencies.get("upstream_dag_ids") or []),
            scope_entries=scope_entries,
        )
        dag_dependencies = {"upstream_dag_ids": dag_upstream_dag_ids}
        actor = (
            str(os.getenv("FFENGINE_STUDIO_ACTOR", "flow_studio")).strip()
            or "flow_studio"
        )
        operation_warnings: list[str] = []

        task_cfgs: list[dict[str, Any]] = []
        sql_mapping_checks: list[dict[str, Any]] = []
        pending_mapping_writes: list[dict[str, Any]] = []
        # Authoritative Connection Types (DAG-level) for the same-vs-cross "max
        # size" rule; stamped into each inline mapping so Save + runtime + the
        # persisted file all agree on the dialects.
        save_src_dialect, save_tgt_dialect = _resolve_save_dialect_names(payload)
        for idx, item in enumerate(tasks_input, start=1):
            task_type = _normalize_task_type(item.get("task_type"))
            source_schema = str(item.get("source_schema") or "").strip()
            source_table = str(item.get("source_table") or "").strip()
            target_schema = str(item.get("target_schema") or "").strip()
            target_table = str(item.get("target_table") or "").strip()
            source_type = str(item.get("source_type") or "table").strip() or "table"
            load_method = (
                str(
                    item.get("load_method") or "create_if_not_exists_or_truncate"
                ).strip()
                or "create_if_not_exists_or_truncate"
            )
            upsert_match_columns = _normalize_upsert_match_columns(
                item.get("upsert_match_columns")
            )
            script_run_environment = (
                str(item.get("script_run_environment") or "").strip().lower()
            )
            script_sql = str(item.get("script_sql") or "").strip() or None
            dag_task_dag_id = str(item.get("dag_task_dag_id") or "").strip() or None
            file_source = None
            file_target = None
            dbt_cfg: dict[str, Any] = {}

            if task_type == STUDIO_TASK_TYPE_SOURCE_TARGET:
                file_source = normalize_file_source(item, source_type)
                file_target = normalize_file_target(item)
                if file_source is not None:
                    normalized_source_schema = ""
                    normalized_source_table = ""
                    auto_source_schema = "file"
                    auto_source_table = _slugify(
                        os.path.basename(file_source["file_path"]) or "file", "file"
                    )
                elif str(source_type).strip().lower() == "kafka":
                    # F6.3 — bir topic schema.table cifti degildir (EX-D036).
                    normalized_source_schema = ""
                    normalized_source_table = ""
                    auto_source_schema = "kafka"
                    auto_source_table = _slugify(
                        str(item.get("kafka_topic") or "") or "topic", "topic"
                    )
                else:
                    normalized_source_schema = source_schema or (
                        "sql" if source_type == "sql" else ""
                    )
                    normalized_source_table = source_table or (
                        "query" if source_type == "sql" else ""
                    )
                    auto_source_schema = normalized_source_schema
                    auto_source_table = normalized_source_table
                auto_load_method = load_method
            elif task_type == STUDIO_TASK_TYPE_SCRIPT_RUN:
                if script_run_environment not in STUDIO_VALID_SCRIPT_RUN_ENVIRONMENTS:
                    raise ValueError(
                        "script_run_environment must be one of: 'source' or 'target'."
                    )
                if not script_sql:
                    raise ValueError(
                        "script_sql is required when task_type='script_run'."
                    )
                normalized_source_schema = source_schema
                normalized_source_table = source_table
                auto_source_schema = "script"
                auto_source_table = script_run_environment
                auto_load_method = "script"
            elif task_type == STUDIO_TASK_TYPE_BINDING:
                normalized_source_schema = ""
                normalized_source_table = ""
                auto_source_schema = "binding"
                auto_source_table = str(idx)
                auto_load_method = "binding"
            elif task_type == STUDIO_TASK_TYPE_DBT:
                # F3.2 — validate + normalize the dbt contract fields; the
                # Enterprise-provider gate lives inside (fail-loud in
                # Community). NOTE: this elif must stay BEFORE the implicit
                # 'dag' else-branch below.
                dbt_cfg = _validated_dbt_fields(item)
                normalized_source_schema = ""
                normalized_source_table = ""
                auto_source_schema = "dbt"
                auto_source_table = dbt_cfg["dbt_command"]
                auto_load_method = "dbt"
            else:
                if not dag_task_dag_id:
                    raise ValueError(
                        "dag_task_dag_id is required when task_type='dag'."
                    )
                if dag_task_dag_id == dag_path.stem:
                    raise ValueError("dag_task_dag_id cannot reference itself.")
                if scope_entries.get(dag_task_dag_id) is None:
                    raise ValueError(
                        f"dag_task_dag_id contains invalid dag_id: {dag_task_dag_id}"
                    )
                normalized_source_schema = source_schema
                normalized_source_table = source_table
                auto_source_schema = "dag"
                auto_source_table = _slugify(dag_task_dag_id, "dag")
                auto_load_method = "dag"

            task_group_id = str(
                item.get("task_group_id") or ""
            ).strip() or _auto_task_group_id(
                source_db=str(payload.get("source_conn_id") or ""),
                src_schema=auto_source_schema,
                src_table=auto_source_table,
                target_db=str(payload.get("target_conn_id") or ""),
                load_method=auto_load_method,
                tgt_schema=target_schema,
                tgt_table=target_table,
                task_index=idx,
            )
            raw_depends_on = item.get("depends_on")
            if raw_depends_on is None:
                raw_depends_on = []
            if not isinstance(raw_depends_on, list):
                raise ValueError(
                    f"depends_on must be a list: task_group_id={task_group_id}"
                )
            task_cfg: dict[str, Any] = {
                "task_type": task_type,
                "task_group_id": task_group_id,
                "depends_on": [
                    dep_id
                    for dep_id in dict.fromkeys(
                        str(dep or "").strip() for dep in raw_depends_on
                    )
                    if dep_id
                ],
                "source_schema": normalized_source_schema,
                "source_table": normalized_source_table,
                "source_type": source_type,
                "inline_sql": str(item.get("inline_sql") or "").strip() or None,
                "script_run_environment": script_run_environment or None,
                "script_sql": script_sql,
                "dag_task_dag_id": dag_task_dag_id,
                "column_mapping_mode": str(
                    item.get("column_mapping_mode") or "source"
                ).strip()
                or "source",
                "target_schema": target_schema,
                "target_table": target_table,
                "load_method": load_method,
                "where": item.get("where") or None,
                "batch_size": int(item.get("batch_size", 10000)),
                "partitioning": {
                    "enabled": bool(item.get("partitioning_enabled", False)),
                    "mode": item.get("partitioning_mode", "auto_numeric"),
                    "column": item.get("partitioning_column") or None,
                    "parts": int(item.get("partitioning_parts", 2)),
                    "distinct_limit": int(
                        item.get("partitioning_distinct_limit") or 16
                    ),
                    "ranges": item.get("partitioning_ranges") or [],
                },
                "tags": tags,
            }
            if task_type == STUDIO_TASK_TYPE_BINDING:
                task_cfg = {
                    "task_type": task_type,
                    "task_group_id": task_group_id,
                    "depends_on": task_cfg["depends_on"],
                    "tags": tags,
                }
            if task_type == STUDIO_TASK_TYPE_DBT:
                # F3.2 — narrow YAML: only the dbt contract keys travel to
                # disk (binding-task precedent); engine fields never leak.
                task_cfg = {
                    "task_type": task_type,
                    "task_group_id": task_group_id,
                    "depends_on": task_cfg["depends_on"],
                    **dbt_cfg,
                    "tags": tags,
                }
            bindings = _normalize_bindings(item.get("bindings"))
            if bindings:
                task_cfg["bindings"] = bindings
            if upsert_match_columns:
                task_cfg["upsert_match_columns"] = upsert_match_columns
            # F2.1 — emit bulk fields only for source_target tasks and only when
            # enabled, so existing off-path YAML stays byte-identical.
            if task_type == STUDIO_TASK_TYPE_SOURCE_TARGET and _coerce_bool(
                item.get("use_bulk_api"), default=False
            ):
                task_cfg["use_bulk_api"] = True
                bulk_method = str(item.get("bulk_api_method") or "").strip()
                if bulk_method:
                    task_cfg["bulk_api_method"] = bulk_method
            if file_source is not None:
                task_cfg["column_mapping_mode"] = "mapping_file"
                task_cfg.update(file_source)
            if file_target is not None:
                task_cfg.update(file_target)
            # F6.3 — kafka/CDC alanlari YAML'a tasinir (round-trip; sessiz
            # dusurme = INV-1). Kafka-olmayan task'larda dict bos -> YAML
            # byte-ayni kalir.
            if task_type == STUDIO_TASK_TYPE_SOURCE_TARGET:
                task_cfg.update(normalize_kafka_cdc(item))
            mode = str(task_cfg.get("column_mapping_mode") or "source")
            mapping_content = str(item.get("mapping_content") or "")
            if mapping_content.strip() and save_src_dialect and save_tgt_dialect:
                mapping_content = _stamp_mapping_dialects(
                    mapping_content,
                    source_dialect=save_src_dialect,
                    target_dialect=save_tgt_dialect,
                )
            if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and load_method == "upsert":
                raise ValueError(
                    "load_method='upsert' is only valid for source_target tasks."
                )
            if task_type != STUDIO_TASK_TYPE_SOURCE_TARGET and upsert_match_columns:
                raise ValueError(
                    "upsert_match_columns is only supported when task_type='source_target'."
                )
            if (
                task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
                and load_method == "upsert"
                and not upsert_match_columns
            ):
                raise ValueError(
                    "upsert_match_columns is required when load_method='upsert'."
                )
            if (
                task_type == STUDIO_TASK_TYPE_SOURCE_TARGET
                and source_type == "sql"
                and mode != "mapping_file"
            ):
                raise ValueError(
                    "column_mapping_mode='mapping_file' is required when source_type='sql'."
                )
            if task_type == STUDIO_TASK_TYPE_SOURCE_TARGET and mode == "mapping_file":
                mapping_rel = _auto_mapping_relative_file(idx, task_group_id)
                mapping_path = _resolve_mapping_file_path(flow_dir, mapping_rel)
                task_cfg["mapping_file"] = mapping_rel
                if not mapping_content.strip() and not mapping_path.is_file():
                    raise ValueError(
                        "mapping_content is required when column_mapping_mode='mapping_file' "
                        f"and mapping file does not exist: {mapping_rel}. "
                        "Use Generate Mapping or provide mapping_content."
                    )
                pending_mapping_writes.append(
                    {
                        "task_group_id": task_group_id,
                        "mapping_path": mapping_path,
                        "mapping_content": mapping_content,
                    }
                )
                if source_type == "sql":
                    sql_mapping_checks.append(
                        {
                            "task_group_id": task_group_id,
                            "inline_sql": task_cfg.get("inline_sql"),
                            "mapping_path": mapping_path,
                            "mapping_content": mapping_content,
                        }
                    )
            task_cfgs.append(task_cfg)

        for pending in pending_mapping_writes:
            mapping_content = str(pending.get("mapping_content") or "")
            mapping_path = pending["mapping_path"]
            if mapping_content.strip():
                _parse_yaml_mapping_text(mapping_content, label=mapping_path.as_posix())
            else:
                _read_mapping_object(mapping_path)

        resolve_task_dependencies(task_cfgs)
        compile_dag_parameter_flow(dag_params, task_cfgs)

        # F3.2b (EX-D016) — Asset save-time guards, BEFORE any file write:
        # (1) consumer membership/stale-URI: this DAG's asset triggers must
        #     be derivable from another DAG's emitting cosmos producer NOW;
        # (2) producer orphan: this save must not strand any other DAG's
        #     asset consumption (mode change, emit off, task removal...).
        if scheduler.get("trigger_type") == "asset":
            _validate_asset_consumer_membership(
                dag_path.stem, list(scheduler.get("assets") or [])
            )
        _validate_no_orphaned_asset_consumers(
            dag_path.stem,
            {
                "target_db_var": payload.get("target_conn_id"),
                "flow_tasks": task_cfgs,
            },
        )

        if sql_mapping_checks:
            for check in sql_mapping_checks:
                inline_sql = str(check.get("inline_sql") or "").strip()
                if not inline_sql:
                    raise ValueError(
                        f"inline_sql is required when source_type='sql'. task_group_id={check['task_group_id']}"
                    )
                sql_columns = [
                    col["name"]
                    for col in extract_sql_select_columns_for_conn(
                        payload["source_conn_id"], inline_sql
                    )
                ]
                mapping_content = str(check.get("mapping_content") or "")
                if mapping_content.strip():
                    mapping_obj = _parse_yaml_mapping_text(
                        mapping_content,
                        label=f"task_group_id={check['task_group_id']}",
                    )
                else:
                    mapping_obj = _read_mapping_object(check["mapping_path"])
                mapping_columns = _mapping_yaml_to_source_columns(mapping_obj)
                if sql_columns != mapping_columns:
                    raise ValueError(
                        "SQL select columns are incompatible with mapping: "
                        f"task_group_id={check['task_group_id']}; "
                        f"expected={sql_columns}; actual={mapping_columns}"
                    )

        history_root = _revision_history_root(flow_dir, dag_path.stem)
        pre_update_bundle: dict[str, Any] | None = None
        if update and dag_path.is_file() and config_path.is_file():
            pre_update_bundle = _read_active_bundle(dag_path, config_path, flow_dir)

        try:
            for pending in pending_mapping_writes:
                mapping_content = str(pending.get("mapping_content") or "")
                if not mapping_content.strip():
                    continue
                _parse_yaml_mapping_text(
                    mapping_content, label=pending["mapping_path"].as_posix()
                )
                normalized_text = (
                    mapping_content
                    if mapping_content.endswith("\n")
                    else f"{mapping_content}\n"
                )
                mapping_path: Path = pending["mapping_path"]
                mapping_path.parent.mkdir(parents=True, exist_ok=True)
                if mapping_path.is_file():
                    existing = mapping_path.read_text(encoding="utf-8")
                    if _semantic_yaml_equal(existing, normalized_text):
                        continue
                mapping_path.write_text(normalized_text, encoding="utf-8")

            new_auto_mapping_paths: set[Path] = set()
            for task_cfg in task_cfgs:
                rel = str(task_cfg.get("mapping_file") or "").strip()
                if not _is_auto_mapping_relative_file(rel):
                    continue
                new_auto_mapping_paths.add(_resolve_mapping_file_path(flow_dir, rel))
            stale_auto_paths = existing_auto_mapping_paths - new_auto_mapping_paths
            for stale_path in sorted(stale_auto_paths):
                if stale_path.is_file():
                    _best_effort_unlink(stale_path)

            config_obj = {
                "source_db_var": payload["source_conn_id"],
                "target_db_var": payload["target_conn_id"],
                "flow_tasks": task_cfgs,
                "custom_tags": user_tags,
                "scheduler": scheduler,
                "dag_dependencies": dag_dependencies,
                "dag_params": dag_params,
            }
            if isinstance(payload.get("engine"), dict):
                config_obj["engine"] = dict(payload["engine"])
            if notifications:
                config_obj["notifications"] = notifications
            config_path.write_text(
                yaml.safe_dump(config_obj, sort_keys=False, allow_unicode=False),
                encoding="utf-8",
            )
            dag_source = _render_group_dag_source(
                dag_id=dag_path.stem,
                config_path=config_path,
                tags=tags,
                upstream_dag_ids=dag_upstream_dag_ids,
                raw_config=config_obj,
            )
            dag_path.write_text(dag_source, encoding="utf-8")
            if update:
                pause_sync_warning = _sync_dag_paused_state(
                    dag_path.stem, active=bool(scheduler.get("active", True))
                )
                if pause_sync_warning:
                    operation_warnings.append(pause_sync_warning)
        except Exception:
            if update and pre_update_bundle is not None:
                _apply_bundle_to_active(
                    flow_dir=flow_dir,
                    dag_path=dag_path,
                    config_path=config_path,
                    bundle=pre_update_bundle,
                )
            raise

        if update and pre_update_bundle is not None:
            current_bundle = _read_active_bundle(dag_path, config_path, flow_dir)
            previous_hash = str(
                (pre_update_bundle.get("hashes") or {}).get("bundle") or ""
            )
            current_hash = str((current_bundle.get("hashes") or {}).get("bundle") or "")
            if previous_hash and current_hash and previous_hash != current_hash:
                _save_bundle_as_revision(
                    flow_dir=flow_dir,
                    dag_id=dag_path.stem,
                    dag_path=dag_path,
                    config_path=config_path,
                    source=REVISION_SOURCE_UPDATE,
                    actor=actor,
                )
        elif not update:
            _save_bundle_as_revision(
                flow_dir=flow_dir,
                dag_id=dag_path.stem,
                dag_path=dag_path,
                config_path=config_path,
                source=REVISION_SOURCE_CREATE_INITIAL,
                actor=actor,
            )

        revision_items = _list_revision_items(history_root, limit=_history_keep_limit())
        active_revision_id = _resolve_active_revision_id(
            history_root=history_root,
            dag_path=dag_path,
            config_path=config_path,
            flow_dir=flow_dir,
        )

        metadata = {
            "flow_dir": flow_dir.as_posix(),
            "config_path": config_path.as_posix(),
            "dag_path": dag_path.as_posix(),
            "dag_id": dag_path.stem,
            "task_group_id": task_cfgs[0]["task_group_id"],
            "task_count": len(task_cfgs),
            "group_no": group_no,
            "tags": tags,
            "auto_tags": auto_tags,
            "user_tags": user_tags,
            "active_revision_id": active_revision_id,
            "revision_count": len(revision_items),
            "scheduler": scheduler,
            "dag_dependencies": dag_dependencies,
        }
        _write_studio_metadata(flow_dir, metadata)

        response = {
            "flow_dir": metadata["flow_dir"],
            "config_path": metadata["config_path"],
            "dag_path": metadata["dag_path"],
            "dag_id": metadata["dag_id"],
            "task_group_id": task_cfgs[0]["task_group_id"],
            "active_revision_id": active_revision_id,
            "revision_count": len(revision_items),
            "scheduler": scheduler,
            "dag_dependencies": dag_dependencies,
        }
        if operation_warnings:
            response["warnings"] = operation_warnings
        return response
