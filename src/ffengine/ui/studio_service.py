"""
Flow Studio MVP service layer.

Phase 1 (T01-T04, T07, T11) and Phase 2 (T05-T10, T08-T09, T12) endpoints use this module.
"""

from __future__ import annotations

import hashlib
import json
import os
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
from ffengine.config.validator import ConfigValidator
from ffengine.db.airflow_adapter import AirflowConnectionAdapter
from ffengine.db.session import DBSession
from ffengine.dialects.type_mapper import TypeMapper, UnsupportedTypeError
from ffengine.mapping.generator import MappingGenerator
from ffengine.mapping.resolver import VALID_MAPPING_VERSIONS, _dialect_name

STUDIO_METADATA_NAME = ".flow_studio.json"
STUDIO_DAG_MARKER = "# generated_by: flow_studio"
STUDIO_HISTORY_DIR_NAME = ".flow_studio_history"
STUDIO_HISTORY_KEEP_LIMIT = 20
STUDIO_CUSTOM_TAG_MAX_COUNT = 10
STUDIO_CUSTOM_TAG_MAX_LENGTH = 32
STUDIO_DAG_DEPENDENCY_MAX_COUNT = 200
STUDIO_DEFAULT_START_DATE = "2023-01-01T00:00:00"
STUDIO_DEFAULT_ACTIVE = True
REVISION_SOURCE_CREATE_INITIAL = "create_initial"
REVISION_SOURCE_UPDATE = "update"

_REVISION_DIR_RE = re.compile(r"^rev_(\d{6})$")
_DAG_LOCKS: dict[str, threading.Lock] = {}
_DAG_LOCKS_GUARD = threading.Lock()


def _slugify(value: str, default: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_").lower()
    return cleaned or default


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
            "airflow_variable_key": str(item.get("airflow_variable_key") or "").strip() or None,
        }
        normalized.append(normalized_item)
    return normalized


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
        raise ValueError("scheduler.cron_expression must be a valid 5-field cron expression.")
    try:
        from croniter import croniter

        croniter(cron)
    except ImportError:
        allowed = re.compile(r"^[\d\*/,\-]+$")
        for field in cron.split():
            if field == "?":
                raise ValueError("scheduler.cron_expression must be a valid 5-field cron expression.")
            if not allowed.fullmatch(field):
                raise ValueError("scheduler.cron_expression must be a valid 5-field cron expression.")
    except Exception as exc:
        raise ValueError("scheduler.cron_expression must be a valid 5-field cron expression.") from exc
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


def normalize_scheduler(raw_scheduler: Any) -> dict[str, Any]:
    if raw_scheduler is None:
        payload: dict[str, Any] = {}
    elif isinstance(raw_scheduler, dict):
        payload = dict(raw_scheduler)
    else:
        raise ValueError("scheduler must be an object.")

    timezone_name = str(payload.get("timezone") or "").strip() or get_airflow_default_timezone_name()
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError("scheduler.timezone must be a valid IANA timezone.") from exc

    cron_expression = _normalize_scheduler_cron(payload.get("cron_expression"))
    start_date = _normalize_scheduler_start_date(payload.get("start_date"), timezone_name=timezone_name)
    active = _coerce_bool(payload.get("active"), default=STUDIO_DEFAULT_ACTIVE)

    return {
        "cron_expression": cron_expression,
        "timezone": timezone_name,
        "active": active,
        "start_date": start_date,
    }


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
        f"{project_slug}_{domain_slug}_{level_slug}_{flow_slug}_group_{int(group_no)}_dag.py"
    )


def _build_yaml_filename(
    project: str,
    domain: str,
    level: str,
    flow: str,
    group_no: int,
) -> str:
    return (
        f"{project}_{domain}_{level}_{flow}_group_{int(group_no)}.yaml"
    )


def _extract_group_no_from_name(name: str) -> int | None:
    match = re.search(r"_group_(\d+)", name or "")
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
        for item in flow_dir.glob("*_group_*.yaml"):
            g = _extract_group_no_from_name(item.name)
            if g is not None:
                groups.add(g)

    if flow_dag_dir.is_dir():
        for item in flow_dag_dir.glob("*_group_*_dag.py"):
            g = _extract_group_no_from_name(item.name)
            if g is not None:
                groups.add(g)

    return (max(groups) + 1) if groups else 1


def _projects_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_PROJECTS_ROOT", "/opt/airflow/projects")
    return Path(root)


def _generated_dag_root() -> Path:
    return Path(os.getenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags"))


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
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {task_id}")
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
            raise ValueError(
                f"depends_on must be a list: task_group_id={task_id}"
            )
        for dep in depends_on:
            dep_id = str(dep or "").strip()
            if not dep_id:
                continue
            if dep_id == task_id:
                raise ValueError(f"depends_on cannot reference itself: {task_id}")
            if dep_id not in id_set:
                raise ValueError(
                    f"depends_on contains invalid task_group_id: {dep_id}"
                )
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


def _collect_scope_studio_dag_entries(project: str, domain: str) -> dict[str, dict[str, Any]]:
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
            cfg_project, cfg_domain, cfg_level, cfg_flow = _extract_scope_from_config_path(config_path)
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
            upstreams = list((scope_entries.get(dag_id) or {}).get("upstream_dag_ids") or [])
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
        flow_dir = _projects_root() / scope_project / scope_domain / scope_level / scope_flow
        flow_dag_dir = _generated_dag_root() / scope_project / scope_domain / scope_level / scope_flow
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
            str(row.get("level") or ""),
            str(row.get("flow") or ""),
            int(row.get("group_no") or 0),
            str(row.get("dag_id") or ""),
        )
    )

    referenced_by = sorted(
        [
            str(entry.get("dag_id") or "")
            for entry in scope_entries.values()
            if current_dag_id and current_dag_id in list(entry.get("upstream_dag_ids") or [])
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
    if not dag_id or not isinstance(dag_path, Path) or not isinstance(config_path, Path):
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
    )
    if not dag_path.is_file() or dag_path.read_text(encoding="utf-8") != dag_source:
        dag_path.write_text(dag_source, encoding="utf-8")


def _render_group_dag_source(
    *,
    dag_id: str,
    config_path: Path,
    tags: list[str],
    upstream_dag_ids: list[str] | None = None,
) -> str:
    cfg = json.dumps(config_path.as_posix())
    did = json.dumps(dag_id)
    dtags = json.dumps(tags)
    upstream_ids = json.dumps(list(dict.fromkeys(upstream_dag_ids or [])))
    return f'''{STUDIO_DAG_MARKER}
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
import hashlib
from airflow import DAG
try:
    from airflow.sdk import task
except Exception:
    from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from ffengine.airflow.operator import (
    FFEngineOperator,
    aggregate_partition_payloads,
    plan_partitions_for_task,
    prepare_target_for_task,
    run_partition_for_task,
)

CONFIG_PATH = Path({cfg})
DAG_ID = {did}
DAG_TAGS = {dtags}
UPSTREAM_DAG_IDS = {upstream_ids}


def _slug_task_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", str(value or "").strip()).strip("_").lower()


def _bounded_task_id(value: str, max_len: int = 250) -> str:
    normalized = _slug_task_token(value) or "task"
    if len(normalized) <= max_len:
        return normalized
    suffix = "__h_" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    head_len = max(1, max_len - len(suffix))
    head = normalized[:head_len].rstrip("_")
    if not head:
        head = "task"
    return f"{{head}}{{suffix}}"


dag_slug = _slug_task_token(DAG_ID) or "dag"


def _resolve_task_dependencies(task_defs):
    task_ids = []
    id_set = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Each flow_task must be a dict.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("task_group_id is required for each flow_task.")
        if task_id in id_set:
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {{task_id}}")
        task_ids.append(task_id)
        id_set.add(task_id)

    edges = []
    seen_edges = set()
    for idx, task in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task.get("depends_on")
        if depends_on is None:
            depends_on = []
        if not isinstance(depends_on, list):
            raise ValueError(f"depends_on must be a list: task_group_id={{task_id}}")
        for dep in depends_on:
            dep_id = str(dep or "").strip()
            if not dep_id:
                continue
            if dep_id == task_id:
                raise ValueError(f"depends_on cannot reference itself: {{task_id}}")
            if dep_id not in id_set:
                raise ValueError(f"depends_on contains invalid task_group_id: {{dep_id}}")
            edge = (dep_id, task_id)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            edges.append(edge)

    graph = {{task_id: [] for task_id in task_ids}}
    for upstream, downstream in edges:
        graph[upstream].append(downstream)
    state = {{}}

    def _dfs(node):
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


raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {{}}
if not isinstance(raw, dict):
    raise ValueError("YAML root must be a dict.")

source_conn_id = str(raw.get("source_db_var") or "").strip()
target_conn_id = str(raw.get("target_db_var") or "").strip()
task_defs = raw.get("flow_tasks") or []
scheduler = raw.get("scheduler") or {{}}
if not isinstance(scheduler, dict):
    scheduler = {{}}

if not source_conn_id or not target_conn_id:
    raise ValueError("source_db_var and target_db_var are required.")
if not isinstance(task_defs, list) or not task_defs:
    raise ValueError("flow_tasks must be a list with at least one task.")

cron_expression = scheduler.get("cron_expression")
if isinstance(cron_expression, str):
    cron_expression = cron_expression.strip() or None
else:
    cron_expression = None

timezone_name = str(scheduler.get("timezone") or "UTC").strip() or "UTC"
try:
    scheduler_tz = ZoneInfo(timezone_name)
except Exception:
    scheduler_tz = ZoneInfo("UTC")

start_date_raw = str(scheduler.get("start_date") or "{STUDIO_DEFAULT_START_DATE}").strip() or "{STUDIO_DEFAULT_START_DATE}"
try:
    dag_start_date = datetime.fromisoformat(start_date_raw.replace("Z", "+00:00"))
except ValueError:
    dag_start_date = datetime(2023, 1, 1, 0, 0, 0)
if dag_start_date.tzinfo is None:
    dag_start_date = dag_start_date.replace(tzinfo=scheduler_tz)
else:
    dag_start_date = dag_start_date.astimezone(scheduler_tz)

dag_active = bool(scheduler.get("active", True))
edges = _resolve_task_dependencies(task_defs)
effective_schedule = None if UPSTREAM_DAG_IDS else cron_expression
task_ids_with_upstream = {{downstream for _upstream, downstream in edges}}
root_task_ids = [task_id for task_id in [str(task.get("task_group_id") or "").strip() for task in task_defs] if task_id not in task_ids_with_upstream]
root_task_order = {{task_id: idx + 1 for idx, task_id in enumerate(root_task_ids)}}

with DAG(
    dag_id=DAG_ID,
    schedule=effective_schedule,
    start_date=dag_start_date,
    catchup=False,
    tags=DAG_TAGS,
    is_paused_upon_creation=not dag_active,
) as dag:
    task_groups = {{}}
    for task_def in task_defs:
        task_group_id = str(task_def.get("task_group_id") or "").strip()
        partition_cfg = task_def.get("partitioning")
        partition_enabled = isinstance(partition_cfg, dict) and bool(partition_cfg.get("enabled", False))
        if not partition_enabled:
            task_slug = _slug_task_token(task_group_id) or f"task_{{max(1, len(task_groups) + 1)}}"
            if UPSTREAM_DAG_IDS and task_group_id in root_task_ids:
                upstream_slug_parts = [_slug_task_token(uid) for uid in UPSTREAM_DAG_IDS]
                upstream_slug_parts = [item for item in upstream_slug_parts if item]
                joined_upstream_slug = "__".join(upstream_slug_parts)
                root_order = int(root_task_order.get(task_group_id) or 1)
                if joined_upstream_slug:
                    task_id_value = _bounded_task_id(
                        f"run_after__{{joined_upstream_slug}}__{{dag_slug}}__r{{root_order}}"
                    )
                else:
                    task_id_value = _bounded_task_id(f"run__{{dag_slug}}__r{{root_order}}")
            else:
                task_id_value = f"run_{{task_group_id}}"
            task_groups[task_group_id] = FFEngineOperator(
                config_path=str(CONFIG_PATH),
                task_group_id=task_group_id,
                source_conn_id=source_conn_id,
                target_conn_id=target_conn_id,
                task_id=task_id_value,
            )
            continue
        group_id = "flow__" + re.sub(r"[^A-Za-z0-9_]+", "_", task_group_id).strip("_").lower()
        with TaskGroup(group_id=group_id) as flow_group:
            @task(task_id="plan_partitions")
            def _plan_partitions(
                _config_path=str(CONFIG_PATH),
                _task_group_id=task_group_id,
                _source_conn_id=source_conn_id,
                _target_conn_id=target_conn_id,
            ):
                return plan_partitions_for_task(
                    config_path=_config_path,
                    task_group_id=_task_group_id,
                    source_conn_id=_source_conn_id,
                    target_conn_id=_target_conn_id,
                )

            @task(task_id="prepare_target")
            def _prepare_target(
                _plan_specs,
                _config_path=str(CONFIG_PATH),
                _task_group_id=task_group_id,
                _source_conn_id=source_conn_id,
                _target_conn_id=target_conn_id,
            ):
                _ = _plan_specs
                return prepare_target_for_task(
                    config_path=_config_path,
                    task_group_id=_task_group_id,
                    source_conn_id=_source_conn_id,
                    target_conn_id=_target_conn_id,
                )

            @task(task_id="run_partition")
            def _run_partition(
                partition_spec,
                _config_path=str(CONFIG_PATH),
                _task_group_id=task_group_id,
                _source_conn_id=source_conn_id,
                _target_conn_id=target_conn_id,
            ):
                return run_partition_for_task(
                    config_path=_config_path,
                    task_group_id=_task_group_id,
                    source_conn_id=_source_conn_id,
                    target_conn_id=_target_conn_id,
                    partition_spec=partition_spec,
                )

            @task(task_id="aggregate")
            def _aggregate_partition_payloads(results):
                return aggregate_partition_payloads(results)

            plan_ctx = _plan_partitions()
            prepare_ctx = _prepare_target(plan_ctx)
            run_payloads = _run_partition.expand(partition_spec=plan_ctx)
            prepare_ctx >> run_payloads
            _aggregate_partition_payloads(run_payloads)
        task_groups[task_group_id] = flow_group

    for upstream, downstream in edges:
        task_groups[upstream] >> task_groups[downstream]

    if UPSTREAM_DAG_IDS:
        upstream_triggers = {{}}
        upstream_waiters = {{}}
        for upstream_dag_id in UPSTREAM_DAG_IDS:
            trigger_task_id = "trigger_upstream__" + re.sub(r"[^A-Za-z0-9_]+", "_", str(upstream_dag_id)).strip("_").lower()
            upstream_triggers[upstream_dag_id] = TriggerDagRunOperator(
                task_id=trigger_task_id,
                trigger_dag_id=upstream_dag_id,
                logical_date="{{{{ dag_run.logical_date }}}}",
                wait_for_completion=False,
                reset_dag_run=False,
                skip_when_already_exists=False,
            )
            waiter_task_id = "wait_upstream__" + re.sub(r"[^A-Za-z0-9_]+", "_", str(upstream_dag_id)).strip("_").lower()
            upstream_waiters[upstream_dag_id] = ExternalTaskSensor(
                task_id=waiter_task_id,
                external_dag_id=upstream_dag_id,
                external_task_id=None,
                allowed_states=["success"],
                failed_states=["failed"],
                check_existence=False,
                mode="reschedule",
                poke_interval=60,
                timeout=12 * 60 * 60,
            )
            upstream_triggers[upstream_dag_id] >> upstream_waiters[upstream_dag_id]

        for waiter in upstream_waiters.values():
            for root_task_id in root_task_ids:
                waiter >> task_groups[root_task_id]

'''


def _ensure_path_under_root(path: Path, root: Path) -> Path:
    """Path traversal guard: path must stay under root."""
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"Invalid path: {path!s}") from exc
    return resolved


def _best_effort_unlink(path: Path, *, retries: int = 80, wait_seconds: float = 0.1) -> bool:
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
    if not isinstance(mapping_obj, dict):
        raise ValueError("Mapping YAML root must be a dict.")
    version = mapping_obj.get("version")
    if version not in VALID_MAPPING_VERSIONS:
        raise ValueError(
            f"Unsupported mapping file version: {version!r}. "
            f"Gecerli: {sorted(VALID_MAPPING_VERSIONS)}"
        )
    entries = mapping_obj.get("columns")
    if not isinstance(entries, list) or not entries:
        raise ValueError("columns in Mapping YAML is empty or invalid.")
    out: list[str] = []
    for idx, item in enumerate(entries, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Mapping columns[{idx-1}] must be a dict.")
        src = str(item.get("source_name") or "").strip()
        if not src:
            raise ValueError(f"Mapping columns[{idx-1}] source_name cannot be empty.")
        out.append(src)
    return out


def _parse_yaml_mapping_text(mapping_content: str, *, label: str) -> dict[str, Any]:
    try:
        parsed = yaml.safe_load(mapping_content)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid mapping YAML ({label}): {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid mapping YAML ({label} ): root must be a dict.")
    _mapping_yaml_to_source_columns(parsed)
    return parsed


def _read_mapping_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Mapping file not found: {path.as_posix()}")
    return _parse_yaml_mapping_text(path.read_text(encoding="utf-8"), label=path.as_posix())


def _normalize_description_type(type_code: Any) -> str:
    if type_code is None:
        return "TEXT"
    if isinstance(type_code, str):
        raw = type_code
    elif hasattr(type_code, "__name__"):
        raw = str(getattr(type_code, "__name__", ""))
    else:
        raw = str(type_code)
    cleaned = re.sub(r"[^A-Za-z0-9_ ]+", "_", raw).strip("_ ").upper()
    return cleaned or "TEXT"


def _wrap_zero_row_sql_for_dialect(inline_sql: str, dialect_name: str) -> str:
    base = str(inline_sql or "").strip().rstrip(";")
    if not base:
        raise ValueError("inline_sql is required when source_type='sql'.")
    if dialect_name == "mssql":
        return f"SELECT TOP 0 * FROM ({base}) AS ffengine_inline_sql"
    if dialect_name == "oracle":
        return f"SELECT * FROM ({base}) ffengine_inline_sql WHERE 1=0"
    return f"SELECT * FROM ({base}) AS ffengine_inline_sql LIMIT 0"


def extract_sql_select_columns(src_session: DBSession, src_dialect, inline_sql: str) -> list[dict[str, str]]:
    """Extract column names and normalized type names from SQL query metadata."""
    dialect_name = _dialect_name(src_dialect)
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
        name = str(col[0] if len(col) > 0 else "").strip()
        if not name:
            continue
        type_code = col[1] if len(col) > 1 else None
        cols.append({"name": name, "source_type": _normalize_description_type(type_code)})
    if not cols:
        raise ValueError("No columns found during SQL metadata extraction.")
    return cols


def extract_sql_select_columns_for_conn(source_conn_id: str, inline_sql: str) -> list[dict[str, str]]:
    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    src_dialect = resolve_dialect(src_params["conn_type"])
    with DBSession(src_params, src_dialect) as src_session:
        return extract_sql_select_columns(src_session, src_dialect, inline_sql)


def _collect_existing_auto_mapping_paths(config_path: Path, flow_dir: Path) -> set[Path]:
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
    columns: list[dict[str, str]],
    src_dialect_name: str,
    tgt_dialect_name: str,
    version: str = "v1",
) -> tuple[dict[str, Any], list[str]]:
    if version not in VALID_MAPPING_VERSIONS:
        raise ValueError(
            f"Invalid mapping version: {version!r}. "
            f"Gecerli: {sorted(VALID_MAPPING_VERSIONS)}"
        )
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []
    fallback_target = TypeMapper.map_type("TEXT", src_dialect_name, tgt_dialect_name)
    for col in columns:
        src_name = str(col.get("name") or "").strip()
        src_type = str(col.get("source_type") or "TEXT").strip().upper() or "TEXT"
        if not src_name:
            continue
        try:
            tgt_type = TypeMapper.map_type(src_type, src_dialect_name, tgt_dialect_name)
        except UnsupportedTypeError:
            tgt_type = fallback_target
            warnings.append(
                f"{src_name}: source_type={src_type!r} could not be resolved, target_type={tgt_type!r} fallback applied."
            )
        rows.append(
            {
                "source_name": src_name,
                "target_name": src_name,
                "source_type": src_type,
                "target_type": tgt_type,
                "nullable": True,
            }
        )
    if not rows:
        raise ValueError("No usable columns found for mapping generation.")
    return (
        {
            "version": version,
            "source_dialect": src_dialect_name,
            "target_dialect": tgt_dialect_name,
            "columns": rows,
        },
        warnings,
    )


def _mapping_dump_text(mapping_obj: dict[str, Any]) -> str:
    return yaml.safe_dump(mapping_obj, sort_keys=False, allow_unicode=True)


def _generate_mapping_content_for_task(
    *,
    source_conn_id: str,
    target_conn_id: str,
    task: dict[str, Any],
    task_no: int,
) -> str:
    source_type = str(task.get("source_type") or "table").strip() or "table"
    task_group_id = str(task.get("task_group_id") or "").strip() or f"task_{max(1, int(task_no))}"

    preview_payload: dict[str, Any] = {
        "source_conn_id": str(source_conn_id or "").strip(),
        "target_conn_id": str(target_conn_id or "").strip(),
        "source_type": source_type,
        "task_no": max(1, int(task_no)),
        "task_group_id": task_group_id,
        "version": "v1",
    }
    if source_type in {"table", "view"}:
        preview_payload["source_schema"] = str(task.get("source_schema") or "").strip()
        preview_payload["source_table"] = str(task.get("source_table") or "").strip()
    elif source_type == "sql":
        preview_payload["inline_sql"] = str(task.get("inline_sql") or "").strip()

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
    raw = str(os.getenv("FFENGINE_STUDIO_HISTORY_KEEP_LIMIT", str(STUDIO_HISTORY_KEEP_LIMIT))).strip()
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
    stale = dirs[:-max(1, keep_limit)]
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


def _read_active_bundle(dag_path: Path, config_path: Path, flow_dir: Path) -> dict[str, Any]:
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
    bundle_hash = _sha256_text(json.dumps(file_hashes, sort_keys=True))
    file_hashes["bundle"] = bundle_hash

    return {
        "dag_text": dag_text,
        "config_text": config_text,
        "config_obj": config_obj,
        "mapping_texts": mapping_texts,
        "hashes": file_hashes,
    }


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
        raise FileNotFoundError(f"Revision manifest not found: {manifest_path.as_posix()}")
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


def _list_revision_items(history_root: Path, *, limit: int | None = None) -> list[dict[str, Any]]:
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
    for item in _list_revision_items(history_root):
        if item.get("bundle_hash") == bundle_hash:
            return str(item.get("revision_id") or "") or None
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
    match = re.search(r"_group_(\d+)_dag$", dag_id)
    if match:
        return int(match.group(1))
    cfg_match = re.search(r"_group_(\d+)\.ya?ml$", config_path.name)
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
                "source_schema": str(task.get("source_schema") or "").strip(),
                "source_table": str(task.get("source_table") or "").strip(),
                "source_type": str(task.get("source_type") or "table").strip() or "table",
                "inline_sql": str(task.get("inline_sql") or "").strip() or None,
                "target_schema": str(task.get("target_schema") or "").strip(),
                "target_table": str(task.get("target_table") or "").strip(),
                "load_method": (
                    str(task.get("load_method") or "create_if_not_exists_or_truncate").strip()
                    or "create_if_not_exists_or_truncate"
                ),
                "column_mapping_mode": (
                    str(task.get("column_mapping_mode") or "source").strip() or "source"
                ),
                "mapping_file": str(task.get("mapping_file") or "").strip() or None,
                "mapping_content": _load_mapping_content_for_task(config_resolved.parent, task),
                "where": str(task.get("where") or "").strip() or None,
                "batch_size": int(task.get("batch_size") or 10000),
                "partitioning_enabled": bool(partitioning.get("enabled", False)),
                "partitioning_mode": str(partitioning.get("mode") or "auto_numeric").strip() or "auto_numeric",
                "partitioning_column": str(partitioning.get("column") or "").strip() or None,
                "partitioning_parts": int(partitioning.get("parts") or 2),
                "partitioning_distinct_limit": int(partitioning.get("distinct_limit") or 16),
                "partitioning_ranges": partitioning.get("ranges") or [],
                "bindings": _normalize_bindings(task.get("bindings")),
            }
        )

    first_task = normalized_tasks[0]
    custom_tags = _normalize_custom_tags(raw.get("custom_tags"))
    scheduler = normalize_scheduler(raw.get("scheduler"))
    dag_dependencies = _normalize_dag_dependencies(raw.get("dag_dependencies"))

    payload = {
        "project": project,
        "domain": domain,
        "level": level,
        "flow": flow,
        "custom_tags": custom_tags,
        "scheduler": scheduler,
        "dag_dependencies": dag_dependencies,
        "group_no": _extract_group_no(did, config_path),
        "task_group_id": first_task["task_group_id"],
        "source_conn_id": str(raw.get("source_db_var") or "").strip(),
        "target_conn_id": str(raw.get("target_db_var") or "").strip(),
        "source_schema": first_task["source_schema"],
        "source_table": first_task["source_table"],
        "source_type": first_task["source_type"],
        "inline_sql": first_task["inline_sql"],
        "target_schema": first_task["target_schema"],
        "target_table": first_task["target_table"],
        "load_method": first_task["load_method"],
        "column_mapping_mode": first_task["column_mapping_mode"],
        "mapping_file": first_task["mapping_file"],
        "mapping_content": first_task["mapping_content"],
        "where": first_task["where"],
        "batch_size": first_task["batch_size"],
        "partitioning_enabled": first_task["partitioning_enabled"],
        "partitioning_mode": first_task["partitioning_mode"],
        "partitioning_column": first_task["partitioning_column"],
        "partitioning_parts": first_task["partitioning_parts"],
        "partitioning_distinct_limit": first_task["partitioning_distinct_limit"],
        "partitioning_ranges": first_task["partitioning_ranges"],
        "bindings": first_task["bindings"],
        "flow_tasks": normalized_tasks,
    }

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


def _parse_state_changed(before: dict[str, Any] | None, after: dict[str, Any] | None) -> bool:
    if before is None:
        return after is not None
    if after is None:
        return False
    if str(after.get("dag_version_id") or "") != str(before.get("dag_version_id") or ""):
        return True
    if str(after.get("dag_hash") or "") != str(before.get("dag_hash") or ""):
        return True
    if str(after.get("serialized_last_updated") or "") != str(before.get("serialized_last_updated") or ""):
        return True
    if int(after.get("version_number") or 0) > int(before.get("version_number") or 0):
        return True
    return False


def _wait_for_parse_refresh(dag_id: str, before_state: dict[str, Any] | None) -> bool:
    if not _env_bool("FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE", True):
        return True
    timeout_seconds_raw = str(os.getenv("FFENGINE_STUDIO_PROMOTE_VERIFY_TIMEOUT_SECONDS", "35")).strip()
    interval_seconds_raw = str(os.getenv("FFENGINE_STUDIO_PROMOTE_VERIFY_INTERVAL_SECONDS", "1")).strip()
    try:
        timeout_seconds = max(2.0, float(timeout_seconds_raw))
    except ValueError:
        timeout_seconds = 35.0
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
        return {"ok": False, "details": {}, "warnings": ["Metadata cleanup skipped because dag_id is empty."]}

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
        ("dag_tags", [("airflow.models.dag", "DagTag"), ("airflow.models.dagtag", "DagTag")]),
        ("dag_code", [("airflow.models.dagcode", "DagCode")]),
        ("dag_models", [("airflow.models.dag", "DagModel"), ("airflow.models.dagmodel", "DagModel")]),
        ("parse_import_errors", [("airflow.models.errors", "ParseImportError"), ("airflow.models.errors", "ImportError")]),
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
                        query = query.filter(getattr(model, "filename").like(f"%{did}%"))
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
    existing_auto_mapping_paths = _collect_existing_auto_mapping_paths(config_path, flow_dir)

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

        def _finalize_promote_response(*, no_op: bool = False) -> dict[str, Any]:
            revision_state = get_dag_revisions(did)
            auto_tags: list[str] = []
            try:
                rel = config_path.resolve().relative_to(_projects_root().resolve())
                if len(rel.parts) >= 4:
                    auto_tags = _derive_tags(rel.parts[0], rel.parts[1], rel.parts[2], rel.parts[3])
            except ValueError:
                auto_tags = []
            raw_cfg = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            user_tags = _normalize_custom_tags(raw_cfg.get("custom_tags") if isinstance(raw_cfg, dict) else [])
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
            return {
                "dag_id": did,
                "dag_path": dag_path.as_posix(),
                "config_path": config_path.as_posix(),
                "active_revision_id": revision_state.get("active_revision_id"),
                "revision_count": revision_state.get("count", 0),
                "promoted_revision_id": rid,
                "dag_dependencies": dag_dependencies,
                "no_op": no_op,
            }

        current_bundle_hash = str((rollback_bundle.get("hashes") or {}).get("bundle") or "")
        target_bundle_hash = str((((target_bundle.get("manifest") or {}).get("hashes") or {}).get("bundle") or ""))
        if current_bundle_hash and target_bundle_hash and current_bundle_hash == target_bundle_hash:
            return _finalize_promote_response(no_op=True)

        before_state = _airflow_parse_state(did)
        try:
            _apply_bundle_to_active(
                flow_dir=flow_dir,
                dag_path=dag_path,
                config_path=config_path,
                bundle=target_bundle,
            )
            if not _wait_for_parse_refresh(did, before_state):
                raise TimeoutError("Airflow parse dogrulamasi zaman asimina ugradi.")
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

        return _finalize_promote_response(no_op=False)


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
                if not isinstance(ref_config_path, Path) or not ref_config_path.is_file():
                    warnings.append(
                        f"Reference cleanup skipped (YAML missing): {ref_dag_id}"
                    )
                    continue
                try:
                    ref_cfg = _load_yaml_root(ref_config_path)
                    ref_deps = _normalize_dag_dependencies(ref_cfg.get("dag_dependencies"))
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
                    warnings.append(
                        f"Reference cleanup failed for {ref_dag_id}: {exc}"
                    )

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
                warnings.append(f"Mapping file could not be deleted: {mapping_path.as_posix()}")

        if config_path.is_file():
            if _best_effort_unlink(config_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(config_path.as_posix())
            else:
                warnings.append(f"YAML file could not be deleted: {config_path.as_posix()}")

        if dag_path.is_file():
            if _best_effort_unlink(dag_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(dag_path.as_posix())
            else:
                warnings.append(f"DAG file could not be deleted: {dag_path.as_posix()}")

        if history_root.exists():
            if _best_effort_rmtree(history_root):
                deleted_paths.append(history_root.as_posix())
            else:
                warnings.append(f"History directory could not be deleted: {history_root.as_posix()}")

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
                    warnings.append(f"Metadata file could not be deleted: {metadata_path.as_posix()}")

        if cleaned_reference_dags:
            for ref_dag_id in cleaned_reference_dags:
                ref_entry = scope_entries.get(ref_dag_id) or {}
                try:
                    _render_single_studio_dag_entry(ref_entry)
                except Exception as exc:
                    warnings.append(f"DAG render refresh failed for {ref_dag_id}: {exc}")

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
    source_type = payload.get("source_type", "table")
    source_schema = payload.get("source_schema")
    source_table = payload.get("source_table")
    source_conn_id = str(payload.get("source_conn_id") or "").strip()
    target_conn_id = str(payload.get("target_conn_id") or "").strip()
    target_schema = payload["target_schema"]
    target_table = payload["target_table"]
    load_method = payload.get("load_method", "create_if_not_exists_or_truncate")
    normalized_source_schema = str(source_schema or "").strip() or ("sql" if source_type == "sql" else "")
    normalized_source_table = str(source_table or "").strip() or ("query" if source_type == "sql" else "")

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
        "task_group_id": task_group_id,
        "source_schema": normalized_source_schema,
        "source_table": normalized_source_table,
        "source_type": source_type,
        "inline_sql": payload.get("inline_sql"),
        "column_mapping_mode": payload.get("column_mapping_mode", "source"),
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": payload.get("where"),
        "batch_size": int(payload.get("batch_size", 10000)),
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
    if source_type == "sql" and task["column_mapping_mode"] != "mapping_file":
        raise ValueError("column_mapping_mode='mapping_file' is required when source_type='sql'.")
    if payload.get("column_mapping_mode") == "mapping_file":
        task["mapping_file"] = _auto_mapping_relative_file(1, str(task_group_id))
    return task


def build_task_dict_for_validation_from_task(
    task_payload: dict[str, Any],
    *,
    source_conn_id: str,
    target_conn_id: str,
    task_index: int,
) -> dict[str, Any]:
    source_schema = str(task_payload.get("source_schema") or "").strip()
    source_table = str(task_payload.get("source_table") or "").strip()
    target_schema = str(task_payload.get("target_schema") or "").strip()
    target_table = str(task_payload.get("target_table") or "").strip()
    source_type = str(task_payload.get("source_type") or "table").strip() or "table"
    normalized_source_schema = source_schema or ("sql" if source_type == "sql" else "")
    normalized_source_table = source_table or ("query" if source_type == "sql" else "")
    load_method = (
        str(task_payload.get("load_method") or "create_if_not_exists_or_truncate").strip()
        or "create_if_not_exists_or_truncate"
    )
    task_group_id = str(task_payload.get("task_group_id") or "").strip() or _auto_task_group_id(
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
        "task_group_id": task_group_id,
        "source_schema": normalized_source_schema,
        "source_table": normalized_source_table,
        "source_type": source_type,
        "inline_sql": task_payload.get("inline_sql"),
        "column_mapping_mode": str(task_payload.get("column_mapping_mode") or "source").strip() or "source",
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": task_payload.get("where"),
        "batch_size": int(task_payload.get("batch_size", 10000)),
        "partitioning": {
            "enabled": bool(task_payload.get("partitioning_enabled", False)),
            "mode": task_payload.get("partitioning_mode", "auto_numeric"),
            "column": task_payload.get("partitioning_column"),
            "parts": int(task_payload.get("partitioning_parts", 2)),
            "distinct_limit": int(task_payload.get("partitioning_distinct_limit") or 16),
            "ranges": task_payload.get("partitioning_ranges") or [],
        },
    }
    bindings = _normalize_bindings(task_payload.get("bindings"))
    if bindings:
        task["bindings"] = bindings
    if source_type == "sql" and task["column_mapping_mode"] != "mapping_file":
        raise ValueError("column_mapping_mode='mapping_file' is required when source_type='sql'.")
    if task["column_mapping_mode"] == "mapping_file":
        task["mapping_file"] = _auto_mapping_relative_file(task_index, task_group_id)
    return task


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
            validator.validate(task)
            normalized_tasks.append(task)
        resolve_task_dependencies(normalized_tasks)
        return

    task = build_task_dict_for_validation(payload)
    validator.validate(task)
    resolve_task_dependencies([task])


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
                    "start_date": run.start_date.isoformat() if run.start_date else None,
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
        create_date_iso = _iso_or_none(create_date) or _dag_file_creation_fallback(fileloc_text)
        bucket = "external"
        relative_path: str | None = None
        folder_parts: list[str] = []

        is_root_file = fileloc_norm == root_norm
        is_under_root = bool(fileloc_norm) and fileloc_norm.startswith(root_prefix)
        if is_root_file or is_under_root:
            bucket = "dags_root"
            if is_under_root:
                rel = fileloc_norm[len(root_prefix) :]
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
) -> list[str]:
    """Returns the Variable key list from Airflow metadata."""
    from airflow.models import Variable
    from airflow.utils.session import create_session

    safe_limit = max(1, min(int(limit or 200), 1000))
    search_val = (search or "").strip().lower()

    with create_session() as session:
        q = session.query(Variable.key).order_by(Variable.key.asc())
        if search_val:
            q = q.filter(Variable.key.ilike(f"%{search_val}%"))
        rows = q.limit(safe_limit).all()

    keys = [str(row[0] or "") for row in rows if str(row[0] or "").strip()]
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
    case_insensitive_exact = [s for s in available_schemas if str(s or "").lower() == requested_lower]
    if len(case_insensitive_exact) == 1:
        return case_insensitive_exact[0]

    prefix_matches = [s for s in available_schemas if str(s or "").lower().startswith(requested_lower)]
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
    case_insensitive_exact = [t for t in available_tables if str(t or "").lower() == requested_lower]
    if len(case_insensitive_exact) == 1:
        return case_insensitive_exact[0]

    # Accept common UI/manual entry variations such as Event_Logs vs EventLogs.
    def _canon(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())

    requested_canon = _canon(requested)
    if requested_canon:
        canon_matches = [t for t in available_tables if _canon(str(t or "")) == requested_canon]
        if len(canon_matches) == 1:
            return canon_matches[0]
        if len(canon_matches) > 1:
            raise ValueError(
                f"Table '{requested}' birden fazla kanonik eslesme verdi: {', '.join(canon_matches[:5])}"
            )

    prefix_matches = [t for t in available_tables if str(t or "").lower().startswith(requested_lower)]
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
    items = tables[safe_offset : safe_offset + safe_limit]

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
        columns = dialect.get_table_schema(session.conn, resolved_schema, resolved_table)

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


def generate_mapping_preview(payload: dict[str, Any]) -> dict[str, Any]:
    source_type = str(payload.get("source_type") or "table").strip() or "table"
    source_conn_id = str(payload.get("source_conn_id") or "").strip()
    target_conn_id = str(payload.get("target_conn_id") or "").strip()
    if not source_conn_id or not target_conn_id:
        raise ValueError("source_conn_id and target_conn_id are required.")

    src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
    src_dialect = resolve_dialect(src_params["conn_type"])
    tgt_dialect = resolve_dialect(tgt_params["conn_type"])

    src_name = _dialect_name(src_dialect)
    tgt_name = _dialect_name(tgt_dialect)
    version = str(payload.get("version") or "v1").strip() or "v1"
    task_no = max(1, int(payload.get("task_no") or 1))
    task_group_id = str(payload.get("task_group_id") or "").strip()
    if not task_group_id:
        task_group_id = f"task_{task_no}"
    generated_mapping_file = _auto_mapping_relative_file(task_no, task_group_id)
    warnings: list[str] = []

    if source_type in {"table", "view"}:
        source_schema = str(payload.get("source_schema") or "").strip()
        source_table = str(payload.get("source_table") or "").strip()
        if not source_schema or not source_table:
            raise ValueError("source_schema and source_table are required when source_type=table|view.")
        with DBSession(src_params, src_dialect) as src_session:
            mapping_obj = MappingGenerator().generate(
                src_session.conn,
                src_dialect,
                tgt_dialect,
                source_schema,
                source_table,
                version=version,
            )
    elif source_type == "sql":
        inline_sql = str(payload.get("inline_sql") or "").strip()
        if not inline_sql:
            raise ValueError("inline_sql is required when source_type='sql'.")
        sql_cols = extract_sql_select_columns_for_conn(source_conn_id, inline_sql)
        mapping_obj, warnings = _build_mapping_from_columns(
            columns=sql_cols,
            src_dialect_name=src_name,
            tgt_dialect_name=tgt_name,
            version=version,
        )
    else:
        raise ValueError("source_type can only be table|view|sql.")

    mapping_text = _mapping_dump_text(mapping_obj)
    return {
        "mapping_content": mapping_text,
        "generated_mapping_file": generated_mapping_file,
        "warnings": warnings,
        "column_count": len(mapping_obj.get("columns") or []),
    }


def create_or_update_dag(
    payload: dict[str, Any],
    *,
    update: bool = False,
    dag_id: str | None = None,
) -> dict[str, Any]:
    validate_pipeline_payload(payload)

    project = _slugify(payload["project"], "default_project")
    domain = _slugify(payload["domain"], "default_domain")
    level = _slugify(payload["level"], "level1")
    flow = _slugify(payload["flow"], "src_to_stg")

    task_payloads = payload.get("flow_tasks")
    if isinstance(task_payloads, list) and task_payloads:
        tasks_input = [dict(item or {}) for item in task_payloads]
    else:
        tasks_input = [dict(payload)]

    lock_ctx = _dag_operation_lock(str(dag_id or "").strip()) if update else nullcontext()
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
                raise ValueError(
                    f"DAG to update not found: dag_id={update_dag_id}"
                )
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
            if (cfg_project, cfg_domain, cfg_level, cfg_flow) != (project, domain, level, flow):
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
            config_path = flow_dir / _build_yaml_filename(project, domain, level, flow, group_no)

        existing_auto_mapping_paths = _collect_existing_auto_mapping_paths(config_path, flow_dir)
        auto_tags = _derive_tags(project, domain, level, flow)
        user_tags = _normalize_custom_tags(payload.get("custom_tags"))
        tags = _merge_tags(auto_tags, user_tags)
        scheduler = normalize_scheduler(payload.get("scheduler"))
        if update and "dag_dependencies" not in payload:
            existing_cfg = _load_yaml_root(config_path)
            dag_dependencies = _normalize_dag_dependencies(existing_cfg.get("dag_dependencies"))
        else:
            dag_dependencies = _normalize_dag_dependencies(payload.get("dag_dependencies"))
        scope_entries = _collect_scope_studio_dag_entries(project, domain)
        dag_upstream_dag_ids = _validate_dag_dependencies_for_scope(
            project=project,
            domain=domain,
            dag_id=dag_path.stem,
            upstream_dag_ids=list(dag_dependencies.get("upstream_dag_ids") or []),
            scope_entries=scope_entries,
        )
        dag_dependencies = {"upstream_dag_ids": dag_upstream_dag_ids}
        actor = str(os.getenv("FFENGINE_STUDIO_ACTOR", "flow_studio")).strip() or "flow_studio"
        operation_warnings: list[str] = []

        task_cfgs: list[dict[str, Any]] = []
        sql_mapping_checks: list[dict[str, Any]] = []
        pending_mapping_writes: list[dict[str, Any]] = []
        for idx, item in enumerate(tasks_input, start=1):
            source_schema = str(item.get("source_schema") or "").strip()
            source_table = str(item.get("source_table") or "").strip()
            target_schema = str(item.get("target_schema") or "").strip()
            target_table = str(item.get("target_table") or "").strip()
            source_type = str(item.get("source_type") or "table").strip() or "table"
            normalized_source_schema = source_schema or ("sql" if source_type == "sql" else "")
            normalized_source_table = source_table or ("query" if source_type == "sql" else "")
            load_method = (
                str(item.get("load_method") or "create_if_not_exists_or_truncate").strip()
                or "create_if_not_exists_or_truncate"
            )
            task_group_id = str(item.get("task_group_id") or "").strip() or _auto_task_group_id(
                source_db=str(payload.get("source_conn_id") or ""),
                src_schema=normalized_source_schema,
                src_table=normalized_source_table,
                target_db=str(payload.get("target_conn_id") or ""),
                load_method=load_method,
                tgt_schema=target_schema,
                tgt_table=target_table,
                task_index=idx,
            )
            raw_depends_on = item.get("depends_on")
            if raw_depends_on is None:
                raw_depends_on = []
            if not isinstance(raw_depends_on, list):
                raise ValueError(f"depends_on must be a list: task_group_id={task_group_id}")
            task_cfg: dict[str, Any] = {
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
                "column_mapping_mode": str(item.get("column_mapping_mode") or "source").strip() or "source",
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
                    "distinct_limit": int(item.get("partitioning_distinct_limit") or 16),
                    "ranges": item.get("partitioning_ranges") or [],
                },
                "tags": tags,
            }
            bindings = _normalize_bindings(item.get("bindings"))
            if bindings:
                task_cfg["bindings"] = bindings
            mode = task_cfg["column_mapping_mode"]
            mapping_content = str(item.get("mapping_content") or "")
            if source_type == "sql" and mode != "mapping_file":
                raise ValueError("column_mapping_mode='mapping_file' is required when source_type='sql'.")
            if mode == "mapping_file":
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

        if sql_mapping_checks:
            for check in sql_mapping_checks:
                inline_sql = str(check.get("inline_sql") or "").strip()
                if not inline_sql:
                    raise ValueError(
                        f"inline_sql is required when source_type='sql'. task_group_id={check['task_group_id']}"
                    )
                sql_columns = [
                    col["name"] for col in extract_sql_select_columns_for_conn(payload["source_conn_id"], inline_sql)
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
                _parse_yaml_mapping_text(mapping_content, label=pending["mapping_path"].as_posix())
                normalized_text = mapping_content if mapping_content.endswith("\n") else f"{mapping_content}\n"
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
            }
            config_path.write_text(
                yaml.safe_dump(config_obj, sort_keys=False, allow_unicode=False),
                encoding="utf-8",
            )
            dag_source = _render_group_dag_source(
                dag_id=dag_path.stem,
                config_path=config_path,
                tags=tags,
                upstream_dag_ids=dag_upstream_dag_ids,
            )
            dag_path.write_text(dag_source, encoding="utf-8")
            if update:
                pause_sync_warning = _sync_dag_paused_state(dag_path.stem, active=bool(scheduler.get("active", True)))
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
            previous_hash = str((pre_update_bundle.get("hashes") or {}).get("bundle") or "")
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
