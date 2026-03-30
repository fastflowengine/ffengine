"""
ETL Studio MVP servis katmani.

Faz 1 (T01-T04, T07, T11) ve Faz 2 (T05-T10, T08-T09, T12) endpoint'leri bu modulu kullanir.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import Any

import yaml

from ffengine.airflow.operator import resolve_dialect
from ffengine.config.validator import ConfigValidator
from ffengine.db.airflow_adapter import AirflowConnectionAdapter
from ffengine.db.session import DBSession

STUDIO_METADATA_NAME = ".etl_studio.json"
STUDIO_DAG_MARKER = "# generated_by: etl_studio"
LEGACY_DAG_ID_PREFIXES = ("ffengine_config_", "ffengine_")


def _slugify(value: str, default: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_").lower()
    return cleaned or default


def _auto_task_group_id(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    task_index: int = 1,
) -> str:
    idx = max(1, int(task_index))
    return (
        f"{_slugify(src_schema, 'src')}_{_slugify(src_table, 'table')}"
        f"_to_{_slugify(tgt_schema, 'tgt')}_{_slugify(tgt_table, 'table')}_task_{idx}"
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


def _extract_flow_target(flow: str) -> str:
    """src_to_stg -> stg, stg_to_dwh -> dwh, fallback -> flow slug."""
    raw = _slugify(flow, "flow")
    if "_to_" in raw:
        right = raw.split("_to_")[-1].strip("_")
        if right:
            return right
    return raw


def _build_dag_filename(domain: str, level: str, flow: str, group_no: int) -> str:
    domain_slug = _slugify(domain, "domain")
    level_slug = _slugify(level, "level1")
    flow_target = _extract_flow_target(flow)
    return f"{domain_slug}_to_{flow_target}_{level_slug}_group_{int(group_no)}_dag.py"


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


def _find_existing_studio_dag(flow_dag_dir: Path) -> Path | None:
    if not flow_dag_dir.is_dir():
        return None
    matches = []
    for item in sorted(flow_dag_dir.glob("*_dag.py")):
        try:
            if STUDIO_DAG_MARKER in item.read_text(encoding="utf-8"):
                matches.append(item)
        except OSError:
            continue
    if len(matches) > 1:
        raise ValueError(
            "Ayni flow klasorunde birden fazla ETL Studio DAG dosyasi bulundu."
        )
    return matches[0] if matches else None


def _projects_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_PROJECTS_ROOT", "/opt/airflow/projects")
    return Path(root)


def _generated_dag_root() -> Path:
    root = Path(os.getenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags"))
    # Legacy değer "/.../dags/generated" geldiyse mirror model için "/.../dags" kullan.
    if root.name == "generated":
        return root.parent
    return root


def _legacy_generated_dir(gen_root: Path) -> Path:
    return gen_root / "generated"


def _legacy_dag_filename(project: str, domain: str, level: str, flow: str) -> str:
    return f"{project}_{domain}_{level}_{flow}.py"


def _legacy_yaml_candidates(flow_dir: Path, group_no: int) -> list[Path]:
    return [
        flow_dir / f"config_group_{int(group_no)}.yaml",
        flow_dir / "config.yaml",
    ]


def _find_legacy_studio_dag(
    gen_root: Path,
    project: str,
    domain: str,
    level: str,
    flow: str,
) -> Path | None:
    legacy_dir = _legacy_generated_dir(gen_root)
    legacy_path = legacy_dir / _legacy_dag_filename(project, domain, level, flow)
    if not legacy_path.is_file():
        return None
    try:
        if STUDIO_DAG_MARKER in legacy_path.read_text(encoding="utf-8"):
            return legacy_path
    except OSError:
        return None
    return None


def resolve_task_dependencies(task_defs: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    etl_tasks icin bagimlilik kenarlarini uretir.
    - depends_on varsa onu kullanir.
    - depends_on yoksa YAML sirasina gore zincirler.
    """
    if not isinstance(task_defs, list):
        raise ValueError("etl_tasks bir liste olmalidir.")

    task_ids: list[str] = []
    id_set: set[str] = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Her etl_task bir dict olmalidir.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("Her etl_task icin task_group_id zorunludur.")
        if task_id in id_set:
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {task_id}")
        task_ids.append(task_id)
        id_set.add(task_id)

    edges: list[tuple[str, str]] = []
    previous_task_id: str | None = None
    for idx, task in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task.get("depends_on")
        if depends_on is None:
            if previous_task_id is not None:
                edges.append((previous_task_id, task_id))
        else:
            if not isinstance(depends_on, list):
                raise ValueError(
                    f"depends_on list olmalidir: task_group_id={task_id}"
                )
            for dep in depends_on:
                dep_id = str(dep or "").strip()
                if not dep_id:
                    continue
                if dep_id not in id_set:
                    raise ValueError(
                        f"depends_on gecersiz task_group_id iceriyor: {dep_id}"
                    )
                edges.append((dep_id, task_id))
        previous_task_id = task_id

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


def _render_group_dag_source(
    *,
    dag_id: str,
    config_path: Path,
    tags: list[str],
) -> str:
    cfg = json.dumps(config_path.as_posix())
    did = json.dumps(dag_id)
    dtags = json.dumps(tags)
    return f'''{STUDIO_DAG_MARKER}
import json
from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG

from ffengine.airflow.operator import FFEngineOperator

CONFIG_PATH = Path({cfg})
DAG_ID = {did}
DAG_TAGS = {dtags}


def _resolve_task_dependencies(task_defs):
    task_ids = []
    id_set = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Her etl_task bir dict olmalidir.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("Her etl_task icin task_group_id zorunludur.")
        if task_id in id_set:
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {{task_id}}")
        task_ids.append(task_id)
        id_set.add(task_id)

    edges = []
    previous_task_id = None
    for idx, task in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task.get("depends_on")
        if depends_on is None:
            if previous_task_id is not None:
                edges.append((previous_task_id, task_id))
        else:
            if not isinstance(depends_on, list):
                raise ValueError(f"depends_on list olmalidir: task_group_id={{task_id}}")
            for dep in depends_on:
                dep_id = str(dep or "").strip()
                if not dep_id:
                    continue
                if dep_id not in id_set:
                    raise ValueError(f"depends_on gecersiz task_group_id iceriyor: {{dep_id}}")
                edges.append((dep_id, task_id))
        previous_task_id = task_id

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
    raise ValueError("YAML root dict olmalidir.")

source_conn_id = str(raw.get("source_db_var") or "").strip()
target_conn_id = str(raw.get("target_db_var") or "").strip()
task_defs = raw.get("etl_tasks") or []

if not source_conn_id or not target_conn_id:
    raise ValueError("source_db_var ve target_db_var zorunludur.")
if not isinstance(task_defs, list) or not task_defs:
    raise ValueError("etl_tasks en az bir task iceren list olmalidir.")

edges = _resolve_task_dependencies(task_defs)

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=DAG_TAGS,
) as dag:
    operators = {{}}
    for task in task_defs:
        task_group_id = str(task.get("task_group_id") or "").strip()
        operators[task_group_id] = FFEngineOperator(
            config_path=str(CONFIG_PATH),
            task_group_id=task_group_id,
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            task_id=f"run_{{task_group_id}}",
        )
    for upstream, downstream in edges:
        operators[upstream] >> operators[downstream]
'''


def _ensure_path_under_root(path: Path, root: Path) -> Path:
    """Path traversal korumasi: path root altinda olmalidir."""
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"Gecersiz path: {path!s}") from exc
    return resolved


def _load_studio_metadata(flow_dir: Path) -> dict[str, Any] | None:
    meta_path = flow_dir / STUDIO_METADATA_NAME
    if not meta_path.is_file():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _is_legacy_dag_id(dag_id: str) -> bool:
    did = (dag_id or "").strip().lower()
    return any(did.startswith(prefix) for prefix in LEGACY_DAG_ID_PREFIXES)


def _extract_group_no(dag_id: str, config_path: Path) -> int:
    match = re.search(r"_group_(\d+)_dag$", dag_id)
    if match:
        return int(match.group(1))
    cfg_match = re.search(r"_group_(\d+)\.ya?ml$", config_path.name)
    if cfg_match:
        return int(cfg_match.group(1))
    raise ValueError("group_no dag_id/config isminden cozumlenemedi.")


def _extract_config_path_from_dag_source(dag_path: Path) -> Path:
    source = dag_path.read_text(encoding="utf-8")
    if STUDIO_DAG_MARKER not in source:
        raise ValueError("Bu DAG ETL Studio tarafindan uretilmemis.")
    match = re.search(
        r"CONFIG_PATH\s*=\s*Path\((['\"])(?P<path>.+?)\1\)",
        source,
    )
    if not match:
        raise ValueError("DAG icinde CONFIG_PATH cozumlenemedi.")
    return Path(match.group("path"))


def _find_studio_dag_file_by_id(dag_id: str) -> Path | None:
    gen_root = _generated_dag_root()
    candidate_name = f"{dag_id}.py"
    for path in gen_root.rglob(candidate_name):
        if path.is_file():
            return path
    return None


def resolve_dag_config_for_update(dag_id: str) -> dict[str, Any]:
    did = (dag_id or "").strip()
    if not did:
        raise ValueError("dag_id zorunludur.")

    dag_path = _find_studio_dag_file_by_id(did)
    if dag_path is None:
        if _is_legacy_dag_id(did):
            return {
                "dag_id": did,
                "supported_for_update": False,
                "reason": "legacy_dag_id_not_supported",
                "migration_hint": (
                    "Bu DAG legacy formatta. ETL Studio'da yeni naming ile "
                    "'Create DAG + YAML' calistirip yeni DAG uzerinden update edin."
                ),
                "migration_url": "/etl-studio/",
            }
        raise FileNotFoundError(f"DAG bulunamadi: {did}")

    try:
        config_path = _extract_config_path_from_dag_source(dag_path)
    except ValueError:
        if _is_legacy_dag_id(did):
            return {
                "dag_id": did,
                "supported_for_update": False,
                "reason": "legacy_dag_id_not_supported",
                "migration_hint": (
                    "Bu DAG legacy formatta. ETL Studio'da yeni naming ile "
                    "'Create DAG + YAML' calistirip yeni DAG uzerinden update edin."
                ),
                "migration_url": "/etl-studio/",
            }
        raise
    if not config_path.is_file():
        raise ValueError("DAG bulundu ancak bagli YAML dosyasi bulunamadi.")

    projects_root = _projects_root().resolve()
    config_resolved = config_path.resolve()
    try:
        rel = config_resolved.relative_to(projects_root)
    except ValueError as exc:
        raise ValueError("YAML path ETL Studio projects root altinda degil.") from exc
    if len(rel.parts) < 5:
        raise ValueError("YAML path hiyerarsisi gecersiz.")
    project, domain, level, flow = rel.parts[:4]

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("YAML root dict olmalidir.")
    tasks = raw.get("etl_tasks") or []
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("YAML etl_tasks listesi bos veya gecersiz.")
    normalized_tasks: list[dict[str, Any]] = []
    for idx, task in enumerate(tasks, start=1):
        if not isinstance(task, dict):
            raise ValueError(f"etl_tasks[{idx-1}] dict olmalidir.")
        partitioning = task.get("partitioning") or {}
        if not isinstance(partitioning, dict):
            partitioning = {}
        normalized_tasks.append(
            {
                "task_group_id": str(task.get("task_group_id") or "").strip() or None,
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
                "where": str(task.get("where") or "").strip() or None,
                "batch_size": int(task.get("batch_size") or 10000),
                "partitioning_enabled": bool(partitioning.get("enabled", False)),
                "partitioning_mode": str(partitioning.get("mode") or "auto").strip() or "auto",
                "partitioning_column": str(partitioning.get("column") or "").strip() or None,
                "partitioning_parts": int(partitioning.get("parts") or 2),
                "partitioning_ranges": partitioning.get("ranges") or [],
                "bindings": _normalize_bindings(task.get("bindings")),
            }
        )

    first_task = normalized_tasks[0]

    payload = {
        "project": project,
        "domain": domain,
        "level": level,
        "flow": flow,
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
        "where": first_task["where"],
        "batch_size": first_task["batch_size"],
        "partitioning_enabled": first_task["partitioning_enabled"],
        "partitioning_mode": first_task["partitioning_mode"],
        "partitioning_column": first_task["partitioning_column"],
        "partitioning_parts": first_task["partitioning_parts"],
        "partitioning_ranges": first_task["partitioning_ranges"],
        "bindings": first_task["bindings"],
        "etl_tasks": normalized_tasks,
    }

    return {
        "dag_id": did,
        "supported_for_update": True,
        "reason": None,
        "migration_hint": None,
        "migration_url": None,
        "payload": payload,
        "dag_path": dag_path.as_posix(),
        "config_path": config_path.as_posix(),
    }


def build_task_dict_for_validation(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Pipeline formundan (T06) ConfigValidator ile uyumlu task dict uretir.
    """
    source_type = payload.get("source_type", "table")
    source_schema = payload.get("source_schema")
    source_table = payload.get("source_table")
    target_schema = payload["target_schema"]
    target_table = payload["target_table"]
    load_method = payload.get("load_method", "create_if_not_exists_or_truncate")
    normalized_source_schema = str(source_schema or "").strip() or ("sql" if source_type == "sql" else "")
    normalized_source_table = str(source_table or "").strip() or ("query" if source_type == "sql" else "")

    task_group_id = payload.get("task_group_id") or _auto_task_group_id(
        src_schema=normalized_source_schema,
        src_table=normalized_source_table,
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
            "mode": payload.get("partitioning_mode", "auto"),
            "column": payload.get("partitioning_column"),
            "parts": int(payload.get("partitioning_parts", 2)),
            "ranges": payload.get("partitioning_ranges") or [],
        },
    }
    bindings = _normalize_bindings(payload.get("bindings"))
    if bindings:
        task["bindings"] = bindings
    if payload.get("column_mapping_mode") == "mapping_file":
        mf = (payload.get("mapping_file") or "").strip()
        if mf:
            task["mapping_file"] = mf
    return task


def build_task_dict_for_validation_from_task(
    task_payload: dict[str, Any],
    *,
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
        src_schema=normalized_source_schema,
        src_table=normalized_source_table,
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
            "mode": task_payload.get("partitioning_mode", "auto"),
            "column": task_payload.get("partitioning_column"),
            "parts": int(task_payload.get("partitioning_parts", 2)),
            "ranges": task_payload.get("partitioning_ranges") or [],
        },
    }
    bindings = _normalize_bindings(task_payload.get("bindings"))
    if bindings:
        task["bindings"] = bindings
    if task["column_mapping_mode"] == "mapping_file":
        mf = str(task_payload.get("mapping_file") or "").strip()
        if mf:
            task["mapping_file"] = mf
    return task


def validate_pipeline_payload(payload: dict[str, Any]) -> None:
    """Pipeline formu (T06): task kurallarini ConfigValidator ile dogrular."""
    validator = ConfigValidator()
    task_items = payload.get("etl_tasks")
    if isinstance(task_items, list) and task_items:
        normalized_tasks: list[dict[str, Any]] = []
        for idx, task_payload in enumerate(task_items, start=1):
            task = build_task_dict_for_validation_from_task(
                dict(task_payload or {}),
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
    """DagRun listesi (T10): filtreler opsiyonel."""
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


def discover_connections() -> list[dict[str, str]]:
    """Airflow metadata'dan tanimli connection listesini dondurur."""
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
    """Airflow metadata'dan Variable key listesini dondurur."""
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
    ETL Studio hiyerarsisi icin mevcut klasor seceneklerini dondurur.
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
        raise ValueError("source yalnizca 'dag', 'projects' veya 'union' olabilir.")

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
        raise ValueError("Schema degeri bos olamaz.")
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

    raise ValueError(f"Schema bulunamadi: {requested}")


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
        columns = dialect.get_table_schema(session.conn, schema, table)

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


def create_or_update_dag(payload: dict[str, Any], *, update: bool = False) -> dict[str, str]:
    validate_pipeline_payload(payload)

    project = _slugify(payload["project"], "default_project")
    domain = _slugify(payload["domain"], "default_domain")
    level = _slugify(payload["level"], "level1")
    flow = _slugify(payload["flow"], "src_to_stg")

    task_payloads = payload.get("etl_tasks")
    if isinstance(task_payloads, list) and task_payloads:
        tasks_input = [dict(item or {}) for item in task_payloads]
    else:
        tasks_input = [dict(payload)]

    root = _projects_root()
    flow_dir = root / project / domain / level / flow
    flow_dir.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(flow_dir, root)
    (flow_dir / "mappings").mkdir(parents=True, exist_ok=True)

    gen_root = _generated_dag_root()
    flow_dag_dir = gen_root / project / domain / level / flow
    flow_dag_dir.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(flow_dag_dir, gen_root)

    existing_studio_dag = _find_existing_studio_dag(flow_dag_dir)
    if update:
        if existing_studio_dag is None:
            raise ValueError(
                "Guncellenecek DAG bulunamadi veya ETL Studio tarafindan uretilmemis."
            )
        dag_path = existing_studio_dag
        group_no = _extract_group_no_from_name(dag_path.stem)
        if group_no is None:
            raise ValueError("Mevcut DAG dosyasindan group_no cozumlenemedi.")
    else:
        group_no = _next_group_no(flow_dir, flow_dag_dir)
        dag_path = flow_dag_dir / _build_dag_filename(domain, level, flow, group_no)
    _ensure_path_under_root(dag_path, gen_root)

    config_path = flow_dir / _build_yaml_filename(project, domain, level, flow, group_no)
    for legacy_yaml in _legacy_yaml_candidates(flow_dir, group_no):
        if legacy_yaml.is_file() and not config_path.exists():
            shutil.copy2(legacy_yaml, config_path)
            try:
                legacy_yaml.unlink()
            except OSError:
                pass

    legacy_dag = _find_legacy_studio_dag(
        gen_root=gen_root,
        project=project,
        domain=domain,
        level=level,
        flow=flow,
    )
    if legacy_dag is not None:
        try:
            legacy_dag.unlink()
        except OSError:
            pass
    old_mirror_legacy = flow_dag_dir / _legacy_dag_filename(project, domain, level, flow)
    if old_mirror_legacy.is_file() and old_mirror_legacy != dag_path:
        try:
            old_mirror_legacy.unlink()
        except OSError:
            pass

    if update and not config_path.exists():
        raise ValueError("Guncellenecek YAML dosyasi bulunamadi.")
    if update and STUDIO_DAG_MARKER not in dag_path.read_text(encoding="utf-8"):
        raise ValueError("Bu DAG ETL Studio tarafindan uretilmemis.")

    tags = _derive_tags(project, domain, level, flow)

    task_cfgs: list[dict[str, Any]] = []
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
            src_schema=normalized_source_schema,
            src_table=normalized_source_table,
            tgt_schema=target_schema,
            tgt_table=target_table,
            task_index=idx,
        )
        task_cfg: dict[str, Any] = {
            "task_group_id": task_group_id,
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
                "mode": item.get("partitioning_mode", "auto"),
                "column": item.get("partitioning_column") or None,
                "parts": int(item.get("partitioning_parts", 2)),
                "ranges": item.get("partitioning_ranges") or [],
            },
            "tags": tags,
        }
        bindings = _normalize_bindings(item.get("bindings"))
        if bindings:
            task_cfg["bindings"] = bindings
        mf = str(item.get("mapping_file") or "").strip()
        if mf:
            task_cfg["mapping_file"] = mf
        task_cfgs.append(task_cfg)

    resolve_task_dependencies(task_cfgs)

    config_obj = {
        "source_db_var": payload["source_conn_id"],
        "target_db_var": payload["target_conn_id"],
        "etl_tasks": task_cfgs,
    }
    config_path.write_text(
        yaml.safe_dump(config_obj, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    dag_source = _render_group_dag_source(
        dag_id=dag_path.stem,
        config_path=config_path,
        tags=tags,
    )
    dag_path.write_text(dag_source, encoding="utf-8")

    metadata = {
        "flow_dir": flow_dir.as_posix(),
        "config_path": config_path.as_posix(),
        "dag_path": dag_path.as_posix(),
        "task_group_id": task_cfgs[0]["task_group_id"],
        "task_count": len(task_cfgs),
        "group_no": group_no,
        "tags": tags,
        "auto_tags": tags,
        "user_tags": [],
    }
    (flow_dir / STUDIO_METADATA_NAME).write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    return {
        "flow_dir": metadata["flow_dir"],
        "config_path": metadata["config_path"],
        "dag_path": metadata["dag_path"],
        "task_group_id": task_cfgs[0]["task_group_id"],
    }
