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


def _slugify(value: str, default: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip())
    cleaned = cleaned.strip("_").lower()
    return cleaned or default


def _auto_task_group_id(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    version: str = "v1",
) -> str:
    return (
        f"{_slugify(src_schema, 'src')}_{_slugify(src_table, 'table')}"
        f"_to_{_slugify(tgt_schema, 'tgt')}_{_slugify(tgt_table, 'table')}_{version}"
    )


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


def build_task_dict_for_validation(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Pipeline formundan (T06) ConfigValidator ile uyumlu task dict uretir.
    """
    source_schema = payload["source_schema"]
    source_table = payload["source_table"]
    target_schema = payload["target_schema"]
    target_table = payload["target_table"]
    source_type = payload.get("source_type", "table")
    load_method = payload.get("load_method", "create_if_not_exists_or_truncate")

    task_group_id = payload.get("task_group_id") or _auto_task_group_id(
        src_schema=source_schema,
        src_table=source_table or "sql_source",
        tgt_schema=target_schema,
        tgt_table=target_table,
    )

    task: dict[str, Any] = {
        "task_group_id": task_group_id,
        "source_schema": source_schema,
        "source_table": source_table,
        "source_type": source_type,
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
    if payload.get("column_mapping_mode") == "mapping_file":
        mf = (payload.get("mapping_file") or "").strip()
        if mf:
            task["mapping_file"] = mf
    return task


def validate_pipeline_payload(payload: dict[str, Any]) -> None:
    """Pipeline formu (T06): task kurallarini ConfigValidator ile dogrular."""
    task = build_task_dict_for_validation(payload)
    ConfigValidator().validate(task)


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


def _list_child_dirs(path: Path) -> list[str]:
    if not path.is_dir():
        return []
    items: list[str] = []
    try:
        for entry in path.iterdir():
            if entry.is_dir():
                items.append(entry.name)
    except OSError:
        return []
    return sorted(set(items))


def discover_hierarchy_options(
    project: str | None = None,
    domain: str | None = None,
    level: str | None = None,
) -> dict[str, list[str]]:
    """
    ETL Studio hiyerarsisi icin mevcut klasor seceneklerini dondurur.
    Hem projects root hem dag root taranir ve union alinir.
    """
    project_val = (project or "").strip()
    domain_val = (domain or "").strip()
    level_val = (level or "").strip()
    roots = [_projects_root(), _generated_dag_root()]

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


def discover_schemas(conn_id: str) -> list[str]:
    params = AirflowConnectionAdapter.get_connection_params(conn_id)
    dialect = resolve_dialect(params["conn_type"])
    with DBSession(params, dialect) as session:
        return dialect.list_schemas(session.conn)


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
        tables = dialect.list_tables(session.conn, schema)

    search_val = (search or "").strip().lower()
    if search_val:
        tables = [tbl for tbl in tables if search_val in tbl.lower()]

    safe_limit = max(1, min(int(limit or 50), 50))
    safe_offset = max(0, int(offset or 0))
    total = len(tables)
    items = tables[safe_offset : safe_offset + safe_limit]

    return {
        "schema": schema,
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
    group_no = int(payload["group_no"])
    if group_no < 1:
        raise ValueError("group_no pozitif bir tamsayi olmalidir.")

    source_schema = payload["source_schema"]
    source_table = payload["source_table"]
    target_schema = payload["target_schema"]
    target_table = payload["target_table"]
    source_type = payload.get("source_type", "table")
    load_method = payload.get("load_method", "create_if_not_exists_or_truncate")

    task_group_id = payload.get("task_group_id") or _auto_task_group_id(
        src_schema=source_schema,
        src_table=source_table or "sql_source",
        tgt_schema=target_schema,
        tgt_table=target_table,
    )

    root = _projects_root()
    flow_dir = root / project / domain / level / flow
    flow_dir.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(flow_dir, root)
    (flow_dir / "mappings").mkdir(parents=True, exist_ok=True)

    config_path = flow_dir / _build_yaml_filename(project, domain, level, flow, group_no)
    for legacy_yaml in _legacy_yaml_candidates(flow_dir, group_no):
        if legacy_yaml.is_file() and not config_path.exists():
            shutil.copy2(legacy_yaml, config_path)
            try:
                legacy_yaml.unlink()
            except OSError:
                pass

    gen_root = _generated_dag_root()
    flow_dag_dir = gen_root / project / domain / level / flow
    flow_dag_dir.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(flow_dag_dir, gen_root)
    dag_path = flow_dag_dir / _build_dag_filename(domain, level, flow, group_no)
    _ensure_path_under_root(dag_path, gen_root)

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

    if update and not dag_path.exists():
        raise ValueError(
            "Guncellenecek DAG bulunamadi veya ETL Studio tarafindan uretilmemis."
        )
    if update and not config_path.exists():
        raise ValueError("Guncellenecek YAML dosyasi bulunamadi.")
    if update and STUDIO_DAG_MARKER not in dag_path.read_text(encoding="utf-8"):
        raise ValueError("Bu DAG ETL Studio tarafindan uretilmemis.")

    tags = _derive_tags(project, domain, level, flow)

    task_cfg: dict[str, Any] = {
        "task_group_id": task_group_id,
        "source_schema": source_schema,
        "source_table": source_table,
        "source_type": source_type,
        "column_mapping_mode": payload.get("column_mapping_mode", "source"),
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": payload.get("where") or None,
        "batch_size": int(payload.get("batch_size", 10000)),
        "partitioning": {
            "enabled": bool(payload.get("partitioning_enabled", False)),
            "mode": payload.get("partitioning_mode", "auto"),
            "column": payload.get("partitioning_column") or None,
            "parts": int(payload.get("partitioning_parts", 2)),
            "ranges": payload.get("partitioning_ranges") or [],
        },
        "tags": tags,
    }
    mf = (payload.get("mapping_file") or "").strip()
    if mf:
        task_cfg["mapping_file"] = mf
    resolve_task_dependencies([task_cfg])

    config_obj = {
        "source_db_var": payload["source_conn_id"],
        "target_db_var": payload["target_conn_id"],
        "etl_tasks": [task_cfg],
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
        "task_group_id": task_group_id,
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
        "task_group_id": task_group_id,
    }
