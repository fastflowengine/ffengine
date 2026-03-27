"""
ETL Studio MVP servis katmani.

Faz 1 (T01-T04, T07, T11) ve Faz 2 (T05-T10, T08-T09, T12) endpoint'leri bu modulu kullanir.
"""

from __future__ import annotations

import json
import os
import re
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


def _derive_tags(domain: str, level: str, direction: str) -> list[str]:
    return [
        _slugify(domain, "domain"),
        _slugify(level, "level1"),
        _slugify(direction, "src_to_stg"),
    ]


def _projects_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_PROJECTS_ROOT", "/opt/airflow/dags/projects")
    return Path(root)


def _generated_dag_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags/generated")
    return Path(root)


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
    project = _slugify(payload.get("project", "default_project"), "default_project")
    domain = _slugify(payload.get("domain", "default_domain"), "default_domain")
    level = _slugify(payload.get("level", "level1"), "level1")
    direction = _slugify(payload.get("direction", "src_to_stg"), "src_to_stg")
    source_schema = payload["source_schema"]
    source_table = payload.get("source_table") or ""
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
        "source_table": source_table or None,
        "source_type": source_type,
        "column_mapping_mode": payload.get("column_mapping_mode", "source"),
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": payload.get("where"),
        "batch_size": int(payload.get("batch_size", 10000)),
        "pipe_queue_max": int(payload.get("pipe_queue_max", 8)),
        "reader_workers": int(payload.get("reader_workers", 3)),
        "writer_workers": int(payload.get("writer_workers", 5)),
        "extraction_method": payload.get("extraction_method", "auto"),
        "partitioning": {
            "enabled": bool(payload.get("partitioning_enabled", False)),
            "mode": payload.get("partitioning_mode", "auto"),
            "column": payload.get("partitioning_column"),
            "parts": int(payload.get("partitioning_parts", 2)),
            "ranges": payload.get("partitioning_ranges") or [],
        },
        "passthrough_full": payload.get("passthrough_full", True),
    }
    if payload.get("column_mapping_mode") == "mapping_file":
        mf = (payload.get("mapping_file") or "").strip()
        if mf:
            task["mapping_file"] = mf
    sql_text = (payload.get("sql_text") or "").strip()
    if source_type == "sql" and sql_text:
        task["inline_sql"] = sql_text
    if payload.get("sql_file"):
        task["sql_file"] = payload["sql_file"]
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

    project = _slugify(payload.get("project", "default_project"), "default_project")
    domain = _slugify(payload.get("domain", "default_domain"), "default_domain")
    level = _slugify(payload.get("level", "level1"), "level1")
    direction = _slugify(payload.get("direction", "src_to_stg"), "src_to_stg")

    source_schema = payload["source_schema"]
    source_table = payload.get("source_table", "")
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
    flow_dir = root / project / domain / level / direction
    flow_dir.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(flow_dir, root)
    (flow_dir / "sql").mkdir(parents=True, exist_ok=True)
    (flow_dir / "mappings").mkdir(parents=True, exist_ok=True)

    config_path = flow_dir / "config.yaml"
    gen_root = _generated_dag_root()
    dag_path = gen_root / f"{project}_{domain}_{level}_{direction}.py"
    dag_path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_path_under_root(dag_path, gen_root)

    if update and not dag_path.exists():
        raise ValueError(
            "Guncellenecek DAG bulunamadi veya ETL Studio tarafindan uretilmemis."
        )
    if update and STUDIO_DAG_MARKER not in dag_path.read_text(encoding="utf-8"):
        raise ValueError("Bu DAG ETL Studio tarafindan uretilmemis.")

    sql_file = None
    sql_text = (payload.get("sql_text") or "").strip()
    if source_type == "sql" and sql_text:
        sql_filename = f"{task_group_id}.sql"
        sql_rel = Path("sql") / sql_filename
        (flow_dir / sql_rel).write_text(sql_text + "\n", encoding="utf-8")
        sql_file = str(sql_rel).replace("\\", "/")

    auto_tags = _derive_tags(domain, level, direction)
    meta_prev = _load_studio_metadata(flow_dir) if update else None
    prev_user = (meta_prev or {}).get("user_tags") or []
    if update:
        if "tags" not in payload:
            user_tags = prev_user
        else:
            user_tags = [
                str(t).strip() for t in (payload.get("tags") or []) if str(t).strip()
            ]
    else:
        user_tags = [
            str(t).strip() for t in (payload.get("tags") or []) if str(t).strip()
        ]
    tags = list(dict.fromkeys(auto_tags + user_tags))

    task_cfg: dict[str, Any] = {
        "task_group_id": task_group_id,
        "source_schema": source_schema,
        "source_table": source_table or None,
        "source_type": source_type,
        "column_mapping_mode": payload.get("column_mapping_mode", "source"),
        "target_schema": target_schema,
        "target_table": target_table,
        "load_method": load_method,
        "where": payload.get("where") or None,
        "batch_size": int(payload.get("batch_size", 10000)),
        "pipe_queue_max": int(payload.get("pipe_queue_max", 8)),
        "reader_workers": int(payload.get("reader_workers", 3)),
        "writer_workers": int(payload.get("writer_workers", 5)),
        "extraction_method": payload.get("extraction_method", "auto"),
        "passthrough_full": payload.get("passthrough_full", True),
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
    if sql_file:
        task_cfg["sql_file"] = sql_file

    config_obj = {
        "source_db_var": payload["source_conn_id"],
        "target_db_var": payload["target_conn_id"],
        "etl_tasks": [task_cfg],
    }
    config_path.write_text(
        yaml.safe_dump(config_obj, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )

    dag_prefix = _slugify(payload.get("dag_prefix", "ffengine"), "ffengine")
    dag_source = f'''{STUDIO_DAG_MARKER}
from ffengine.airflow.dag_generator import register_dags

register_dags(
    "{flow_dir.as_posix()}",
    globals(),
    dag_prefix="{dag_prefix}",
)
'''
    dag_path.write_text(dag_source, encoding="utf-8")

    metadata = {
        "flow_dir": flow_dir.as_posix(),
        "config_path": config_path.as_posix(),
        "dag_path": dag_path.as_posix(),
        "task_group_id": task_group_id,
        "tags": tags,
        "auto_tags": auto_tags,
        "user_tags": user_tags,
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
