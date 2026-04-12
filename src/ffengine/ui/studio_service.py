"""
Flow Studio MVP servis katmani.

Faz 1 (T01-T04, T07, T11) ve Faz 2 (T05-T10, T08-T09, T12) endpoint'leri bu modulu kullanir.
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


def _projects_root() -> Path:
    root = os.getenv("FFENGINE_STUDIO_PROJECTS_ROOT", "/opt/airflow/projects")
    return Path(root)


def _generated_dag_root() -> Path:
    return Path(os.getenv("FFENGINE_STUDIO_DAG_ROOT", "/opt/airflow/dags"))


def resolve_task_dependencies(task_defs: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    flow_tasks icin bagimlilik kenarlarini uretir.
    - depends_on varsa onu kullanir.
    - depends_on yoksa YAML sirasina gore zincirler.
    """
    if not isinstance(task_defs, list):
        raise ValueError("flow_tasks bir liste olmalidir.")

    task_ids: list[str] = []
    id_set: set[str] = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Her flow_task bir dict olmalidir.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("Her flow_task icin task_group_id zorunludur.")
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
            raise ValueError("Her flow_task bir dict olmalidir.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("Her flow_task icin task_group_id zorunludur.")
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
task_defs = raw.get("flow_tasks") or []

if not source_conn_id or not target_conn_id:
    raise ValueError("source_db_var ve target_db_var zorunludur.")
if not isinstance(task_defs, list) or not task_defs:
    raise ValueError("flow_tasks en az bir task iceren list olmalidir.")

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
        raise ValueError("mapping_file bos olamaz.")
    path = Path(raw)
    if path.is_absolute():
        raise ValueError("mapping_file relative bir yol olmalidir.")
    return Path(raw).as_posix()


def _auto_mapping_relative_file(task_no: int, task_group_id: str) -> str:
    safe_task_no = max(1, int(task_no))
    tg = str(task_group_id or "").strip()
    if not tg:
        raise ValueError("task_group_id bos olamaz.")
    if "/" in tg or "\\" in tg or ".." in tg:
        raise ValueError(f"Gecersiz task_group_id (mapping path icin): {tg!r}")
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
        raise ValueError("Mapping YAML root dict olmalidir.")
    version = mapping_obj.get("version")
    if version not in VALID_MAPPING_VERSIONS:
        raise ValueError(
            f"Desteklenmeyen mapping dosyasi versiyonu: {version!r}. "
            f"Gecerli: {sorted(VALID_MAPPING_VERSIONS)}"
        )
    entries = mapping_obj.get("columns")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Mapping YAML icinde columns listesi bos veya gecersiz.")
    out: list[str] = []
    for idx, item in enumerate(entries, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Mapping columns[{idx-1}] dict olmalidir.")
        src = str(item.get("source_name") or "").strip()
        if not src:
            raise ValueError(f"Mapping columns[{idx-1}] source_name bos olamaz.")
        out.append(src)
    return out


def _parse_yaml_mapping_text(mapping_content: str, *, label: str) -> dict[str, Any]:
    try:
        parsed = yaml.safe_load(mapping_content)
    except yaml.YAMLError as exc:
        raise ValueError(f"Gecersiz mapping YAML ({label}): {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Gecersiz mapping YAML ({label}): root dict olmalidir.")
    _mapping_yaml_to_source_columns(parsed)
    return parsed


def _read_mapping_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Mapping dosyasi bulunamadi: {path.as_posix()}")
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
        raise ValueError("source_type='sql' icin inline_sql zorunludur.")
    if dialect_name == "mssql":
        return f"SELECT TOP 0 * FROM ({base}) AS ffengine_inline_sql"
    if dialect_name == "oracle":
        return f"SELECT * FROM ({base}) ffengine_inline_sql WHERE 1=0"
    return f"SELECT * FROM ({base}) AS ffengine_inline_sql LIMIT 0"


def extract_sql_select_columns(src_session: DBSession, src_dialect, inline_sql: str) -> list[dict[str, str]]:
    """SQL query metadata'sindan kolon adlarini ve normalize tip adini dondurur."""
    dialect_name = _dialect_name(src_dialect)
    query = _wrap_zero_row_sql_for_dialect(inline_sql, dialect_name)
    cursor = src_session.cursor(server_side=False)
    try:
        cursor.execute(query)
        desc = list(cursor.description or [])
    except Exception as exc:
        raise ValueError(f"SQL metadata cikarimi basarisiz: {exc}") from exc
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
        raise ValueError("SQL metadata cikariminda kolon bulunamadi.")
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
            f"Gecersiz mapping versiyonu: {version!r}. "
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
                f"{src_name}: source_type={src_type!r} cozulemedi, target_type={tgt_type!r} fallback uygulandi."
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
        raise ValueError("Mapping uretimi icin kullanilabilir kolon bulunamadi.")
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
        raise FileNotFoundError(f"DAG dosyasi bulunamadi: {dag_path.as_posix()}")
    if not config_path.is_file():
        raise FileNotFoundError(f"YAML dosyasi bulunamadi: {config_path.as_posix()}")

    dag_text = dag_path.read_text(encoding="utf-8")
    config_text = config_path.read_text(encoding="utf-8")
    config_obj = yaml.safe_load(config_text) or {}
    if not isinstance(config_obj, dict):
        raise ValueError("YAML root dict olmalidir.")

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
        raise FileNotFoundError(f"Revision manifest bulunamadi: {manifest_path.as_posix()}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dag_file = revision_dir / "dag.py"
    cfg_file = revision_dir / "config.yaml"
    if not dag_file.is_file() or not cfg_file.is_file():
        raise FileNotFoundError("Revision icinde dag.py veya config.yaml eksik.")
    mapping_texts: dict[str, str] = {}
    for rel in manifest.get("mapping_files") or []:
        rel_path = _normalize_relative_mapping_file(str(rel or ""))
        src = revision_dir / rel_path
        if not src.is_file():
            raise FileNotFoundError(f"Revision mapping dosyasi eksik: {rel_path}")
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
    raise ValueError("group_no dag_id/config isminden cozumlenemedi.")


def _extract_config_path_from_dag_source(dag_path: Path) -> Path:
    source = dag_path.read_text(encoding="utf-8")
    if STUDIO_DAG_MARKER not in source:
        raise ValueError("Bu DAG Flow Studio tarafindan uretilmemis.")
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
        raise ValueError("dag_id zorunludur.")

    dag_path = _find_studio_dag_file_by_id(did)
    if dag_path is None:
        raise FileNotFoundError(f"DAG bulunamadi: {did}")

    config_path = _extract_config_path_from_dag_source(dag_path)
    if not config_path.is_file():
        raise ValueError("DAG bulundu ancak bagli YAML dosyasi bulunamadi.")

    projects_root = _projects_root().resolve()
    config_resolved = config_path.resolve()
    try:
        rel = config_resolved.relative_to(projects_root)
    except ValueError as exc:
        raise ValueError("YAML path Flow Studio projects root altinda degil.") from exc
    if len(rel.parts) < 5:
        raise ValueError("YAML path hiyerarsisi gecersiz.")
    project, domain, level, flow = rel.parts[:4]

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("YAML root dict olmalidir.")
    tasks = raw.get("flow_tasks") or []
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("YAML flow_tasks listesi bos veya gecersiz.")
    normalized_tasks: list[dict[str, Any]] = []
    for idx, task in enumerate(tasks, start=1):
        if not isinstance(task, dict):
            raise ValueError(f"flow_tasks[{idx-1}] dict olmalidir.")
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
                "mapping_content": _load_mapping_content_for_task(config_resolved.parent, task),
                "where": str(task.get("where") or "").strip() or None,
                "batch_size": int(task.get("batch_size") or 10000),
                "partitioning_enabled": bool(partitioning.get("enabled", False)),
                "partitioning_mode": str(partitioning.get("mode") or "auto").strip() or "auto",
                "partitioning_column": str(partitioning.get("column") or "").strip() or None,
                "partitioning_parts": int(partitioning.get("parts") or 2),
                "partitioning_distinct_limit": int(partitioning.get("distinct_limit") or 16),
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
        return {"ok": False, "details": {}, "warnings": ["dag_id bos oldugu icin metadata cleanup atlandi."]}

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
        # Airflow 3'te airflow.models.xcom.XCom alias'i BaseXCom olabilir.
        # Metadata cleanup icin yalniz ORM model olan XComModel kullanilir.
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
                    warnings.append(f"{label} temizligi basarisiz: {exc}")
            try:
                session.commit()
            except Exception as exc:
                session.rollback()
                warnings.append(f"Airflow metadata commit basarisiz: {exc}")
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
        raise ValueError("Promote edilen config root dict olmalidir.")
    required_rels = _auto_mapping_rel_paths_from_config_obj(parsed_cfg)
    mapping_texts = dict(bundle.get("mapping_texts") or {})
    new_auto_mapping_paths: set[Path] = set()
    for rel in required_rels:
        rel_norm = _normalize_relative_mapping_file(rel)
        if rel_norm not in mapping_texts:
            raise ValueError(f"Revision icinde mapping dosyasi eksik: {rel_norm}")
        target = _resolve_mapping_file_path(flow_dir, rel_norm)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(mapping_texts.get(rel_norm) or ""), encoding="utf-8")
        new_auto_mapping_paths.add(target)

    stale_auto_paths = existing_auto_mapping_paths - new_auto_mapping_paths
    for stale_path in sorted(stale_auto_paths):
        if stale_path.is_file():
            _best_effort_unlink(stale_path)


def get_dag_revisions(dag_id: str) -> dict[str, Any]:
    did = (dag_id or "").strip()
    if not did:
        raise ValueError("dag_id zorunludur.")

    dag_path = _find_studio_dag_file_by_id(did)
    if dag_path is None:
        raise FileNotFoundError(f"DAG bulunamadi: {did}")
    config_path = _extract_config_path_from_dag_source(dag_path)
    if not config_path.is_file():
        raise ValueError("DAG bulundu ancak bagli YAML dosyasi bulunamadi.")

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
        raise ValueError("dag_id zorunludur.")
    if not rid:
        raise ValueError("revision_id zorunludur.")
    if not _REVISION_DIR_RE.fullmatch(rid):
        raise ValueError("revision_id formati gecersiz.")

    with _dag_operation_lock(did):
        dag_path = _find_studio_dag_file_by_id(did)
        if dag_path is None:
            raise FileNotFoundError(f"DAG bulunamadi: {did}")
        config_path = _extract_config_path_from_dag_source(dag_path)
        if not config_path.is_file():
            raise ValueError("DAG bulundu ancak bagli YAML dosyasi bulunamadi.")

        flow_dir = config_path.resolve().parent
        history_root = _revision_history_root(flow_dir, did)
        revision_dir = history_root / rid
        if not revision_dir.is_dir():
            raise FileNotFoundError(f"Revision bulunamadi: {rid}")

        rollback_bundle = _read_active_bundle(dag_path, config_path, flow_dir)
        before_state = _airflow_parse_state(did)
        target_bundle = _load_bundle_from_revision(revision_dir)
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
                "Revision promote basarisiz oldu; onceki aktif surume geri donuldu."
            ) from exc

        revision_state = get_dag_revisions(did)
        metadata = _load_studio_metadata(flow_dir) or {}
        metadata.update(
            {
                "flow_dir": flow_dir.as_posix(),
                "config_path": config_path.as_posix(),
                "dag_path": dag_path.as_posix(),
                "dag_id": did,
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
        }


def delete_dag_bundle(
    *,
    dag_id: str,
    actor: str = "flow_studio",
) -> dict[str, Any]:
    _ = str(actor or "").strip() or "flow_studio"
    did = str(dag_id or "").strip()
    if not did:
        raise ValueError("dag_id zorunludur.")

    with _dag_operation_lock(did):
        dag_path = _find_studio_dag_file_by_id(did)
        if dag_path is None:
            raise FileNotFoundError(f"DAG bulunamadi: {did}")
        dag_path = _ensure_path_under_root(dag_path, _generated_dag_root())

        config_path = _extract_config_path_from_dag_source(dag_path)
        if not config_path.is_file():
            raise ValueError("DAG bulundu ancak bagli YAML dosyasi bulunamadi.")
        config_path = _ensure_path_under_root(config_path, _projects_root())

        flow_dir = config_path.resolve().parent
        auto_mapping_paths = _collect_existing_auto_mapping_paths(config_path, flow_dir)
        history_root = _revision_history_root(flow_dir, did)
        metadata_path = flow_dir / STUDIO_METADATA_NAME

        deleted_paths: list[str] = []
        warnings: list[str] = []

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
                warnings.append(f"Mapping dosyasi silinemedi: {mapping_path.as_posix()}")

        if config_path.is_file():
            if _best_effort_unlink(config_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(config_path.as_posix())
            else:
                warnings.append(f"YAML dosyasi silinemedi: {config_path.as_posix()}")

        if dag_path.is_file():
            if _best_effort_unlink(dag_path, retries=6, wait_seconds=0.05):
                deleted_paths.append(dag_path.as_posix())
            else:
                warnings.append(f"DAG dosyasi silinemedi: {dag_path.as_posix()}")

        if history_root.exists():
            if _best_effort_rmtree(history_root):
                deleted_paths.append(history_root.as_posix())
            else:
                warnings.append(f"History dizini silinemedi: {history_root.as_posix()}")

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
                    warnings.append(f"Metadata dosyasi silinemedi: {metadata_path.as_posix()}")

        return {
            "dag_id": did,
            "deleted_paths": sorted(set(deleted_paths)),
            "airflow_cleanup": airflow_cleanup,
            "warnings": warnings,
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
            "mode": payload.get("partitioning_mode", "auto"),
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
        raise ValueError("source_type='sql' icin column_mapping_mode='mapping_file' zorunludur.")
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
            "mode": task_payload.get("partitioning_mode", "auto"),
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
        raise ValueError("source_type='sql' icin column_mapping_mode='mapping_file' zorunludur.")
    if task["column_mapping_mode"] == "mapping_file":
        task["mapping_file"] = _auto_mapping_relative_file(task_index, task_group_id)
    return task


def validate_pipeline_payload(payload: dict[str, Any]) -> None:
    """Pipeline formu (T06): task kurallarini ConfigValidator ile dogrular."""
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
    Flow Studio hiyerarsisi icin mevcut klasor seceneklerini dondurur.
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


def generate_mapping_preview(payload: dict[str, Any]) -> dict[str, Any]:
    source_type = str(payload.get("source_type") or "table").strip() or "table"
    source_conn_id = str(payload.get("source_conn_id") or "").strip()
    target_conn_id = str(payload.get("target_conn_id") or "").strip()
    if not source_conn_id or not target_conn_id:
        raise ValueError("source_conn_id ve target_conn_id zorunludur.")

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
            raise ValueError("source_type=table|view icin source_schema ve source_table zorunludur.")
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
            raise ValueError("source_type='sql' icin inline_sql zorunludur.")
        sql_cols = extract_sql_select_columns_for_conn(source_conn_id, inline_sql)
        mapping_obj, warnings = _build_mapping_from_columns(
            columns=sql_cols,
            src_dialect_name=src_name,
            tgt_dialect_name=tgt_name,
            version=version,
        )
    else:
        raise ValueError("source_type yalnizca table|view|sql olabilir.")

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
                raise ValueError("update-dag icin dag_id query param zorunludur.")
            existing_studio_dag = _find_studio_dag_file_by_id(update_dag_id)
            if existing_studio_dag is None:
                raise ValueError(
                    f"Guncellenecek DAG bulunamadi: dag_id={update_dag_id}"
                )
            dag_path = existing_studio_dag
            _ensure_path_under_root(dag_path, gen_root)
            config_path = _extract_config_path_from_dag_source(dag_path)
            if not config_path.is_file():
                raise ValueError("Guncellenecek YAML dosyasi bulunamadi.")
            _ensure_path_under_root(config_path, root)

            config_resolved = config_path.resolve()
            rel = config_resolved.relative_to(root.resolve())
            if len(rel.parts) < 5:
                raise ValueError("DAG'a bagli YAML path hiyerarsisi gecersiz.")
            cfg_project, cfg_domain, cfg_level, cfg_flow = rel.parts[:4]
            if (cfg_project, cfg_domain, cfg_level, cfg_flow) != (project, domain, level, flow):
                raise ValueError(
                    "dag_id ile payload hiyerarsisi uyusmuyor: "
                    f"dag=({cfg_project}/{cfg_domain}/{cfg_level}/{cfg_flow}) "
                    f"payload=({project}/{domain}/{level}/{flow})"
                )

            group_no = _extract_group_no(dag_path.stem, config_path)
        else:
            group_no = _next_group_no(flow_dir, flow_dag_dir)
            dag_path = flow_dag_dir / _build_dag_filename(domain, level, flow, group_no)
            _ensure_path_under_root(dag_path, gen_root)
            config_path = flow_dir / _build_yaml_filename(project, domain, level, flow, group_no)

        existing_auto_mapping_paths = _collect_existing_auto_mapping_paths(config_path, flow_dir)
        tags = _derive_tags(project, domain, level, flow)
        actor = str(os.getenv("FFENGINE_STUDIO_ACTOR", "flow_studio")).strip() or "flow_studio"

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
                raise ValueError("source_type='sql' icin column_mapping_mode='mapping_file' zorunludur.")
            if mode == "mapping_file":
                mapping_rel = _auto_mapping_relative_file(idx, task_group_id)
                mapping_path = _resolve_mapping_file_path(flow_dir, mapping_rel)
                task_cfg["mapping_file"] = mapping_rel
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

        resolve_task_dependencies(task_cfgs)

        if sql_mapping_checks:
            for check in sql_mapping_checks:
                inline_sql = str(check.get("inline_sql") or "").strip()
                if not inline_sql:
                    raise ValueError(
                        f"source_type='sql' icin inline_sql zorunludur. task_group_id={check['task_group_id']}"
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
                        "SQL select kolonlari mapping ile uyumsuz: "
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
            "auto_tags": tags,
            "user_tags": [],
            "active_revision_id": active_revision_id,
            "revision_count": len(revision_items),
        }
        _write_studio_metadata(flow_dir, metadata)

        return {
            "flow_dir": metadata["flow_dir"],
            "config_path": metadata["config_path"],
            "dag_path": metadata["dag_path"],
            "dag_id": metadata["dag_id"],
            "task_group_id": task_cfgs[0]["task_group_id"],
            "active_revision_id": active_revision_id,
            "revision_count": len(revision_items),
        }
