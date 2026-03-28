# generated_by: etl_studio
import json
from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG

from ffengine.airflow.operator import FFEngineOperator

CONFIG_PATH = Path("/opt/airflow/projects/test/public/level1/src_to_odc/test_public_level1_src_to_odc_group_12.yaml")
DAG_ID = "public_to_odc_level1_group_12_dag"
DAG_TAGS = ["test", "public", "level1", "src_to_odc"]


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
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {task_id}")
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
                raise ValueError(f"depends_on list olmalidir: task_group_id={task_id}")
            for dep in depends_on:
                dep_id = str(dep or "").strip()
                if not dep_id:
                    continue
                if dep_id not in id_set:
                    raise ValueError(f"depends_on gecersiz task_group_id iceriyor: {dep_id}")
                edges.append((dep_id, task_id))
        previous_task_id = task_id

    graph = {task_id: [] for task_id in task_ids}
    for upstream, downstream in edges:
        graph[upstream].append(downstream)
    state = {}

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


raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
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
    operators = {}
    for task in task_defs:
        task_group_id = str(task.get("task_group_id") or "").strip()
        operators[task_group_id] = FFEngineOperator(
            config_path=str(CONFIG_PATH),
            task_group_id=task_group_id,
            source_conn_id=source_conn_id,
            target_conn_id=target_conn_id,
            task_id=f"run_{task_group_id}",
        )
    for upstream, downstream in edges:
        operators[upstream] >> operators[downstream]
