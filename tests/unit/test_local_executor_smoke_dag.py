from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    dag_path = Path(__file__).resolve().parents[2] / "dags" / "local_executor_smoke_dag.py"
    spec = importlib.util.spec_from_file_location("local_executor_smoke_dag", dag_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_local_executor_smoke_dag_structure():
    module = _load_module()
    dag = module.dag

    assert dag.dag_id == "ffengine_local_executor_smoke"
    assert dag.schedule is None

    task_ids = sorted(task.task_id for task in dag.tasks)
    assert task_ids == ["probe_a", "probe_b", "probe_c"]

    for task in dag.tasks:
        assert not task.upstream_task_ids
        assert not task.downstream_task_ids
