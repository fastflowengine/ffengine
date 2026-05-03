from __future__ import annotations

import pytest

from ffengine.airflow.generated_factory import build_generated_dag
from ffengine.airflow.operator import FFEngineOperator


def _base_raw_config() -> dict:
    return {
        "source_db_var": "src_conn",
        "target_db_var": "tgt_conn",
        "scheduler": {
            "cron_expression": None,
            "timezone": "UTC",
            "active": True,
            "start_date": "2024-01-01T00:00:00",
        },
        "__config_path": "/opt/airflow/projects/demo/demo.yaml",
    }


def test_build_generated_dag_source_target_and_scheduler_flags():
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "depends_on": [],
            "partitioning": {"enabled": False},
        }
    ]

    dag = build_generated_dag(
        dag_id="demo_generated_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert dag.dag_id == "demo_generated_dag"
    assert getattr(dag, "schedule", None) is None
    assert dag.is_paused_upon_creation is False
    assert len(dag.tasks) == 1
    assert dag.tasks[0].task_id == "run_orders_task"
    assert isinstance(dag.tasks[0], FFEngineOperator)


def test_build_generated_dag_creates_dag_trigger_and_upstream_waiter_wiring():
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "task_a",
            "task_type": "source_target",
            "depends_on": [],
            "partitioning": {"enabled": False},
        },
        {
            "task_group_id": "task_b",
            "task_type": "dag",
            "dag_task_dag_id": "child_dag_id",
            "depends_on": ["task_a"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="demo_with_upstream",
        dag_tags=["demo"],
        upstream_dag_ids=["external_upstream_dag"],
        raw_config_snapshot=raw,
    )

    task_ids = {task.task_id for task in dag.tasks}
    assert "trigger_upstream__external_upstream_dag" in task_ids
    assert "wait_upstream__external_upstream_dag" in task_ids
    trigger_task_id = "trigger_dag__task_b"
    assert trigger_task_id in task_ids

    trigger_task = dag.task_dict[trigger_task_id]
    assert getattr(trigger_task, "wait_for_completion", None) is True
    assert getattr(trigger_task, "deferrable", None) is False

    waiter = dag.task_dict["wait_upstream__external_upstream_dag"]
    root = dag.task_dict["run_after__external_upstream_dag__demo_with_upstream__r1"]
    assert root.task_id in waiter.downstream_task_ids


def test_build_generated_dag_validates_dependency_cycle():
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "a",
            "task_type": "source_target",
            "depends_on": ["b"],
            "partitioning": {"enabled": False},
        },
        {
            "task_group_id": "b",
            "task_type": "source_target",
            "depends_on": ["a"],
            "partitioning": {"enabled": False},
        },
    ]

    with pytest.raises(ValueError, match="cycle"):
        build_generated_dag(
            dag_id="demo_cycle",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def test_build_generated_dag_partition_task_group_and_script_task():
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "partitioned_task",
            "task_type": "source_target",
            "depends_on": [],
            "partitioning": {"enabled": True},
        },
        {
            "task_group_id": "script_task",
            "task_type": "script_run",
            "script_sql": "select 1",
            "script_run_environment": "source",
            "depends_on": ["partitioned_task"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="demo_partition_script",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    task_ids = {task.task_id for task in dag.tasks}
    assert "flow__partitioned_task.plan_partitions" in task_ids
    assert "flow__partitioned_task.prepare_target" in task_ids
    assert "flow__partitioned_task.run_partition" in task_ids
    assert "flow__partitioned_task.aggregate" in task_ids
    assert "script__script_task" in task_ids
