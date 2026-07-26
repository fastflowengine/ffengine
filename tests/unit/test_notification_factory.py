"""F1.3 — the generated DAG wires flow-level notification callbacks."""

from __future__ import annotations

from ffengine.airflow.generated_factory import build_generated_dag


def _raw_config(notifications=None) -> dict:
    raw = {
        "source_db_var": "src_conn",
        "target_db_var": "tgt_conn",
        "scheduler": {
            "cron_expression": None,
            "timezone": "UTC",
            "active": True,
            "start_date": "2024-01-01T00:00:00",
        },
        "flow_tasks": [
            {
                "task_group_id": "orders_task",
                "task_type": "source_target",
                "depends_on": [],
                "partitioning": {"enabled": False},
            }
        ],
        "__config_path": "/opt/airflow/projects/demo/demo.yaml",
    }
    if notifications is not None:
        raw["notifications"] = notifications
    return raw


def test_dag_injects_notification_callbacks():
    dag = build_generated_dag(
        dag_id="demo_notify_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config(
            {
                "notify_on": ["failure"],
                "notify_emails": ["ops@bank.example"],
                "notify_conn_id": "smtp_default",
            }
        ),
    )
    assert dag.on_failure_callback  # truthy — callback wired
    assert not dag.on_success_callback  # success not requested


def test_dag_injects_both_callbacks():
    dag = build_generated_dag(
        dag_id="demo_notify_both_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config(
            {
                "notify_on": ["failure", "success"],
                "notify_emails": ["ops@bank.example"],
                "notify_conn_id": "smtp_default",
            }
        ),
    )
    assert dag.on_failure_callback
    assert dag.on_success_callback


def test_dag_without_notifications_has_no_callbacks():
    dag = build_generated_dag(
        dag_id="demo_plain_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config(None),
    )
    assert not dag.on_failure_callback
    assert not dag.on_success_callback
