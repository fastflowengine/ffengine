"""F1.3 — the generated DAG wires flow-level notification callbacks."""

from __future__ import annotations

import pytest

from ffengine.airflow.generated_factory import build_generated_dag
from ffengine.airflow.notifications import build_deadline_alert


def _supports_sync_deadline() -> bool:
    """True on Airflow 3.2+ where DeadlineAlert accepts a SyncCallback."""
    try:
        from datetime import timedelta

        from airflow.sdk.definitions.callback import SyncCallback
        from airflow.sdk.definitions.deadline import (
            DeadlineAlert,
            DeadlineReference,
        )

        DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(minutes=1),
            callback=SyncCallback("x.y"),
        )
        return True
    except Exception:  # noqa: BLE001
        return False


_SUPPORTS_SYNC_DEADLINE = _supports_sync_deadline()

_DEADLINE_CFG = {
    "notify_on": ["deadline"],
    "notify_emails": ["ops@bank.example"],
    "notify_conn_id": "smtp_default",
    "notify_deadline_minutes": 5,
}


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


# ---- F1.3c deadline -------------------------------------------------------

def test_build_deadline_alert_none_when_not_configured():
    # pure checks (no Airflow import) — run on any version
    assert build_deadline_alert(None) is None
    assert build_deadline_alert({"notify_on": ["failure"]}) is None
    assert (
        build_deadline_alert(
            {**_DEADLINE_CFG, "notify_deadline_minutes": 0}
        )
        is None
    )


@pytest.mark.skipif(
    _SUPPORTS_SYNC_DEADLINE, reason="only meaningful on Airflow < 3.2"
)
def test_build_deadline_alert_degrades_gracefully_pre_3_2():
    # On Airflow < 3.2 the deadline SDK is absent/incompatible ⇒ None (no raise).
    assert build_deadline_alert(_DEADLINE_CFG) is None


@pytest.mark.skipif(
    not _SUPPORTS_SYNC_DEADLINE, reason="deadline needs Airflow 3.2+"
)
def test_build_deadline_alert_constructs_on_3_2():
    from datetime import timedelta

    alert = build_deadline_alert(_DEADLINE_CFG)
    assert alert is not None
    assert alert.interval == timedelta(minutes=5)
    assert alert.callback.path == (
        "ffengine.airflow.notifications.deadline_notification_callback"
    )
    assert set(alert.callback.kwargs) == {"emails", "conn_id", "template_name"}


@pytest.mark.skipif(
    not _SUPPORTS_SYNC_DEADLINE, reason="deadline needs Airflow 3.2+"
)
def test_dag_gets_deadline_when_configured():
    dag = build_generated_dag(
        dag_id="demo_deadline_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config(_DEADLINE_CFG),
    )
    assert dag.deadline  # DeadlineAlert attached (DAG wraps it in a list)
