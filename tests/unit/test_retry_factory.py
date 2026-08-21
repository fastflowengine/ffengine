"""F7.1 — uretilen DAG'in Airflow-native retry alanlarini tasimasi.

Kritik iddia ZERO-DIFF: `retry` blogu olmayan config'ler icin DAG'a hicbir
`default_args` eklenmez, yani mevcut DAG'larin davranisi (retries=0) aynen
korunur (ARCH-11).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from ffengine.airflow.generated_factory import build_generated_dag


def _raw_config(retry=None) -> dict:
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
    if retry is not None:
        raw["retry"] = retry
    return raw


def _retry_default_args(dag) -> dict:
    """DAG'in retry ile ilgili default_args'ini dondurur (yoksa bos)."""
    args = dict(getattr(dag, "default_args", None) or {})
    return {k: v for k, v in args.items() if k in {"retries", "retry_delay"}}


# --- T-F7.1-05: zero-diff ----------------------------------------------------

def test_dag_without_retry_block_has_no_retry_default_args():
    dag = build_generated_dag(
        dag_id="demo_no_retry_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config(),
    )
    assert _retry_default_args(dag) == {}


def test_retries_zero_behaves_like_no_block():
    """UI kapali retry'i hic gondermez; gonderilse bile omit edilir."""
    dag = build_generated_dag(
        dag_id="demo_retry_zero_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config({"retries": 0}),
    )
    assert _retry_default_args(dag) == {}


# --- T-F7.1-06: retry emit ---------------------------------------------------

def test_dag_with_retry_block_emits_airflow_default_args():
    dag = build_generated_dag(
        dag_id="demo_retry_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config({"retries": 3, "delay_seconds": 120}),
    )
    assert _retry_default_args(dag) == {
        "retries": 3,
        "retry_delay": timedelta(seconds=120),
    }


def test_retry_delay_default_is_applied():
    dag = build_generated_dag(
        dag_id="demo_retry_default_delay_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config({"retries": 2}),
    )
    assert _retry_default_args(dag) == {
        "retries": 2,
        "retry_delay": timedelta(seconds=60),
    }


def test_retry_reaches_the_tasks_themselves():
    """default_args DAG'daki task'lara inmeli -- retry'i Airflow uygular."""
    dag = build_generated_dag(
        dag_id="demo_retry_tasks_dag",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_raw_config({"retries": 4, "delay_seconds": 30}),
    )
    assert dag.tasks, "DAG en az bir task icermeli"
    for task in dag.tasks:
        assert task.retries == 4
        assert task.retry_delay == timedelta(seconds=30)


# --- T-F7.1-07: fail-loud DAG-parse yolunda ----------------------------------

def test_invalid_retry_block_fails_dag_build():
    """Gecersiz blok sessizce yok sayilmaz; DAG parse'i fail-loud kirilir."""
    with pytest.raises(ValueError, match="retry"):
        build_generated_dag(
            dag_id="demo_bad_retry_dag",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=_raw_config({"retries": 99}),
        )
