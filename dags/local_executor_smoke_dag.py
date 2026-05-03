"""
LocalExecutor parallelism smoke DAG (C20).

This DAG is intentionally dependency-free and does not call external systems.
It is used to verify overlapping task execution after switching to LocalExecutor.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def _parallel_probe(task_name: str, sleep_seconds: int = 20) -> dict[str, str | int]:
    start_utc = datetime.now(timezone.utc)
    logging.info(
        "LocalExecutor smoke task started: task=%s start_utc=%s sleep_seconds=%s",
        task_name,
        start_utc.isoformat(),
        sleep_seconds,
    )
    time.sleep(sleep_seconds)
    end_utc = datetime.now(timezone.utc)
    logging.info(
        "LocalExecutor smoke task completed: task=%s end_utc=%s",
        task_name,
        end_utc.isoformat(),
    )
    return {
        "task_name": task_name,
        "start_utc": start_utc.isoformat(),
        "end_utc": end_utc.isoformat(),
        "sleep_seconds": sleep_seconds,
    }


with DAG(
    dag_id="ffengine_local_executor_smoke",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ffengine", "smoke", "localexecutor"],
    doc_md=(
        "C20 smoke DAG for LocalExecutor validation. "
        "Expected behavior: 3 tasks run in parallel with overlapping runtime."
    ),
) as dag:
    probe_a = PythonOperator(
        task_id="probe_a",
        python_callable=_parallel_probe,
        op_kwargs={"task_name": "probe_a", "sleep_seconds": 20},
    )
    probe_b = PythonOperator(
        task_id="probe_b",
        python_callable=_parallel_probe,
        op_kwargs={"task_name": "probe_b", "sleep_seconds": 20},
    )
    probe_c = PythonOperator(
        task_id="probe_c",
        python_callable=_parallel_probe,
        op_kwargs={"task_name": "probe_c", "sleep_seconds": 20},
    )

    # Intentionally no dependency edges: all probes should run in parallel.
    parallel_probes = [probe_a, probe_b, probe_c]
