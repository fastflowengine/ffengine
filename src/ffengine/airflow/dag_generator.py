"""
C07 - Automatic DAG generator.

Scans YAML config directory and creates Airflow DAG objects per task.
"""

import logging
import os
from pathlib import Path

_log = logging.getLogger(__name__)


def generate_dags(
    config_dir: str,
    dag_prefix: str = "ffengine",
    schedule: str | None = None,
    tags: list[str] | None = None,
) -> dict:
    """
    Scans YAML files under config_dir and generates a DAG for each task.

    Parameters
    ----------
    config_dir   : Directory containing YAML config files.
    dag_prefix   : DAG ID prefix (default: "ffengine").
    schedule     : Airflow schedule expression (default: None -> manual).
    tags         : DAG tags.

    Returns
    -------
    dict[str, DAG] : {dag_id: DAG} mapping.
    """
    from datetime import datetime

    import yaml

    try:
        from airflow.sdk import DAG, task
    except ImportError:
        _log.warning("Airflow is not installed - generate_dags skipped.")
        return {}

    config_path = Path(config_dir)
    if not config_path.is_dir():
        _log.warning("Config directory not found: %s", config_dir)
        return {}

    effective_tags = tags or ["ffengine", "auto-generated"]
    dags: dict = {}

    for yaml_file in sorted(config_path.glob("*.yaml")):
        try:
            raw = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        except Exception as exc:
            _log.warning("YAML parse error, skipping: %s - %s", yaml_file.name, exc)
            continue

        if not isinstance(raw, dict):
            _log.warning("Invalid YAML structure, skipping: %s", yaml_file.name)
            continue

        tasks = raw.get("flow_tasks", [])
        if not isinstance(tasks, list):
            continue

        source_conn_id = raw.get("source_db_var", "ffengine_source")
        target_conn_id = raw.get("target_db_var", "ffengine_target")

        for task_def in tasks:
            if not isinstance(task_def, dict):
                continue

            tg_id = task_def.get("task_group_id")
            if not tg_id:
                continue
            partition_cfg = task_def.get("partitioning")
            partition_enabled = isinstance(partition_cfg, dict) and bool(partition_cfg.get("enabled", False))

            dag_id = f"{dag_prefix}_{yaml_file.stem}_{tg_id}"

            dag = DAG(
                dag_id=dag_id,
                schedule=schedule,
                start_date=datetime(2023, 1, 1),
                catchup=False,
                tags=effective_tags,
            )

            from ffengine.airflow.operator import (
                FFEngineOperator,
                aggregate_partition_payloads,
                plan_partitions_for_task,
                prepare_target_for_task,
                run_partition_for_task,
            )

            with dag:
                if not partition_enabled:
                    FFEngineOperator(
                        config_path=str(yaml_file),
                        task_group_id=str(tg_id),
                        source_conn_id=str(source_conn_id),
                        target_conn_id=str(target_conn_id),
                        task_id=f"run_{tg_id}",
                    )
                    dags[dag_id] = dag
                    continue

                @task(task_id=f"plan_{tg_id}")
                def _plan_partitions(
                    _config_path: str = str(yaml_file),
                    _task_group_id: str = str(tg_id),
                    _source_conn_id: str = str(source_conn_id),
                    _target_conn_id: str = str(target_conn_id),
                ) -> list[dict]:
                    return plan_partitions_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                    )

                @task(task_id=f"prepare_{tg_id}")
                def _prepare_target(
                    _plan_specs: list[dict],
                    _config_path: str = str(yaml_file),
                    _task_group_id: str = str(tg_id),
                    _source_conn_id: str = str(source_conn_id),
                    _target_conn_id: str = str(target_conn_id),
                ) -> dict:
                    _ = _plan_specs
                    return prepare_target_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                    )

                @task(task_id=f"run_{tg_id}")
                def _run_partition(
                    partition_spec: dict,
                    _config_path: str = str(yaml_file),
                    _task_group_id: str = str(tg_id),
                    _source_conn_id: str = str(source_conn_id),
                    _target_conn_id: str = str(target_conn_id),
                ) -> dict:
                    return run_partition_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                        partition_spec=partition_spec,
                    )

                @task(task_id=f"aggregate_{tg_id}")
                def _aggregate_partition_payloads(results: list[dict]) -> dict:
                    return aggregate_partition_payloads(results)

                plan_ctx = _plan_partitions()
                prepare_ctx = _prepare_target(plan_ctx)
                run_payloads = _run_partition.expand(partition_spec=plan_ctx)
                prepare_ctx >> run_payloads
                _aggregate_partition_payloads(run_payloads)

            dags[dag_id] = dag

    _log.info("generate_dags: %d DAG generated (%s)", len(dags), config_dir)
    return dags


def register_dags(
    config_dir: str,
    globals_dict: dict,
    **kwargs,
) -> None:
    """
    Registers generate_dags() output into globals() dict.

    Usage (inside dags/*.py):
        from ffengine.airflow.dag_generator import register_dags
        register_dags("/opt/airflow/configs", globals())
    """
    dags = generate_dags(config_dir, **kwargs)
    globals_dict.update(dags)

