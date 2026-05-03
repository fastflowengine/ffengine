from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, TaskGroup, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from ffengine.airflow.operator import (
    FFEngineOperator,
    aggregate_partition_payloads,
    plan_partitions_for_task,
    prepare_target_for_task,
    run_partition_for_task,
)

DEFAULT_START_DATE = "2023-01-01T00:00:00"


def _slug_task_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", str(value or "").strip()).strip("_").lower()


def _bounded_task_id(value: str, max_len: int = 250) -> str:
    normalized = _slug_task_token(value) or "task"
    if len(normalized) <= max_len:
        return normalized
    suffix = "__h_" + hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    head_len = max(1, max_len - len(suffix))
    head = normalized[:head_len].rstrip("_")
    if not head:
        head = "task"
    return f"{head}{suffix}"


def _resolve_task_dependencies(task_defs: list[dict[str, Any]]) -> list[tuple[str, str]]:
    task_ids: list[str] = []
    id_set: set[str] = set()
    for task_def in task_defs:
        if not isinstance(task_def, dict):
            raise ValueError("Each flow_task must be a dict.")
        task_id = str(task_def.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("task_group_id is required for each flow_task.")
        if task_id in id_set:
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {task_id}")
        task_ids.append(task_id)
        id_set.add(task_id)

    edges: list[tuple[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for idx, task_def in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task_def.get("depends_on")
        if depends_on is None:
            depends_on = []
        if not isinstance(depends_on, list):
            raise ValueError(f"depends_on must be a list: task_group_id={task_id}")
        for dep in depends_on:
            dep_id = str(dep or "").strip()
            if not dep_id:
                continue
            if dep_id == task_id:
                raise ValueError(f"depends_on cannot reference itself: {task_id}")
            if dep_id not in id_set:
                raise ValueError(f"depends_on contains invalid task_group_id: {dep_id}")
            edge = (dep_id, task_id)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            edges.append(edge)

    graph = {task_id: [] for task_id in task_ids}
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


def _run_script_task(task_def: dict[str, Any], source_conn_id: str, target_conn_id: str) -> dict[str, Any]:
    from ffengine.airflow.operator import build_airflow_variable_context, resolve_dialect
    from ffengine.config.binding_resolver import BindingResolver
    from ffengine.db.airflow_adapter import AirflowConnectionAdapter
    from ffengine.db.session import DBSession

    script_sql = str(task_def.get("script_sql") or "").strip()
    if not script_sql:
        raise ValueError("script_sql is required when task_type='script_run'.")
    environment = str(task_def.get("script_run_environment") or "").strip().lower()
    if environment not in {"source", "target"}:
        raise ValueError("script_run_environment must be one of: 'source' or 'target'.")
    source_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
    target_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
    source_dialect = resolve_dialect(source_params["conn_type"])
    target_dialect = resolve_dialect(target_params["conn_type"])
    conn_id = source_conn_id if environment == "source" else target_conn_id
    exec_params = source_params if environment == "source" else target_params
    exec_dialect = source_dialect if environment == "source" else target_dialect
    bindings = task_def.get("bindings") or []
    effective_sql = script_sql
    if isinstance(bindings, list) and bindings:
        resolver = BindingResolver()
        airflow_ctx = build_airflow_variable_context()
        with DBSession(source_params, source_dialect) as src_session:
            with DBSession(target_params, target_dialect) as tgt_session:
                resolved = resolver.resolve_sql_bindings(
                    {"where": script_sql, "bindings": bindings},
                    context=airflow_ctx,
                    source_session=src_session,
                    target_session=tgt_session,
                    where_dialect=exec_dialect,
                )
        effective_sql = str(resolved.get("_resolved_where") or script_sql).strip()
    with DBSession(exec_params, exec_dialect) as db_session:
        cursor = db_session.cursor(server_side=False)
        try:
            cursor.execute(effective_sql)
            db_session.conn.commit()
        except Exception:
            db_session.conn.rollback()
            raise
        finally:
            cursor.close()
    return {
        "task_type": "script_run",
        "script_run_environment": environment,
        "connection_id": conn_id,
        "status": "ok",
    }


def _resolve_config_path(raw_config_snapshot: dict[str, Any]) -> str:
    config_path = str(
        raw_config_snapshot.get("__config_path")
        or raw_config_snapshot.get("_config_path")
        or raw_config_snapshot.get("config_path")
        or ""
    ).strip()
    if not config_path:
        raise ValueError("raw_config_snapshot must include '__config_path' for runtime operators.")
    return config_path


def build_generated_dag(
    *,
    dag_id: str,
    dag_tags: list[str] | None,
    upstream_dag_ids: list[str] | None,
    raw_config_snapshot: dict[str, Any],
) -> DAG:
    raw = raw_config_snapshot or {}
    if not isinstance(raw, dict):
        raise ValueError("raw_config_snapshot must be a dict.")

    source_conn_id = str(raw.get("source_db_var") or "").strip()
    target_conn_id = str(raw.get("target_db_var") or "").strip()
    task_defs = raw.get("flow_tasks") or []
    scheduler = raw.get("scheduler") or {}
    if not isinstance(scheduler, dict):
        scheduler = {}

    if not source_conn_id or not target_conn_id:
        raise ValueError("source_db_var and target_db_var are required.")
    if not isinstance(task_defs, list) or not task_defs:
        raise ValueError("flow_tasks must be a list with at least one task.")

    config_path = _resolve_config_path(raw)

    upstream_ids = list(dict.fromkeys([str(x).strip() for x in (upstream_dag_ids or []) if str(x).strip()]))
    cron_expression = scheduler.get("cron_expression")
    if isinstance(cron_expression, str):
        cron_expression = cron_expression.strip() or None
    else:
        cron_expression = None

    timezone_name = str(scheduler.get("timezone") or "UTC").strip() or "UTC"
    try:
        scheduler_tz = ZoneInfo(timezone_name)
    except Exception:
        scheduler_tz = ZoneInfo("UTC")

    start_date_raw = str(scheduler.get("start_date") or DEFAULT_START_DATE).strip() or DEFAULT_START_DATE
    try:
        dag_start_date = datetime.fromisoformat(start_date_raw.replace("Z", "+00:00"))
    except ValueError:
        dag_start_date = datetime(2023, 1, 1, 0, 0, 0)
    if dag_start_date.tzinfo is None:
        dag_start_date = dag_start_date.replace(tzinfo=scheduler_tz)
    else:
        dag_start_date = dag_start_date.astimezone(scheduler_tz)

    dag_active = bool(scheduler.get("active", True))
    edges = _resolve_task_dependencies(task_defs)
    effective_schedule = None if upstream_ids else cron_expression
    task_ids_with_upstream = {downstream for _upstream, downstream in edges}
    root_task_ids = [
        task_id
        for task_id in [str(task_def.get("task_group_id") or "").strip() for task_def in task_defs]
        if task_id not in task_ids_with_upstream
    ]
    root_task_order = {task_id: idx + 1 for idx, task_id in enumerate(root_task_ids)}
    dag_slug = _slug_task_token(dag_id) or "dag"

    with DAG(
        dag_id=dag_id,
        schedule=effective_schedule,
        start_date=dag_start_date,
        catchup=False,
        tags=list(dag_tags or []),
        is_paused_upon_creation=not dag_active,
    ) as dag:
        task_groups: dict[str, Any] = {}
        for task_def in task_defs:
            task_group_id = str(task_def.get("task_group_id") or "").strip()
            task_type = str(task_def.get("task_type") or "source_target").strip() or "source_target"
            task_slug = _slug_task_token(task_group_id) or f"task_{max(1, len(task_groups) + 1)}"
            if task_type == "script_run":
                script_task_id = _bounded_task_id(f"script__{task_slug}")

                @task(task_id=script_task_id)
                def _run_script_task_wrapper(
                    _task_def=task_def,
                    _source_conn_id=source_conn_id,
                    _target_conn_id=target_conn_id,
                ):
                    return _run_script_task(_task_def, _source_conn_id, _target_conn_id)

                task_groups[task_group_id] = _run_script_task_wrapper()
                continue
            if task_type == "dag":
                triggered_dag_id = str(task_def.get("dag_task_dag_id") or "").strip()
                if not triggered_dag_id:
                    raise ValueError("dag_task_dag_id is required when task_type='dag'.")
                if triggered_dag_id == dag_id:
                    raise ValueError("dag_task_dag_id cannot reference itself.")
                trigger_task_id = _bounded_task_id(f"trigger_dag__{task_slug}")
                task_groups[task_group_id] = TriggerDagRunOperator(
                    task_id=trigger_task_id,
                    trigger_dag_id=triggered_dag_id,
                    logical_date="{{ dag_run.logical_date }}",
                    wait_for_completion=True,
                    allowed_states=["success"],
                    failed_states=["failed"],
                    poke_interval=30,
                    deferrable=False,
                    reset_dag_run=False,
                    skip_when_already_exists=False,
                )
                continue
            partition_cfg = task_def.get("partitioning")
            partition_enabled = isinstance(partition_cfg, dict) and bool(partition_cfg.get("enabled", False))
            if not partition_enabled:
                if upstream_ids and task_group_id in root_task_ids:
                    upstream_slug_parts = [_slug_task_token(uid) for uid in upstream_ids]
                    upstream_slug_parts = [item for item in upstream_slug_parts if item]
                    joined_upstream_slug = "__".join(upstream_slug_parts)
                    root_order = int(root_task_order.get(task_group_id) or 1)
                    if joined_upstream_slug:
                        task_id_value = _bounded_task_id(
                            f"run_after__{joined_upstream_slug}__{dag_slug}__r{root_order}"
                        )
                    else:
                        task_id_value = _bounded_task_id(f"run__{dag_slug}__r{root_order}")
                else:
                    task_id_value = f"run_{task_group_id}"
                task_groups[task_group_id] = FFEngineOperator(
                    config_path=config_path,
                    task_group_id=task_group_id,
                    source_conn_id=source_conn_id,
                    target_conn_id=target_conn_id,
                    task_id=task_id_value,
                )
                continue
            group_id = "flow__" + re.sub(r"[^A-Za-z0-9_]+", "_", task_group_id).strip("_").lower()
            with TaskGroup(group_id=group_id) as flow_group:
                @task(task_id="plan_partitions")
                def _plan_partitions(
                    _config_path=config_path,
                    _task_group_id=task_group_id,
                    _source_conn_id=source_conn_id,
                    _target_conn_id=target_conn_id,
                ):
                    return plan_partitions_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                    )

                @task(task_id="prepare_target")
                def _prepare_target(
                    _plan_specs,
                    _config_path=config_path,
                    _task_group_id=task_group_id,
                    _source_conn_id=source_conn_id,
                    _target_conn_id=target_conn_id,
                ):
                    _ = _plan_specs
                    return prepare_target_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                    )

                @task(task_id="run_partition")
                def _run_partition(
                    partition_spec,
                    _config_path=config_path,
                    _task_group_id=task_group_id,
                    _source_conn_id=source_conn_id,
                    _target_conn_id=target_conn_id,
                ):
                    return run_partition_for_task(
                        config_path=_config_path,
                        task_group_id=_task_group_id,
                        source_conn_id=_source_conn_id,
                        target_conn_id=_target_conn_id,
                        partition_spec=partition_spec,
                    )

                @task(task_id="aggregate")
                def _aggregate_partition_payloads(results):
                    return aggregate_partition_payloads(results)

                plan_ctx = _plan_partitions()
                prepare_ctx = _prepare_target(plan_ctx)
                run_payloads = _run_partition.expand(partition_spec=plan_ctx)
                prepare_ctx >> run_payloads
                _aggregate_partition_payloads(run_payloads)
            task_groups[task_group_id] = flow_group

        for upstream, downstream in edges:
            task_groups[upstream] >> task_groups[downstream]

        if upstream_ids:
            upstream_triggers = {}
            upstream_waiters = {}
            for upstream_dag_id in upstream_ids:
                trigger_task_id = "trigger_upstream__" + re.sub(
                    r"[^A-Za-z0-9_]+",
                    "_",
                    str(upstream_dag_id),
                ).strip("_").lower()
                upstream_triggers[upstream_dag_id] = TriggerDagRunOperator(
                    task_id=trigger_task_id,
                    trigger_dag_id=upstream_dag_id,
                    logical_date="{{ dag_run.logical_date }}",
                    wait_for_completion=False,
                    reset_dag_run=False,
                    skip_when_already_exists=False,
                )
                waiter_task_id = "wait_upstream__" + re.sub(
                    r"[^A-Za-z0-9_]+",
                    "_",
                    str(upstream_dag_id),
                ).strip("_").lower()
                upstream_waiters[upstream_dag_id] = ExternalTaskSensor(
                    task_id=waiter_task_id,
                    external_dag_id=upstream_dag_id,
                    external_task_id=None,
                    allowed_states=["success"],
                    failed_states=["failed"],
                    check_existence=False,
                    mode="reschedule",
                    poke_interval=60,
                    timeout=12 * 60 * 60,
                )
                upstream_triggers[upstream_dag_id] >> upstream_waiters[upstream_dag_id]

            for waiter in upstream_waiters.values():
                for root_task_id in root_task_ids:
                    waiter >> task_groups[root_task_id]

    return dag
