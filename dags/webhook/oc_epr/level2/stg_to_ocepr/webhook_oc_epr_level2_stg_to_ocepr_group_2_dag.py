# generated_by: flow_studio
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
import hashlib
from airflow import DAG
try:
    from airflow.sdk import task
except Exception:
    from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from ffengine.airflow.operator import (
    FFEngineOperator,
    aggregate_partition_payloads,
    plan_partitions_for_task,
    prepare_target_for_task,
    run_partition_for_task,
)

CONFIG_PATH = Path("/opt/airflow/projects/webhook/oc_epr/level2/stg_to_ocepr/webhook_oc_epr_level2_stg_to_ocepr_group_2.yaml")
DAG_ID = "webhook_oc_epr_level2_stg_to_ocepr_group_2_dag"
DAG_TAGS = ["webhook", "oc_epr", "level2", "stg_to_ocepr"]
UPSTREAM_DAG_IDS = []


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


dag_slug = _slug_task_token(DAG_ID) or "dag"


def _resolve_task_dependencies(task_defs):
    task_ids = []
    id_set = set()
    for task in task_defs:
        if not isinstance(task, dict):
            raise ValueError("Each flow_task must be a dict.")
        task_id = str(task.get("task_group_id") or "").strip()
        if not task_id:
            raise ValueError("task_group_id is required for each flow_task.")
        if task_id in id_set:
            raise ValueError(f"Ayni task_group_id birden fazla kez kullanildi: {task_id}")
        task_ids.append(task_id)
        id_set.add(task_id)

    edges = []
    seen_edges = set()
    for idx, task in enumerate(task_defs):
        task_id = task_ids[idx]
        depends_on = task.get("depends_on")
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


class TriggerDagRunWithChildLogSummary(TriggerDagRunOperator):
    """Trigger a child DAG and emit grouped child DAG task logs into parent logs."""

    def __init__(
        self,
        *args,
        parent_task_number=None,
        parent_task_group_id=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        try:
            self.parent_task_number = int(parent_task_number) if parent_task_number is not None else None
        except Exception:
            self.parent_task_number = None
        self.parent_task_group_id = str(parent_task_group_id or "").strip() or None

    def execute(self, context):
        result = super().execute(context)
        self._emit_child_log_summary(context)
        return result

    def _emit_child_task_log_lines(self, child_ti):
        try:
            from airflow.utils.log.log_reader import TaskLogReader
        except Exception as exc:
            self.log.warning(
                "Child task log reader import failed: task_id=%s error=%s",
                getattr(child_ti, "task_id", None),
                exc,
            )
            return
        try:
            reader = TaskLogReader()
            metadata = {"end_of_log": False}
            try_number = getattr(child_ti, "try_number", None)
            if try_number is not None:
                try:
                    try_number = int(try_number)
                except Exception:
                    try_number = None
            emitted_any = False
            for chunk in reader.read_log_stream(child_ti, try_number=try_number, metadata=metadata):
                text = str(chunk or "")
                if not text:
                    continue
                emitted_any = True
                for line in text.splitlines():
                    self.log.info("[child_log] %s", line)
            if not emitted_any:
                self.log.info("[child_log] (no log content)")
        except Exception as exc:
            self.log.warning(
                "Child task log read failed: task_id=%s map_index=%s try_number=%s error=%s log_url=%s",
                getattr(child_ti, "task_id", None),
                getattr(child_ti, "map_index", -1),
                getattr(child_ti, "try_number", None),
                exc,
                getattr(child_ti, "log_url", None),
            )

    def _emit_child_log_summary(self, context):
        child_dag_id = str(self.trigger_dag_id or "").strip()
        if not child_dag_id:
            self.log.warning("Child DAG log summary skipped: empty trigger_dag_id.")
            return
        try:
            from airflow.models.dagrun import DagRun
            from airflow.utils.session import create_session
        except Exception as exc:
            self.log.warning(
                "Child DAG log summary unavailable for dag_id=%s: %s",
                child_dag_id,
                exc,
            )
            return

        trigger_run_id = None
        ti = context.get("ti") or context.get("task_instance")
        if ti is not None:
            xcom_key = getattr(type(self), "XCOM_RUN_ID", "trigger_run_id")
            try:
                trigger_run_id = ti.xcom_pull(task_ids=self.task_id, key=xcom_key)
            except Exception:
                trigger_run_id = None

        dag_run_ctx = context.get("dag_run")
        logical_date = getattr(dag_run_ctx, "logical_date", None)

        child_run = None
        child_tis = []
        with create_session() as session:
            query = session.query(DagRun).filter(DagRun.dag_id == child_dag_id)
            if trigger_run_id:
                child_run = query.filter(DagRun.run_id == trigger_run_id).one_or_none()
            if child_run is None and logical_date is not None:
                child_run = (
                    query.filter(DagRun.logical_date == logical_date)
                    .order_by(DagRun.start_date.desc())
                    .first()
                )
            if child_run is None:
                self.log.warning(
                    "Child DAG run not found for dag_id=%s run_id=%s logical_date=%s.",
                    child_dag_id,
                    trigger_run_id,
                    logical_date,
                )
                return
            child_tis = list(child_run.get_task_instances(session=session))

        self.log.info(
            "===== CHILD DAG LOG GROUP START | parent_task_no=%s parent_task_group_id=%s child_dag_id=%s =====",
            self.parent_task_number,
            self.parent_task_group_id,
            child_dag_id,
        )
        self.log.info(
            "Child DAG run summary: dag_id=%s run_id=%s state=%s logical_date=%s start_date=%s end_date=%s parent_task_no=%s parent_task_group_id=%s",
            child_dag_id,
            getattr(child_run, "run_id", None),
            getattr(child_run, "state", None),
            getattr(child_run, "logical_date", None),
            getattr(child_run, "start_date", None),
            getattr(child_run, "end_date", None),
            self.parent_task_number,
            self.parent_task_group_id,
        )
        if not child_tis:
            self.log.info("Child DAG has no task instances: dag_id=%s run_id=%s", child_dag_id, getattr(child_run, "run_id", None))
            self.log.info(
                "===== CHILD DAG LOG GROUP END | parent_task_no=%s parent_task_group_id=%s child_dag_id=%s =====",
                self.parent_task_number,
                self.parent_task_group_id,
                child_dag_id,
            )
            return

        for child_ti in sorted(
            child_tis,
            key=lambda item: (
                str(getattr(item, "task_id", "")),
                int(getattr(item, "map_index", -1) or -1),
            ),
        ):
            self.log.info(
                "Child task log: task_id=%s map_index=%s state=%s try_number=%s log_url=%s",
                getattr(child_ti, "task_id", None),
                getattr(child_ti, "map_index", -1),
                getattr(child_ti, "state", None),
                getattr(child_ti, "try_number", None),
                getattr(child_ti, "log_url", None),
            )
            self.log.info(
                "--- CHILD TASK LOG START | parent_task_no=%s parent_task_group_id=%s child_task_id=%s map_index=%s try_number=%s ---",
                self.parent_task_number,
                self.parent_task_group_id,
                getattr(child_ti, "task_id", None),
                getattr(child_ti, "map_index", -1),
                getattr(child_ti, "try_number", None),
            )
            self._emit_child_task_log_lines(child_ti)
            self.log.info(
                "--- CHILD TASK LOG END | parent_task_no=%s parent_task_group_id=%s child_task_id=%s map_index=%s ---",
                self.parent_task_number,
                self.parent_task_group_id,
                getattr(child_ti, "task_id", None),
                getattr(child_ti, "map_index", -1),
            )
        self.log.info(
            "===== CHILD DAG LOG GROUP END | parent_task_no=%s parent_task_group_id=%s child_dag_id=%s =====",
            self.parent_task_number,
            self.parent_task_group_id,
            child_dag_id,
        )


def _run_script_task(task_def, source_conn_id, target_conn_id):
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


raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
if not isinstance(raw, dict):
    raise ValueError("YAML root must be a dict.")

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

start_date_raw = str(scheduler.get("start_date") or "2023-01-01T00:00:00").strip() or "2023-01-01T00:00:00"
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
effective_schedule = None if UPSTREAM_DAG_IDS else cron_expression
task_ids_with_upstream = {downstream for _upstream, downstream in edges}
root_task_ids = [task_id for task_id in [str(task.get("task_group_id") or "").strip() for task in task_defs] if task_id not in task_ids_with_upstream]
root_task_order = {task_id: idx + 1 for idx, task_id in enumerate(root_task_ids)}

with DAG(
    dag_id=DAG_ID,
    schedule=effective_schedule,
    start_date=dag_start_date,
    catchup=False,
    tags=DAG_TAGS,
    is_paused_upon_creation=not dag_active,
) as dag:
    task_groups = {}
    for task_index, task_def in enumerate(task_defs, start=1):
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
            if triggered_dag_id == DAG_ID:
                raise ValueError("dag_task_dag_id cannot reference itself.")
            trigger_task_id = _bounded_task_id(f"trigger_dag__{task_slug}")
            task_groups[task_group_id] = TriggerDagRunWithChildLogSummary(
                task_id=trigger_task_id,
                trigger_dag_id=triggered_dag_id,
                logical_date="{{ dag_run.logical_date }}",
                parent_task_number=task_index,
                parent_task_group_id=task_group_id,
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
            if UPSTREAM_DAG_IDS and task_group_id in root_task_ids:
                upstream_slug_parts = [_slug_task_token(uid) for uid in UPSTREAM_DAG_IDS]
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
                config_path=str(CONFIG_PATH),
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
                _config_path=str(CONFIG_PATH),
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
                _config_path=str(CONFIG_PATH),
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
                _config_path=str(CONFIG_PATH),
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

    if UPSTREAM_DAG_IDS:
        upstream_triggers = {}
        upstream_waiters = {}
        for upstream_dag_id in UPSTREAM_DAG_IDS:
            trigger_task_id = "trigger_upstream__" + re.sub(r"[^A-Za-z0-9_]+", "_", str(upstream_dag_id)).strip("_").lower()
            upstream_triggers[upstream_dag_id] = TriggerDagRunOperator(
                task_id=trigger_task_id,
                trigger_dag_id=upstream_dag_id,
                logical_date="{{ dag_run.logical_date }}",
                wait_for_completion=False,
                reset_dag_run=False,
                skip_when_already_exists=False,
            )
            waiter_task_id = "wait_upstream__" + re.sub(r"[^A-Za-z0-9_]+", "_", str(upstream_dag_id)).strip("_").lower()
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

