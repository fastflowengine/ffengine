"""
C07 — 3-fazlı DAG pattern: plan → prepare → run.

build_task_group() bir Airflow DAG içinde 3 aşamalı TaskGroup oluşturur.
Her aşama XCom üzerinden veri paylaşır.
"""

import logging

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# XCom anahtarları
# ---------------------------------------------------------------------------


class XComKeys:
    """FFEngine standart XCom key sabitleri."""

    TASK_CONFIG_RESOLVED = "task_config_resolved"
    PARTITION_SPECS = "partition_specs"
    ROWS_TRANSFERRED = "rows_transferred"
    DURATION_SECONDS = "duration_seconds"
    ROWS_PER_SECOND = "rows_per_second"


# ---------------------------------------------------------------------------
# 3-fazlı TaskGroup oluşturucu
# ---------------------------------------------------------------------------


def build_task_group(
    dag,
    *,
    config_path: str,
    task_group_id: str,
    source_conn_id: str,
    target_conn_id: str,
    group_id: str = "ffengine_etl",
    airflow_context: dict | None = None,
):
    """
    Bir Airflow DAG içinde 3-fazlı ETL TaskGroup oluşturur.

    Fazlar:
      1. plan_partitions  — config yükle, binding/mapping çöz, partition planla
      2. prepare_target   — TargetWriter.prepare() (bir kez)
      3. run_partitions   — her partition için ETLManager.run_etl_task()

    Bağımlılık: plan >> prepare >> run

    Parameters
    ----------
    dag              : Airflow DAG nesnesi.
    config_path      : YAML config dosya yolu.
    task_group_id    : Çalıştırılacak task kimliği.
    source_conn_id   : Airflow kaynak Connection ID.
    target_conn_id   : Airflow hedef Connection ID.
    group_id         : TaskGroup kimliği (varsayılan: "ffengine_etl").
    airflow_context  : BindingResolver context dict (test/CLI için).

    Returns
    -------
    TaskGroup nesnesi (plan >> prepare >> run bağımlılığı ile).
    """
    try:
        from airflow.sdk import TaskGroup
        from airflow.sdk.definitions.operators.python import PythonOperator
    except ImportError:
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.python import PythonOperator

    with TaskGroup(group_id=group_id, dag=dag) as tg:

        def _plan(**kwargs):
            from ffengine.config.loader import ConfigLoader
            from ffengine.config.binding_resolver import BindingResolver
            from ffengine.db.airflow_adapter import AirflowConnectionAdapter
            from ffengine.db.session import DBSession
            from ffengine.mapping import MappingResolver
            from ffengine.partition import Partitioner
            from ffengine.airflow.operator import resolve_dialect, build_airflow_variable_context

            task_config = ConfigLoader().load(config_path, task_group_id)

            src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
            tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)

            src_dialect = resolve_dialect(src_params["conn_type"])
            tgt_dialect = resolve_dialect(tgt_params["conn_type"])

            ctx = airflow_context or build_airflow_variable_context()
            task_config = BindingResolver().resolve(task_config, ctx)

            with DBSession(src_params, src_dialect) as src_session:
                mapping = MappingResolver().resolve(
                    task_config, src_session.conn, src_dialect, tgt_dialect,
                )
                task_config["source_columns"] = mapping.source_columns
                task_config["target_columns"] = mapping.target_columns
                # ColumnInfo → dict (XCom JSON serializable)
                task_config["target_columns_meta"] = [
                    {
                        "name": c.name,
                        "data_type": c.data_type,
                        "nullable": c.nullable,
                        "precision": c.precision,
                        "scale": c.scale,
                    }
                    for c in mapping.target_columns_meta
                ]

                specs = Partitioner().plan(
                    task_config, src_session.conn, src_dialect,
                )

            ti = kwargs["ti"]
            ti.xcom_push(key=XComKeys.TASK_CONFIG_RESOLVED, value=task_config)
            ti.xcom_push(key=XComKeys.PARTITION_SPECS, value=specs)

            _log.info(
                "plan_partitions: %d partition, %d kolon",
                len(specs),
                len(task_config.get("source_columns", [])),
            )

        def _prepare(**kwargs):
            from ffengine.db.airflow_adapter import AirflowConnectionAdapter
            from ffengine.db.session import DBSession
            from ffengine.pipeline.target_writer import TargetWriter
            from ffengine.airflow.operator import resolve_dialect

            ti = kwargs["ti"]
            task_config = ti.xcom_pull(
                task_ids=f"{group_id}.plan_partitions",
                key=XComKeys.TASK_CONFIG_RESOLVED,
            )

            tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
            tgt_dialect = resolve_dialect(tgt_params["conn_type"])

            with DBSession(tgt_params, tgt_dialect) as tgt_session:
                writer = TargetWriter(tgt_session, tgt_dialect)
                writer.prepare(task_config)

            _log.info("prepare_target: load_method=%s", task_config.get("load_method"))

        def _run(**kwargs):
            from ffengine.db.airflow_adapter import AirflowConnectionAdapter
            from ffengine.db.session import DBSession
            from ffengine.core.etl_manager import ETLManager
            from ffengine.airflow.operator import resolve_dialect, combine_where

            ti = kwargs["ti"]
            task_config = ti.xcom_pull(
                task_ids=f"{group_id}.plan_partitions",
                key=XComKeys.TASK_CONFIG_RESOLVED,
            )
            specs = ti.xcom_pull(
                task_ids=f"{group_id}.plan_partitions",
                key=XComKeys.PARTITION_SPECS,
            )

            src_params = AirflowConnectionAdapter.get_connection_params(source_conn_id)
            tgt_params = AirflowConnectionAdapter.get_connection_params(target_conn_id)
            src_dialect = resolve_dialect(src_params["conn_type"])
            tgt_dialect = resolve_dialect(tgt_params["conn_type"])

            base_where = task_config.get("_resolved_where")
            total_rows = 0
            max_duration = 0.0
            all_errors = []

            with DBSession(src_params, src_dialect) as src_session:
                with DBSession(tgt_params, tgt_dialect) as tgt_session:
                    manager = ETLManager()
                    for spec in specs:
                        effective = dict(task_config)
                        effective["_resolved_where"] = combine_where(
                            base_where, spec.get("where"),
                        )
                        result = manager.run_etl_task(
                            src_session=src_session,
                            tgt_session=tgt_session,
                            src_dialect=src_dialect,
                            tgt_dialect=tgt_dialect,
                            task_config=effective,
                            partition_spec=None,
                            skip_prepare=True,
                        )
                        total_rows += result.rows
                        if result.duration_seconds > max_duration:
                            max_duration = result.duration_seconds
                        all_errors.extend(result.errors)

            ti.xcom_push(key=XComKeys.ROWS_TRANSFERRED, value=total_rows)
            ti.xcom_push(key=XComKeys.DURATION_SECONDS, value=max_duration)
            throughput = total_rows / max_duration if max_duration > 0 else 0.0
            ti.xcom_push(key=XComKeys.ROWS_PER_SECOND, value=round(throughput, 2))

            _log.info(
                "run_partitions: %d partition, %d rows, %.1fs",
                len(specs), total_rows, max_duration,
            )

        plan_task = PythonOperator(
            task_id="plan_partitions",
            python_callable=_plan,
            dag=dag,
            task_group=tg,
        )
        prepare_task = PythonOperator(
            task_id="prepare_target",
            python_callable=_prepare,
            dag=dag,
            task_group=tg,
        )
        run_task = PythonOperator(
            task_id="run_partitions",
            python_callable=_run,
            dag=dag,
            task_group=tg,
        )

        plan_task >> prepare_task >> run_task

    return tg
