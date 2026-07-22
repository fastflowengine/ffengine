from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import ParamValidationError

from ffengine.airflow.generated_factory import _run_binding_task, build_generated_dag
from ffengine.airflow.operator import build_runtime_binding_context
from ffengine.config.binding_resolver import BindingResolver


pytestmark = pytest.mark.integration


class _DagRun:
    conf = {"business_date": "2026-07-20", "log_level": "DEBUG"}


class _NoBusinessDateDagRun:
    conf = {"log_level": "default"}


def test_airflow_dag_params_binding_xcom_supersedes_trigger_value(caplog):
    caplog.set_level(logging.INFO, logger="ffengine.airflow.generated_factory")
    raw = {
        "source_db_var": "src_conn",
        "target_db_var": "tgt_conn",
        "__config_path": "/opt/airflow/projects/demo/demo.yaml",
        "scheduler": {"timezone": "UTC", "active": True},
        "dag_params": [
            {
                "name": "log_level",
                "type": "string",
                "default": "default",
                "enum": ["default", "DEBUG"],
            },
            {
                "name": "business_date",
                "type": "string",
                "description": "Business date",
            },
        ],
        "flow_tasks": [
            {
                "task_group_id": "bind_business_date",
                "task_type": "binding",
                "bindings": [
                    {
                        "variable_name": "business_date",
                        "binding_source": "default",
                        "default_value": "2026-07-19",
                    }
                ],
                "depends_on": [],
            },
            {
                "task_group_id": "orders",
                "task_type": "source_target",
                "where": "business_date = {{ dag.business_date }}",
                "partitioning": {"enabled": False},
                "depends_on": ["bind_business_date"],
            },
        ],
    }
    dag = build_generated_dag(
        dag_id="params_binding_integration",
        dag_tags=["integration"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )
    binding_task = dag.task_dict["binding__bind_business_date"]
    consumer_task = dag.task_dict["run_orders"]
    assert dag.params["log_level"] == "default"
    assert consumer_task.task_id in binding_task.downstream_task_ids

    binding_xcom = _run_binding_task(
        raw["flow_tasks"][0],
        "src_conn",
        "tgt_conn",
        raw["dag_params"],
        {"params": dict(dag.params), "dag_run": _DagRun(), "ti": MagicMock()},
        superseded_sources={"business_date": "__dag_run_conf__"},
    )
    assert binding_xcom == {"business_date": "2026-07-19"}
    lifecycle_records = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("DAG_PARAMETER_INITIALIZED ")
    ]
    assert len(lifecycle_records) == 1
    assert lifecycle_records[0].splitlines() == [
        "DAG_PARAMETER_INITIALIZED name=business_date source=dag_run_conf "
        'value="2026-07-20"',
        "DAG_PARAMETER_ASSIGNED name=business_date source=default "
        'value="2026-07-19" supersedes=dag_run_conf xcom_output=true',
    ]

    ti = MagicMock()
    ti.xcom_pull.return_value = binding_xcom
    context = build_runtime_binding_context(
        {"params": dict(dag.params), "dag_run": _DagRun(), "ti": ti},
        airflow_variables={},
        binding_task_ids=[binding_task.task_id],
    )
    resolved = BindingResolver().resolve_sql_bindings(
        {"where": "business_date = {{ dag.business_date }}"},
        context=context,
        source_session=None,
        target_session=None,
    )
    assert resolved["_resolved_where"] == "business_date = '2026-07-19'"


def test_airflow_dag_parameter_reassignment_uses_compiled_latest_xcom():
    dag_params = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "business_date", "type": "string"},
    ]
    bindings = [
        {
            "task_group_id": "bind_initial_date",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "business_date",
                "binding_source": "default",
                "default_value": "2026-07-19",
            }],
            "depends_on": [],
        },
        {
            "task_group_id": "bind_updated_date",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "business_date",
                "binding_source": "default",
                "default_value": "2026-07-21",
            }],
            "depends_on": ["bind_initial_date"],
        },
    ]
    raw = {
        "source_db_var": "src_conn",
        "target_db_var": "tgt_conn",
        "__config_path": "/opt/airflow/projects/demo/demo.yaml",
        "scheduler": {"timezone": "UTC", "active": True},
        "dag_params": dag_params,
        "flow_tasks": bindings + [{
            "task_group_id": "orders",
            "task_type": "source_target",
            "where": "business_date = {{ dag.business_date }}",
            "partitioning": {"enabled": False},
            "depends_on": ["bind_updated_date"],
        }],
    }
    dag = build_generated_dag(
        dag_id="params_reassignment_integration",
        dag_tags=["integration"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )
    context = {
        "params": dict(dag.params),
        "dag_run": _NoBusinessDateDagRun(),
        "ti": MagicMock(),
    }
    initial_xcom = _run_binding_task(
        bindings[0], "src_conn", "tgt_conn", dag_params, context
    )
    updated_xcom = _run_binding_task(
        bindings[1], "src_conn", "tgt_conn", dag_params, context
    )
    assert initial_xcom == {"business_date": "2026-07-19"}
    assert updated_xcom == {"business_date": "2026-07-21"}

    ti = MagicMock()
    ti.xcom_pull.return_value = updated_xcom
    runtime_context = build_runtime_binding_context(
        {"params": dict(dag.params), "dag_run": _NoBusinessDateDagRun(), "ti": ti},
        airflow_variables={},
        binding_task_ids=["binding__bind_updated_date"],
    )
    resolved = BindingResolver().resolve_sql_bindings(
        {"where": "business_date = {{ dag.business_date }}"},
        context=runtime_context,
        source_session=None,
        target_session=None,
    )
    assert resolved["_resolved_where"] == "business_date = '2026-07-21'"


def test_partial_reassignment_keeps_unchanged_parameter_from_prior_binding():
    ti = MagicMock()
    xcom_values = {
        "binding__bind_initial": {"test1": 1, "test2": 10},
        "binding__bind_updated": {"test1": 2},
    }
    ti.xcom_pull.side_effect = lambda task_ids, key: xcom_values[task_ids]

    context = build_runtime_binding_context(
        {"params": {}, "dag_run": MagicMock(conf={}), "ti": ti},
        airflow_variables={},
        binding_sources={
            "test1": "binding__bind_updated",
            "test2": "binding__bind_initial",
        },
    )
    resolved = BindingResolver().resolve_sql_bindings(
        {"where": "x = {{ dag.test1 }} AND y = {{ dag.test2 }}"},
        context=context,
        source_session=None,
        target_session=None,
    )

    assert context["binding_values"] == {"test1": 2, "test2": 10}
    assert resolved["_resolved_where"] == "x = 2 AND y = 10"


def test_typed_binding_assignment_supersedes_trigger_for_downstream_tasks():
    dag_params = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "test1", "type": "integer"},
    ]
    bindings = [
        {
            "task_group_id": "bind_initial",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "test1",
                "binding_source": "default",
                "default_value": "1",
            }],
            "depends_on": [],
        },
        {
            "task_group_id": "bind_updated",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "test1",
                "binding_source": "default",
                "default_value": "2",
            }],
            "depends_on": ["bind_initial"],
        },
    ]
    raw = {
        "source_db_var": "src_conn",
        "target_db_var": "tgt_conn",
        "__config_path": "/opt/airflow/projects/demo/demo.yaml",
        "scheduler": {"timezone": "UTC", "active": True},
        "dag_params": dag_params,
        "flow_tasks": bindings + [{
            "task_group_id": "consumer",
            "task_type": "source_target",
            "where": "batch_limit = {{ dag.test1 }}",
            "partitioning": {"enabled": False},
            "depends_on": ["bind_updated"],
        }],
    }
    dag = build_generated_dag(
        dag_id="typed_params_integration",
        dag_tags=["integration"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )
    param = dag.params.get_param("test1")
    assert param.resolve("3") == "3"
    with pytest.raises(ParamValidationError):
        param.resolve("3x")

    trigger_run = MagicMock(conf={"test1": "3"})
    task_context = {
        "dag": dag,
        "params": dict(dag.params),
        "dag_run": trigger_run,
        "ti": MagicMock(),
    }
    first_xcom = _run_binding_task(
        bindings[0], "src_conn", "tgt_conn", dag_params, task_context
    )
    second_xcom = _run_binding_task(
        bindings[1], "src_conn", "tgt_conn", dag_params, task_context
    )
    assert first_xcom == {"test1": 1}
    assert second_xcom == {"test1": 2}

    ti = MagicMock()
    ti.xcom_pull.return_value = second_xcom
    trigger_context = build_runtime_binding_context(
        {"dag": dag, "params": dict(dag.params), "dag_run": trigger_run, "ti": ti},
        airflow_variables={},
        binding_task_ids=["binding__bind_updated"],
    )
    assert trigger_context["dag_run_conf"] == {"test1": 3}
    resolved = BindingResolver().resolve_sql_bindings(
        {"where": "batch_limit = {{ dag.test1 }}"},
        context=trigger_context,
        source_session=None,
        target_session=None,
    )
    assert resolved["_resolved_where"] == "batch_limit = 2"

    null_run = MagicMock(conf={"test1": None})
    fallback_context = build_runtime_binding_context(
        {"dag": dag, "params": dict(dag.params), "dag_run": null_run, "ti": ti},
        airflow_variables={},
        binding_task_ids=["binding__bind_updated"],
    )
    assert fallback_context["dag_run_conf"] == {}
    assert fallback_context["binding_values"] == {"test1": 2}
