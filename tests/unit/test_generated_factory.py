from __future__ import annotations

import inspect
import logging
import pytest
from unittest.mock import MagicMock, patch

from airflow.exceptions import ParamValidationError

from ffengine.airflow.generated_factory import (
    _build_dag_params,
    _run_binding_task,
    _validate_binding_param_values,
    build_generated_dag,
)
from ffengine.airflow.operator import FFEngineOperator
from ffengine.errors.exceptions import EngineError


def _lifecycle_messages(caplog) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith(
            ("DAG_PARAMETER_INITIALIZED ", "DAG_PARAMETER_ASSIGNED ")
        )
    ]


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


def test_partition_wrapper_runs_guard_before_binding_xcom_pull():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_date",
            "task_type": "binding",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-08-11",
                }
            ],
            "depends_on": [],
        },
        {
            "task_group_id": "partitioned_task",
            "task_type": "source_target",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_date"],
            "partitioning": {"enabled": True},
        },
    ]
    dag = build_generated_dag(
        dag_id="guard_before_binding",
        dag_tags=[],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )
    wrapper = dag.task_dict["flow__partitioned_task.plan_partitions"]
    ti = MagicMock(task_id=wrapper.task_id)
    ti.xcom_pull.return_value = {"run_date": "2026-08-11"}
    dag_run = MagicMock(
        dag_id=dag.dag_id, run_id="manual__1", start_date="2026-08-11"
    )

    with (
        patch(
            "ffengine.airflow.generated_factory.get_current_context",
            return_value={"ti": ti, "dag_run": dag_run},
        ),
        patch(
            "ffengine.core.runtime_guard.run_runtime_guards",
            side_effect=EngineError("license-stop"),
        ) as guard,
    ):
        with pytest.raises(EngineError, match="license-stop"):
            wrapper.python_callable()

    ti.xcom_pull.assert_not_called()
    guard.assert_called_once_with(
        {
            "dag_id": "guard_before_binding",
            "task_id": "flow__partitioned_task.plan_partitions",
            "run_id": "manual__1",
            "dag_run_start_date": "2026-08-11",
        }
    )


def test_dag_params_become_airflow_params_and_binding_is_thin_task():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {
            "name": "log_level",
            "type": "string",
            "default": "default",
            "enum": ["default", "DEBUG"],
        },
        {"name": "run_date", "type": "string", "description": "Run date"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_run_date",
            "task_type": "binding",
            "depends_on": [],
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-02",
                }
            ],
        },
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "depends_on": ["bind_run_date"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="demo_params_binding",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert dag.params["log_level"] == "default"
    assert dag.params["run_date"] is None
    assert "binding__bind_run_date" in dag.task_dict
    binding_task = dag.task_dict["binding__bind_run_date"]
    assert "run_orders_task" in binding_task.downstream_task_ids
    assert binding_task.show_return_value_in_logs is False


@pytest.mark.parametrize(
    ("param_type", "accepted", "rejected", "expected_type", "pattern"),
    [
        ("integer", "3", "3x", ["string", "integer", "null"], r"^-?\d+$"),
        (
            "number",
            "3.5",
            "3.5x",
            ["string", "number", "null"],
            r"^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$",
        ),
        ("boolean", "true", "yes", ["string", "boolean", "null"], r"^(true|false)$"),
        ("string", "value", 3, ["string", "null"], None),
    ],
)
def test_optional_dag_param_schema_accepts_only_valid_string_representations(
    param_type, accepted, rejected, expected_type, pattern
):
    param = _build_dag_params([{"name": "value", "type": param_type}])["value"]

    assert param.schema["type"] == expected_type
    assert param.schema["x-ffengine-type"] == param_type
    assert param.schema["x-ffengine-schema-version"] == 2
    assert param.schema.get("pattern") == pattern
    assert param.resolve(accepted) == accepted
    with pytest.raises(ParamValidationError):
        param.resolve(rejected)


def test_binding_values_are_coerced_and_validated_against_param_schema():
    declarations = {
        "batch_limit": {"name": "batch_limit", "type": "integer"},
        "enabled": {"name": "enabled", "type": "boolean"},
    }

    values = _validate_binding_param_values(
        {"batch_limit": "25", "enabled": "true"}, declarations
    )

    assert values == {"batch_limit": 25, "enabled": True}


def test_first_binding_logs_trigger_initial_value_and_new_assignment(caplog):
    caplog.set_level(logging.INFO, logger="ffengine.airflow.generated_factory")
    dag_run = MagicMock()
    dag_run.conf = {"test1": "dag parameter değeri", "log_level": "default"}

    values = _run_binding_task(
        {
            "bindings": [
                {
                    "variable_name": "test1",
                    "binding_source": "default",
                    "default_value": "selam task içinden",
                }
            ]
        },
        "unused_source",
        "unused_target",
        [{"name": "test1", "type": "string"}],
        {"dag_run": dag_run, "params": {}, "ti": MagicMock()},
        superseded_sources={"test1": "__dag_run_conf__"},
    )

    assert values == {"test1": "selam task içinden"}
    assert _lifecycle_messages(caplog) == [
        "DAG_PARAMETER_INITIALIZED name=test1 source=dag_run_conf "
        'value="dag parameter değeri"\n'
        "DAG_PARAMETER_ASSIGNED name=test1 source=default "
        'value="selam task içinden" supersedes=dag_run_conf xcom_output=true'
    ]


def test_binding_task_logs_typed_assignments_without_trigger(caplog):
    caplog.set_level(logging.INFO, logger="ffengine.airflow.generated_factory")
    dag_run = MagicMock()
    dag_run.conf = {}

    values = _run_binding_task(
        {
            "bindings": [
                {
                    "variable_name": "batch_limit",
                    "binding_source": "default",
                    "default_value": "25",
                },
                {
                    "variable_name": "enabled",
                    "binding_source": "default",
                    "default_value": "true",
                },
            ]
        },
        "unused_source",
        "unused_target",
        [
            {"name": "batch_limit", "type": "integer"},
            {"name": "enabled", "type": "boolean"},
        ],
        {"dag_run": dag_run, "params": {}, "ti": MagicMock()},
        superseded_sources={
            "batch_limit": "__dag_run_conf__",
            "enabled": "__dag_run_conf__",
        },
    )

    assert values == {"batch_limit": 25, "enabled": True}
    assert _lifecycle_messages(caplog) == [
        "DAG_PARAMETER_INITIALIZED name=batch_limit source=none value=null\n"
        "DAG_PARAMETER_ASSIGNED name=batch_limit source=default "
        "value=25 supersedes=none xcom_output=true",
        "DAG_PARAMETER_INITIALIZED name=enabled source=none value=null\n"
        "DAG_PARAMETER_ASSIGNED name=enabled source=default "
        "value=true supersedes=none xcom_output=true",
    ]


def test_binding_lifecycle_logs_typed_trigger_and_each_reassignment(caplog):
    caplog.set_level(logging.INFO, logger="ffengine.airflow.generated_factory")
    dag_run = MagicMock()
    dag_run.conf = {"test1": "3"}
    param = MagicMock()
    param.schema = {"x-ffengine-type": "integer"}
    dag = MagicMock()
    dag.params.get_param.return_value = param

    ti = MagicMock()
    first_values = _run_binding_task(
        {
            "bindings": [{
                "variable_name": "test1",
                "binding_source": "default",
                "default_value": "1",
            }]
        },
        "unused_source",
        "unused_target",
        [{"name": "test1", "type": "integer"}],
        {"dag": dag, "dag_run": dag_run, "params": {}, "ti": ti},
        superseded_sources={"test1": "__dag_run_conf__"},
    )
    second_values = _run_binding_task(
        {
            "bindings": [{
                "variable_name": "test1",
                "binding_source": "default",
                "default_value": "2",
            }]
        },
        "unused_source",
        "unused_target",
        [{"name": "test1", "type": "integer"}],
        {"dag": dag, "dag_run": dag_run, "params": {}, "ti": ti},
        superseded_sources={"test1": "bind_initial"},
    )

    assert first_values == {"test1": 1}
    assert second_values == {"test1": 2}
    ti.xcom_pull.assert_not_called()

    messages = _lifecycle_messages(caplog)
    assert len(messages) == 2
    assert messages[0].splitlines() == [
        "DAG_PARAMETER_INITIALIZED name=test1 source=dag_run_conf value=3",
        "DAG_PARAMETER_ASSIGNED name=test1 source=default "
        "value=1 supersedes=dag_run_conf xcom_output=true",
    ]
    assert messages[1] == (
        "DAG_PARAMETER_ASSIGNED name=test1 source=default value=2 "
        "supersedes=bind_initial xcom_output=true"
    )


def test_binding_lifecycle_log_uses_controlled_lines_and_excludes_context(caplog):
    caplog.set_level(logging.INFO, logger="ffengine.airflow.generated_factory")
    dag_run = MagicMock()
    dag_run.conf = {
        "test1": "first line\nsecond line",
        "unrelated": "DAGRUN_SECRET_SENTINEL",
    }

    _run_binding_task(
        {
            "bindings": [
                {
                    "variable_name": "test1",
                    "binding_source": "default",
                    "default_value": "binding value",
                    "sql": "SQL_SECRET_SENTINEL",
                }
            ]
        },
        "SOURCE_CONNECTION_SECRET_SENTINEL",
        "TARGET_CONNECTION_SECRET_SENTINEL",
        [{"name": "test1", "type": "string"}],
        {
            "dag_run": dag_run,
            "params": {},
            "ti": MagicMock(),
            "airflow_variables": {"secret.key": "AIRFLOW_VARIABLE_SECRET_SENTINEL"},
        },
        superseded_sources={"test1": "__dag_run_conf__"},
    )

    records = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("DAG_PARAMETER_INITIALIZED ")
    ]
    assert len(records) == 1
    assert len(records[0].splitlines()) == 2
    assert 'value="first line\\nsecond line"' in records[0]
    assert "DAGRUN_SECRET_SENTINEL" not in records[0]
    assert "AIRFLOW_VARIABLE_SECRET_SENTINEL" not in records[0]
    assert "SQL_SECRET_SENTINEL" not in records[0]
    assert "CONNECTION_SECRET_SENTINEL" not in records[0]


def test_binding_value_rejects_undeclared_dag_param():
    with pytest.raises(ValueError, match="not a declared DAG parameter"):
        _validate_binding_param_values({"secret": "value"}, {})


def test_binding_value_rejects_builtin_log_level_target():
    declarations = {
        "log_level": {
            "name": "log_level",
            "type": "string",
            "default": "default",
            "enum": ["default", "DEBUG"],
        }
    }

    with pytest.raises(
        ValueError,
        match="Built-in DAG parameter 'log_level' cannot be assigned",
    ):
        _validate_binding_param_values({"log_level": "DEBUG"}, declarations)


def test_factory_allows_trigger_only_dag_param_without_binding_assignment():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": [],
            "partitioning": {"enabled": False},
        }
    ]

    dag = build_generated_dag(
        dag_id="trigger_only",
        dag_tags=[],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert dag.task_dict["run_orders_task"].binding_task_ids == []


def test_factory_propagates_binding_through_transitive_task_dependency():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_date",
            "task_type": "binding",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
        },
        {
            "task_group_id": "bridge_task",
            "task_type": "source_target",
            "depends_on": ["bind_date"],
            "partitioning": {"enabled": False},
        },
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bridge_task"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="transitive_binding_dependency",
        dag_tags=[],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert dag.task_dict["run_orders_task"].binding_task_ids == [
        "binding__bind_date"
    ]


def test_factory_allows_ordered_parameter_reassignment():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_initial_date",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-01",
            }],
            "depends_on": [],
        },
        {
            "task_group_id": "bind_updated_date",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-02",
            }],
            "depends_on": ["bind_initial_date"],
        },
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_updated_date"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="ordered_reassignment",
        dag_tags=[],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    initial = dag.task_dict["binding__bind_initial_date"]
    updated = dag.task_dict["binding__bind_updated_date"]
    consumer = dag.task_dict["run_orders_task"]
    assert updated.task_id in initial.downstream_task_ids
    assert consumer.task_id in updated.downstream_task_ids
    wrapper_params = inspect.signature(updated.python_callable).parameters
    assert "_binding_task_ids" not in wrapper_params


def test_factory_carries_latest_binding_source_per_referenced_parameter():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "test1", "type": "integer"},
        {"name": "test2", "type": "integer"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_initial",
            "task_type": "binding",
            "bindings": [
                {
                    "variable_name": "test1",
                    "binding_source": "default",
                    "default_value": "1",
                },
                {
                    "variable_name": "test2",
                    "binding_source": "default",
                    "default_value": "10",
                },
            ],
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
        {
            "task_group_id": "consumer",
            "task_type": "source_target",
            "where": "x = {{ dag.test1 }} AND y = {{ dag.test2 }}",
            "depends_on": ["bind_updated"],
            "partitioning": {"enabled": False},
        },
    ]

    dag = build_generated_dag(
        dag_id="partial_reassignment",
        dag_tags=[],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    consumer = dag.task_dict["run_consumer"]
    assert consumer.binding_sources == {
        "test1": "binding__bind_updated",
        "test2": "binding__bind_initial",
    }


def test_factory_rejects_ambiguous_parameter_sources_at_branch_merge():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_date_a",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-01",
            }],
            "depends_on": [],
        },
        {
            "task_group_id": "bind_date_b",
            "task_type": "binding",
            "bindings": [{
                "variable_name": "run_date",
                "binding_source": "default",
                "default_value": "2026-01-02",
            }],
            "depends_on": [],
        },
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ dag.run_date }}",
            "depends_on": ["bind_date_a", "bind_date_b"],
            "partitioning": {"enabled": False},
        },
    ]

    with pytest.raises(ValueError, match="Ambiguous DAG parameter source"):
        build_generated_dag(
            dag_id="ambiguous_reassignment",
            dag_tags=[],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def test_factory_rejects_custom_dag_parameter_default():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string", "default": "2026-01-01"},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ run_date }}",
            "depends_on": [],
            "partitioning": {"enabled": False},
        }
    ]

    with pytest.raises(ValueError, match="must not define default, required, or enum"):
        build_generated_dag(
            dag_id="invalid_custom_default",
            dag_tags=[],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def test_factory_rejects_custom_dag_parameter_required_field():
    raw = _base_raw_config()
    raw["dag_params"] = [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "run_date", "type": "string", "required": False},
    ]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_date",
            "task_type": "binding",
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-01",
                }
            ],
        }
    ]

    with pytest.raises(ValueError, match="must not define default, required, or enum"):
        build_generated_dag(
            dag_id="invalid_custom_required",
            dag_tags=[],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def test_airflow_namespace_does_not_read_variable_at_parse_time():
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "where": "business_date = {{ airflow.etl.business_date }}",
            "depends_on": [],
            "partitioning": {"enabled": False},
        }
    ]

    with patch("airflow.models.Variable.get") as variable_get:
        build_generated_dag(
            dag_id="airflow_namespace",
            dag_tags=[],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )

    variable_get.assert_not_called()


# --- F3.2: dbt task via the task-type provider seam --------------------------

@pytest.fixture
def dbt_provider_registry(monkeypatch):
    from ffengine.airflow import task_type_registry as reg

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    reg.clear_task_type_providers()
    yield reg
    reg.clear_task_type_providers()


def _dbt_task_def(task_id="dbt_build", depends_on=None, vars_map=None) -> dict:
    task = {
        "task_group_id": task_id,
        "task_type": "dbt",
        "depends_on": list(depends_on or []),
        "dbt_project_ref": "finance",
        "dbt_command": "build",
        "dbt_select": "tag:nightly",
    }
    if vars_map is not None:
        task["dbt_vars"] = vars_map
    return task


def test_build_generated_dag_dbt_task_is_thin_wrapper_and_wires_depends_on(
    dbt_provider_registry,
):
    from airflow.providers.standard.operators.empty import EmptyOperator

    captured = {}

    def provider(**kwargs):
        captured.update(kwargs)
        return EmptyOperator(task_id=kwargs["task_id"])

    dbt_provider_registry.register_task_type_provider("dbt", provider)

    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "depends_on": [],
            "partitioning": {"enabled": False},
        },
        _dbt_task_def(depends_on=["orders_task"]),
    ]

    dag = build_generated_dag(
        dag_id="demo_dbt",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert "dbt__dbt_build" in dag.task_dict
    assert captured["task_id"] == "dbt__dbt_build"
    assert captured["source_conn_id"] == "src_conn"
    assert captured["target_conn_id"] == "tgt_conn"
    assert captured["task_def"]["dbt_project_ref"] == "finance"
    assert captured["binding_sources"] == {}
    assert not isinstance(dag.task_dict["dbt__dbt_build"], FFEngineOperator)
    assert "dbt__dbt_build" in dag.task_dict["run_orders_task"].downstream_task_ids


def test_dbt_task_receives_compiled_binding_sources(dbt_provider_registry):
    from airflow.providers.standard.operators.empty import EmptyOperator

    captured = {}

    def provider(**kwargs):
        captured.update(kwargs)
        return EmptyOperator(task_id=kwargs["task_id"])

    dbt_provider_registry.register_task_type_provider("dbt", provider)

    raw = _base_raw_config()
    raw["dag_params"] = [{"name": "run_date", "type": "string"}]
    raw["flow_tasks"] = [
        {
            "task_group_id": "bind_run_date",
            "task_type": "binding",
            "depends_on": [],
            "bindings": [
                {
                    "variable_name": "run_date",
                    "binding_source": "default",
                    "default_value": "2026-01-02",
                }
            ],
        },
        _dbt_task_def(
            depends_on=["bind_run_date"],
            vars_map={"run_date": "{{ dag.run_date }}"},
        ),
    ]

    build_generated_dag(
        dag_id="demo_dbt_vars",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=raw,
    )

    assert captured["binding_sources"] == {"run_date": "binding__bind_run_date"}


def test_generated_dag_dbt_without_provider_fails_loud_at_parse(
    dbt_provider_registry,
):
    raw = _base_raw_config()
    raw["flow_tasks"] = [_dbt_task_def()]

    with pytest.raises(ValueError, match="task-type provider"):
        build_generated_dag(
            dag_id="demo_dbt_no_provider",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def _asset_raw_config(assets=None, cron=None):
    raw = _base_raw_config()
    raw["flow_tasks"] = [
        {
            "task_group_id": "orders_task",
            "task_type": "source_target",
            "depends_on": [],
            "partitioning": {"enabled": False},
        },
    ]
    raw["scheduler"] = {
        "trigger_type": "asset",
        "assets": (
            assets
            if assets is not None
            else [
                "postgres://db/analytics/fct_orders",
                "postgres://db/analytics/dim_customer",
            ]
        ),
    }
    if cron:
        raw["scheduler"]["cron_expression"] = cron
    return raw


def test_generated_dag_renders_asset_and_schedule(dbt_provider_registry):
    # F3.2b Faz 6: v1 semantics = list of Assets = AND-only. No sensor, no
    # polling loop, no catalog — Airflow's native Asset timetable.
    dbt_provider_registry.register_task_type_provider(
        "dbt", lambda **kwargs: None
    )
    dag = build_generated_dag(
        dag_id="demo_asset_trigger",
        dag_tags=["demo"],
        upstream_dag_ids=[],
        raw_config_snapshot=_asset_raw_config(),
    )
    condition = dag.timetable.asset_condition
    if hasattr(condition, "iter_assets"):
        # Airflow 3.1 compatibility.
        assets = [asset for _key, asset in condition.iter_assets()]
    else:
        # Airflow 3.2+: AssetAll exposes its operands through `objects`.
        assets = list(condition.objects)
    uris = sorted(asset.uri for asset in assets)
    assert uris == [
        "postgres://db/analytics/dim_customer",
        "postgres://db/analytics/fct_orders",
    ]


def test_generated_dag_asset_trigger_is_edition_gated(monkeypatch):
    from ffengine.airflow import task_type_registry as reg

    monkeypatch.setenv("FFENGINE_EDITION", "community")
    reg.clear_task_type_providers()
    try:
        with pytest.raises(ValueError, match="Enterprise"):
            build_generated_dag(
                dag_id="demo_asset_gate",
                dag_tags=["demo"],
                upstream_dag_ids=[],
                raw_config_snapshot=_asset_raw_config(),
            )
    finally:
        reg.clear_task_type_providers()


def test_generated_dag_asset_trigger_rejects_upstream_deps(
    dbt_provider_registry,
):
    dbt_provider_registry.register_task_type_provider(
        "dbt", lambda **kwargs: None
    )
    with pytest.raises(ValueError, match="upstream"):
        build_generated_dag(
            dag_id="demo_asset_upstream",
            dag_tags=["demo"],
            upstream_dag_ids=["other_dag"],
            raw_config_snapshot=_asset_raw_config(),
        )


def test_generated_dag_asset_trigger_rejects_cron_combination(
    dbt_provider_registry,
):
    dbt_provider_registry.register_task_type_provider(
        "dbt", lambda **kwargs: None
    )
    with pytest.raises(ValueError, match="cron_expression"):
        build_generated_dag(
            dag_id="demo_asset_cron",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=_asset_raw_config(cron="0 3 * * *"),
        )


def test_generated_dag_asset_trigger_rejects_empty_assets(
    dbt_provider_registry,
):
    dbt_provider_registry.register_task_type_provider(
        "dbt", lambda **kwargs: None
    )
    with pytest.raises(ValueError, match="at least one"):
        build_generated_dag(
            dag_id="demo_asset_empty",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=_asset_raw_config(assets=[]),
        )


def test_generated_dag_unknown_trigger_type_fails_loud(
    dbt_provider_registry,
):
    # Hand-edited RAW_CONFIG with e.g. 'Asset' must fail the parse, not
    # silently degrade to cron/manual scheduling (review finding).
    raw = _asset_raw_config()
    raw["scheduler"]["trigger_type"] = "Asset"
    with pytest.raises(ValueError, match="Unsupported scheduler.trigger_type"):
        build_generated_dag(
            dag_id="demo_asset_typo",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )


def test_generated_dag_assets_without_asset_mode_fail_loud(
    dbt_provider_registry,
):
    # scheduler.assets present while trigger_type is cron/missing would be
    # silently dropped — the factory must reject the contradiction.
    raw = _asset_raw_config(cron="0 3 * * *")
    raw["scheduler"]["trigger_type"] = "cron"
    with pytest.raises(ValueError, match="only valid with"):
        build_generated_dag(
            dag_id="demo_assets_cron_contradiction",
            dag_tags=["demo"],
            upstream_dag_ids=[],
            raw_config_snapshot=raw,
        )
