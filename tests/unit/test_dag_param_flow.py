from __future__ import annotations

import pytest

from ffengine.config.dag_param_flow import (
    AMBIGUOUS_SOURCE,
    TRIGGER_SOURCE,
    compile_dag_parameter_flow,
)


def _params() -> list[dict]:
    return [
        {"name": "log_level", "type": "string", "default": "default"},
        {"name": "test1", "type": "integer"},
    ]


def _binding(task_id: str, value: str, depends_on: list[str]) -> dict:
    return {
        "task_group_id": task_id,
        "task_type": "binding",
        "depends_on": depends_on,
        "bindings": [{
            "variable_name": "test1",
            "binding_source": "default",
            "default_value": value,
        }],
    }


def _query(task_id: str, depends_on: list[str]) -> dict:
    return {
        "task_group_id": task_id,
        "task_type": "source_target",
        "depends_on": depends_on,
        "where": "value = {{ dag.test1 }}",
    }


def test_linear_flow_propagates_latest_binding_across_non_binding_tasks():
    tasks = [
        _binding("bind_1", "1", []),
        _query("query_1", ["bind_1"]),
        _query("query_2", ["query_1"]),
        _binding("bind_2", "2", ["query_2"]),
        _query("query_3", ["bind_2"]),
    ]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.input_sources["bind_1"]["test1"] == TRIGGER_SOURCE
    assert plan.input_sources["query_1"]["test1"] == "bind_1"
    assert plan.input_sources["query_2"]["test1"] == "bind_1"
    assert plan.input_sources["bind_2"]["test1"] == "bind_1"
    assert plan.input_sources["query_3"]["test1"] == "bind_2"
    assert plan.binding_task_ids_for("query_2") == ["bind_1"]
    assert plan.binding_task_ids_for("query_3") == ["bind_2"]


def test_trigger_only_parameter_requires_no_binding_assignment():
    tasks = [_query("query_from_trigger", [])]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.input_sources["query_from_trigger"]["test1"] == TRIGGER_SOURCE
    assert plan.binding_task_ids_for("query_from_trigger") == []


def test_partial_reassignment_selects_latest_producer_per_parameter():
    params = _params() + [{"name": "test2", "type": "integer"}]
    bind_initial = _binding("bind_initial", "1", [])
    bind_initial["bindings"].append({
        "variable_name": "test2",
        "binding_source": "default",
        "default_value": "10",
    })
    bind_updated = _binding("bind_updated", "2", ["bind_initial"])
    consumer = {
        "task_group_id": "consumer",
        "task_type": "source_target",
        "depends_on": ["bind_updated"],
        "where": "x = {{ dag.test1 }} AND y = {{ dag.test2 }}",
    }

    plan = compile_dag_parameter_flow(
        params, [bind_initial, bind_updated, consumer]
    )

    assert plan.binding_sources_for("consumer") == {
        "test1": "bind_updated",
        "test2": "bind_initial",
    }


def test_branch_conflict_fails_when_parameter_is_consumed():
    tasks = [
        _binding("bind_left", "1", []),
        _binding("bind_right", "2", []),
        _query("merge_query", ["bind_left", "bind_right"]),
    ]

    with pytest.raises(ValueError, match="Ambiguous DAG parameter source.*test1"):
        compile_dag_parameter_flow(_params(), tasks)


def test_binding_at_merge_reconciles_ambiguous_branch_values():
    tasks = [
        _binding("bind_left", "1", []),
        _binding("bind_right", "2", []),
        _binding("bind_reconciled", "3", ["bind_left", "bind_right"]),
        _query("query_after_merge", ["bind_reconciled"]),
    ]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.input_sources["bind_reconciled"]["test1"] == AMBIGUOUS_SOURCE
    assert plan.input_sources["query_after_merge"]["test1"] == "bind_reconciled"


def test_binding_task_sources_are_restricted_and_targets_are_declared():
    invalid_source = _binding("bind_invalid", "1", [])
    invalid_source["bindings"][0]["binding_source"] = "airflow_variable"
    with pytest.raises(ValueError, match="source, target, or default"):
        compile_dag_parameter_flow(_params(), [invalid_source])

    undeclared = _binding("bind_undeclared", "1", [])
    undeclared["bindings"][0]["variable_name"] = "missing"
    with pytest.raises(ValueError, match="not a declared DAG parameter"):
        compile_dag_parameter_flow(_params(), [undeclared])


def test_binding_task_rejects_builtin_log_level_target():
    binding = _binding("bind_log_level", "DEBUG", [])
    binding["bindings"][0]["variable_name"] = "log_level"

    with pytest.raises(
        ValueError,
        match="Built-in DAG parameter 'log_level' cannot be assigned",
    ):
        compile_dag_parameter_flow(_params(), [binding])


# --- F3.2: dbt task param references come from dbt_vars ----------------------

def _dbt(task_id: str, depends_on: list[str], vars_map: dict) -> dict:
    return {
        "task_group_id": task_id,
        "task_type": "dbt",
        "depends_on": depends_on,
        "dbt_project_ref": "finance",
        "dbt_command": "build",
        "dbt_select": "tag:nightly",
        "dbt_vars": vars_map,
    }


def test_dbt_task_references_extracted_from_dbt_vars():
    tasks = [
        _binding("bind_1", "1", []),
        _dbt("dbt_build", ["bind_1"], {"test1": "{{ dag.test1 }}"}),
    ]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.input_sources["dbt_build"]["test1"] == "bind_1"
    assert plan.binding_sources_for("dbt_build") == {"test1": "bind_1"}


def test_file_paths_are_scanned_for_param_references():
    """F1.5 REGRESYON: dosya yollari da `{{ p }}` tasir.

    Canli bulgu (2026-08-20): `target_file_path` tarama listesinde olmadigi
    icin binding kaynaklari SESSIZCE bos kaliyordu; hata calisma aninda
    "target_file_path sablonunda cozulemeyen deger" olarak patliyordu.
    `_reference_expression`in kendi sozlesmesi tam bunu uyariyor (INV-6).
    """
    tasks = [
        _binding("bind_1", "SBT", []),
        {
            "task_group_id": "aktar_1",
            "task_type": "source_target",
            "depends_on": ["bind_1"],
            "target_file_path": "/out/rapor_{{ test1 }}.csv",
        },
    ]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.binding_sources_for("aktar_1") == {"test1": "bind_1"}


def test_source_file_path_is_scanned_for_param_references():
    tasks = [
        _binding("bind_1", "SBT", []),
        {
            "task_group_id": "oku_1",
            "task_type": "source_target",
            "depends_on": ["bind_1"],
            "source_type": "file",
            "file_path": "/in/veri_{{ test1 }}.csv",
        },
    ]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.binding_sources_for("oku_1") == {"test1": "bind_1"}


def test_dbt_task_without_vars_references_nothing():
    tasks = [_binding("bind_1", "1", []), _dbt("dbt_build", ["bind_1"], {})]

    plan = compile_dag_parameter_flow(_params(), tasks)

    assert plan.binding_sources_for("dbt_build") == {}


def test_dbt_task_undeclared_param_in_vars_fails_loud():
    tasks = [_dbt("dbt_build", [], {"missing": "{{ dag.missing }}"})]

    with pytest.raises(ValueError, match="not declared"):
        compile_dag_parameter_flow(_params(), tasks)


def test_dbt_task_ambiguous_parameter_source_fails_loud():
    tasks = [
        _binding("bind_a", "1", []),
        _binding("bind_b", "2", []),
        _dbt("dbt_build", ["bind_a", "bind_b"], {"test1": "{{ dag.test1 }}"}),
    ]

    with pytest.raises(ValueError, match="Ambiguous"):
        compile_dag_parameter_flow(_params(), tasks)
