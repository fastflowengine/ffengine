"""F3.2 - dbt task config contract (shape validation + vars expression text).

The runner lives in Enterprise; Community owns only this contract. Shape
rules (approved F3.2 plan): dbt_project_ref/dbt_command/dbt_select required;
command whitelist {run, build, test}; project ref RELATIVE (containment is
enforced at runtime by the Enterprise runner); select rejects only empty and
control characters (argv-list execution leaves no shell surface); threads
positive int; vars flat with identifier keys and JSON-scalar or full-token
``{{ dag.param }}`` values only.
"""

from __future__ import annotations

import pytest

from ffengine.config.dbt_contract import (
    VALID_DBT_COMMANDS,
    dbt_vars_expression_text,
    validate_dbt_task_fields,
)


def _task(**overrides) -> dict:
    base = {
        "task_type": "dbt",
        "task_group_id": "build_finance_models",
        "dbt_project_ref": "finance",
        "dbt_command": "build",
        "dbt_select": "tag:nightly",
    }
    base.update(overrides)
    return base


# --- required fields / command whitelist ------------------------------------

def test_valid_minimal_task_normalizes():
    normalized = validate_dbt_task_fields(_task())
    assert normalized["dbt_project_ref"] == "finance"
    assert normalized["dbt_command"] == "build"
    assert normalized["dbt_select"] == "tag:nightly"
    assert "dbt_target" not in normalized
    assert "dbt_threads" not in normalized
    assert "dbt_vars" not in normalized


@pytest.mark.parametrize(
    "missing", ["dbt_project_ref", "dbt_command", "dbt_select"]
)
def test_required_fields_fail_loud(missing):
    task = _task()
    task.pop(missing)
    with pytest.raises(ValueError, match=missing):
        validate_dbt_task_fields(task)


def test_valid_commands_frozen():
    assert VALID_DBT_COMMANDS == frozenset({"run", "build", "test"})


def test_invalid_command_rejected():
    with pytest.raises(ValueError, match="dbt_command"):
        validate_dbt_task_fields(_task(dbt_command="snapshot"))


# --- project ref: relative-only shape ---------------------------------------

@pytest.mark.parametrize(
    "bad_ref",
    ["/opt/dbt/finance", "C:\\dbt\\finance", "c:/dbt", "../finance",
     "finance/../../etc", "finance\\..\\up", "", "   "],
)
def test_project_ref_must_be_relative_and_traversal_free(bad_ref):
    with pytest.raises(ValueError, match="dbt_project_ref"):
        validate_dbt_task_fields(_task(dbt_project_ref=bad_ref))


def test_project_ref_nested_relative_accepted():
    normalized = validate_dbt_task_fields(
        _task(dbt_project_ref="finance/marts")
    )
    assert normalized["dbt_project_ref"] == "finance/marts"


# --- select: empty/control chars only ---------------------------------------

@pytest.mark.parametrize(
    "selector",
    ["tag:nightly", "staging.orders+", "@source_a", "m1 m2,path/x*",
     "config.materialized:table"],
)
def test_legitimate_dbt_selectors_accepted(selector):
    normalized = validate_dbt_task_fields(_task(dbt_select=selector))
    assert normalized["dbt_select"] == selector


@pytest.mark.parametrize("bad", ["", "  ", "a\nb", "a\tb", "a\x00b", "a\x1bb"])
def test_select_empty_or_control_chars_rejected(bad):
    with pytest.raises(ValueError, match="dbt_select"):
        validate_dbt_task_fields(_task(dbt_select=bad))


# --- target / threads --------------------------------------------------------

def test_target_optional_and_normalized():
    normalized = validate_dbt_task_fields(_task(dbt_target=" prod "))
    assert normalized["dbt_target"] == "prod"


def test_target_control_chars_rejected():
    with pytest.raises(ValueError, match="dbt_target"):
        validate_dbt_task_fields(_task(dbt_target="pr\nod"))


def test_threads_positive_int_accepted():
    assert validate_dbt_task_fields(_task(dbt_threads=4))["dbt_threads"] == 4


@pytest.mark.parametrize("bad", [0, -1, "2", 2.5, True, False])
def test_threads_non_positive_or_non_int_rejected(bad):
    with pytest.raises(ValueError, match="dbt_threads"):
        validate_dbt_task_fields(_task(dbt_threads=bad))


# --- vars: flat, identifier keys, scalar or full {{ dag.X }} tokens ----------

def test_vars_scalars_and_dag_tokens_accepted():
    normalized = validate_dbt_task_fields(
        _task(
            dbt_vars={
                "run_date": "{{ dag.run_date }}",
                "full_refresh": False,
                "batch_size": 500,
                "ratio": 0.25,
                "label": "nightly",
            }
        )
    )
    assert normalized["dbt_vars"]["run_date"] == "{{ dag.run_date }}"
    assert normalized["dbt_vars"]["full_refresh"] is False


@pytest.mark.parametrize("bad_key", ["1bad", "a-b", "a b", "", "dag.x"])
def test_vars_non_identifier_keys_rejected(bad_key):
    with pytest.raises(ValueError, match="dbt_vars"):
        validate_dbt_task_fields(_task(dbt_vars={bad_key: "x"}))


@pytest.mark.parametrize(
    "bad_value",
    [None, ["a"], {"nested": 1},
     "prefix {{ dag.run_date }}",      # mixed text template
     "{{ run_date }}",                 # simple namespace not allowed in v1
     "{{ airflow.some_key }}"],        # airflow namespace not allowed in v1
)
def test_vars_non_scalar_or_partial_templates_rejected(bad_value):
    with pytest.raises(ValueError, match="dbt_vars"):
        validate_dbt_task_fields(_task(dbt_vars={"run_date": bad_value}))


def test_vars_must_be_flat_mapping():
    with pytest.raises(ValueError, match="dbt_vars"):
        validate_dbt_task_fields(_task(dbt_vars=["not", "a", "dict"]))


# --- incompatible fields ------------------------------------------------------

@pytest.mark.parametrize(
    "field,value",
    [
        ("script_sql", "SELECT 1"),
        ("dag_task_dag_id", "other_dag"),
        ("bindings", [{"variable_name": "x"}]),
        ("load_method", "upsert"),
        ("mapping_file", "m.yaml"),
        ("upsert_match_columns", ["id"]),
        ("target_table", "t"),
        ("partitioning", {"enabled": True}),
    ],
)
def test_incompatible_fields_rejected(field, value):
    with pytest.raises(ValueError, match=field):
        validate_dbt_task_fields(_task(**{field: value}))


def test_disabled_partitioning_placeholder_tolerated():
    normalized = validate_dbt_task_fields(
        _task(partitioning={"enabled": False})
    )
    assert normalized["dbt_command"] == "build"


# --- vars expression text (param-graph feed) ---------------------------------

def test_vars_expression_text_deterministic_sorted():
    task = _task(
        dbt_vars={
            "zeta": "{{ dag.zeta }}",
            "alpha": "{{ dag.alpha }}",
            "flag": True,
        }
    )
    text = dbt_vars_expression_text(task)
    assert text == "{{ dag.alpha }}\nTrue\n{{ dag.zeta }}"


def test_vars_expression_text_empty_without_vars():
    assert dbt_vars_expression_text(_task()) == ""
