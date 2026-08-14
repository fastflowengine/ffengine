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
    VALID_DBT_EXECUTION_MODES,
    VALID_DBT_TARGET_PLATFORMS,
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
    [["a"], {"nested": 1},
     "prefix {{ dag.run_date }}",      # mixed text template
     "{{ run_date }}",                 # simple namespace not allowed in v1
     "{{ airflow.some_key }}"],        # airflow namespace not allowed in v1
)
def test_vars_non_scalar_or_partial_templates_rejected(bad_value):
    with pytest.raises(ValueError, match="dbt_vars"):
        validate_dbt_task_fields(_task(dbt_vars={"run_date": bad_value}))


def test_vars_null_is_a_valid_json_scalar():
    normalized = validate_dbt_task_fields(_task(dbt_vars={"optional": None}))
    assert normalized["dbt_vars"] == {"optional": None}


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


# --- F3.2b (Cosmos, EX-D013/EX-D014) - execution / test behavior / assets ---
# Default execution is cosmos (FAD v27.0); the hardened DbtOperator remains
# the explicit `task` fallback. Unknown modes/values fail loud; the task
# fallback publishes no Assets, so emit_datasets=true and dbt_test_behavior
# are cosmos-mode-only (silent no-op is forbidden).

def test_execution_defaults_to_cosmos():
    assert validate_dbt_task_fields(_task())["dbt_execution"] == "cosmos"


def test_execution_task_fallback_accepted_and_stripped():
    normalized = validate_dbt_task_fields(_task(dbt_execution=" task "))
    assert normalized["dbt_execution"] == "task"


def test_execution_unknown_mode_fails_loud():
    with pytest.raises(ValueError, match="dbt_execution"):
        validate_dbt_task_fields(_task(dbt_execution="kubernetes"))


def test_valid_execution_modes_frozen():
    assert VALID_DBT_EXECUTION_MODES == frozenset({"cosmos", "task"})


def test_test_behavior_after_each_accepted_in_cosmos_mode():
    normalized = validate_dbt_task_fields(_task(dbt_test_behavior="after_each"))
    assert normalized["dbt_test_behavior"] == "after_each"


def test_test_behavior_unknown_value_fails_loud():
    with pytest.raises(ValueError, match="dbt_test_behavior"):
        validate_dbt_task_fields(_task(dbt_test_behavior="after_all"))


def test_test_behavior_rejected_in_task_mode():
    with pytest.raises(ValueError, match="dbt_test_behavior"):
        validate_dbt_task_fields(
            _task(dbt_execution="task", dbt_test_behavior="after_each")
        )


def test_test_behavior_omitted_stays_absent():
    assert "dbt_test_behavior" not in validate_dbt_task_fields(_task())


def test_emit_datasets_true_accepted_in_cosmos_mode():
    assert validate_dbt_task_fields(_task(emit_datasets=True))[
        "emit_datasets"
    ] is True


def test_emit_datasets_false_round_trips_explicitly():
    assert validate_dbt_task_fields(_task(emit_datasets=False))[
        "emit_datasets"
    ] is False


def test_emit_datasets_omitted_stays_absent():
    assert "emit_datasets" not in validate_dbt_task_fields(_task())


@pytest.mark.parametrize("bad", ["true", 1, [], {}])
def test_emit_datasets_requires_real_bool(bad):
    with pytest.raises(ValueError, match="emit_datasets"):
        validate_dbt_task_fields(_task(emit_datasets=bad))


def test_emit_datasets_true_rejected_in_task_mode():
    with pytest.raises(ValueError, match="emit_datasets"):
        validate_dbt_task_fields(_task(dbt_execution="task", emit_datasets=True))


def test_emit_datasets_false_allowed_in_task_mode():
    normalized = validate_dbt_task_fields(
        _task(dbt_execution="task", emit_datasets=False)
    )
    assert normalized["emit_datasets"] is False
    assert normalized["dbt_execution"] == "task"


# --- F6.4 — dbt_target_platform (adapter selection, EX-D035) ----------------
# The platform selector is a secret-free config field (EX-D030 precedent):
# the ProfileMapping CLASS choice is a parse-time decision, and parse-time
# Connection reads are forbidden (F3.2b), so runtime auto-detect is not an
# option. Default is postgres — every pre-F6.4 cosmos config keeps its exact
# behavior (T-F6.4-3). Databricks and Spark are DIFFERENT platforms with
# different adapters (INV-10); an unknown value fails loud.

def test_target_platform_defaults_to_postgres_in_cosmos_mode():
    normalized = validate_dbt_task_fields(_task())
    assert normalized["dbt_target_platform"] == "postgres"


@pytest.mark.parametrize("platform", ["postgres", "spark", "databricks"])
def test_target_platform_valid_values_accepted(platform):
    normalized = validate_dbt_task_fields(
        _task(dbt_target_platform=f" {platform} ")
    )
    assert normalized["dbt_target_platform"] == platform


def test_target_platform_unknown_value_fails_loud():
    with pytest.raises(ValueError, match="dbt_target_platform"):
        validate_dbt_task_fields(_task(dbt_target_platform="snowflake-spark"))


def test_target_platform_set_rejected_in_task_mode():
    """Task fallback profili deployment-owned profiles.yml'den alir; alanin
    orada sessizce yok sayilmasi INV-1 ihlali olurdu."""
    with pytest.raises(ValueError, match="dbt_target_platform"):
        validate_dbt_task_fields(
            _task(dbt_execution="task", dbt_target_platform="spark")
        )


def test_target_platform_absent_in_task_mode():
    normalized = validate_dbt_task_fields(_task(dbt_execution="task"))
    assert "dbt_target_platform" not in normalized


def test_valid_target_platforms_frozen():
    assert VALID_DBT_TARGET_PLATFORMS == frozenset(
        {"postgres", "spark", "databricks"}
    )


def test_target_platform_control_chars_fail_loud():
    with pytest.raises(ValueError, match="dbt_target_platform"):
        validate_dbt_task_fields(_task(dbt_target_platform="spa\nrk"))
