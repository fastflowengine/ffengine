"""
F3.2 - dbt task config contract (Community-owned shape validation).

The dbt runner is Enterprise-only; Community owns only the task *shape*:
which fields exist, their types, and the vars template rules. Path
containment, executable resolution and process handling are enforced by the
Enterprise runner at task runtime - never at DAG parse (no parse-time I/O).

Rules (approved F3.2 plan):
- ``dbt_project_ref``, ``dbt_command``, ``dbt_select`` are required.
- ``dbt_command`` is whitelisted to {run, build, test}.
- ``dbt_project_ref`` is a RELATIVE path under ``FFENGINE_DBT_PROJECT_ROOT``;
  absolute paths and ``..`` traversal are rejected here, symlink escape at
  runtime.
- ``dbt_select`` rejects only empty values and control characters: dbt is
  invoked with an argv list (never a shell), so there is no shell-injection
  surface that would justify a restrictive selector charset.
- ``dbt_vars`` is a flat mapping; keys are identifiers; values are JSON
  scalars or a full ``{{ dag.<param> }}`` token. Mixed text templates, other
  namespaces, and nested values are rejected in v1 (fail-loud).

F3.2b (Cosmos, EX-D013/EX-D014):
- ``dbt_execution`` selects the orchestration path: ``cosmos`` (model-level
  rendering, the FAD v27.0 default) or ``task`` (the hardened black-box
  DbtOperator fallback). Unknown modes fail loud; there is no silent mode
  degradation in either direction.
- ``dbt_test_behavior`` is cosmos-mode-only; v1 supports ``after_each``.
- ``emit_datasets`` (bool, default false) is cosmos-mode-only when true:
  the task fallback publishes no Airflow Assets, so ``true`` + ``task``
  is rejected instead of silently not emitting.
"""

from __future__ import annotations

import re
from typing import Any

VALID_DBT_COMMANDS = frozenset({"run", "build", "test"})
VALID_DBT_EXECUTION_MODES = frozenset({"cosmos", "task"})
DEFAULT_DBT_EXECUTION = "cosmos"
VALID_DBT_TEST_BEHAVIORS = frozenset({"after_each"})

DBT_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DBT_DAG_TOKEN_RE = re.compile(r"^\{\{\s*dag\.[A-Za-z_][A-Za-z0-9_]*\s*\}\}$")

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:")

# Fields that belong to other task types (or to the engine data plane) and
# must never appear on a dbt task. ``partitioning`` is special-cased: the
# disabled placeholder emitted by legacy payloads is tolerated.
_INCOMPATIBLE_FIELDS = (
    "script_sql",
    "script_run_environment",
    "dag_task_dag_id",
    "bindings",
    "load_method",
    "mapping_file",
    "upsert_match_columns",
    "source_schema",
    "source_table",
    "target_schema",
    "target_table",
    "where",
)


def _has_control_chars(text: str) -> bool:
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in text)


def _clean_str(
    task: dict, field: str, *, error_cls, required: bool
) -> str | None:
    raw = task.get(field)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        if required:
            raise error_cls(f"{field} is required when task_type='dbt'.")
        return None
    if not isinstance(raw, str):
        raise error_cls(f"{field} must be a string.")
    value = raw.strip()
    if _has_control_chars(value):
        raise error_cls(
            f"{field} must not contain control characters (fail-loud)."
        )
    return value


def _validate_project_ref(value: str, *, error_cls) -> str:
    if value.startswith(("/", "\\")) or _WINDOWS_DRIVE_RE.match(value):
        raise error_cls(
            "dbt_project_ref must be a relative path under "
            "FFENGINE_DBT_PROJECT_ROOT; absolute paths are rejected "
            "(fail-loud)."
        )
    if any(part == ".." for part in re.split(r"[\\/]+", value)):
        raise error_cls(
            "dbt_project_ref must not contain '..' path segments "
            "(fail-loud)."
        )
    return value


def _validate_threads(task: dict, *, error_cls) -> int | None:
    raw = task.get("dbt_threads")
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise error_cls("dbt_threads must be a positive integer.")
    return raw


def _validate_var_value(key: str, value: Any, *, error_cls) -> Any:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        if "{{" in value or "}}" in value:
            token = value.strip()
            if not DBT_DAG_TOKEN_RE.match(token):
                raise error_cls(
                    f"dbt_vars value for {key!r} must be a JSON scalar or a "
                    "full '{{ dag.<param> }}' token; mixed text templates "
                    "and other namespaces are rejected (fail-loud)."
                )
            return token
        if _has_control_chars(value):
            raise error_cls(
                f"dbt_vars value for {key!r} must not contain control "
                "characters (fail-loud)."
            )
        return value
    raise error_cls(
        f"dbt_vars value for {key!r} must be a JSON scalar or a full "
        "'{{ dag.<param> }}' token; list and object values are rejected "
        "(fail-loud)."
    )


def _validate_vars(task: dict, *, error_cls) -> dict | None:
    raw = task.get("dbt_vars")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise error_cls("dbt_vars must be a flat mapping of scalar values.")
    validated: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not DBT_VAR_NAME_RE.match(key):
            raise error_cls(
                f"dbt_vars key {key!r} must be an identifier "
                "([A-Za-z_][A-Za-z0-9_]*)."
            )
        validated[key] = _validate_var_value(key, value, error_cls=error_cls)
    return validated


def _reject_incompatible_fields(task: dict, *, error_cls) -> None:
    for field in _INCOMPATIBLE_FIELDS:
        if task.get(field):
            raise error_cls(
                f"{field} is not allowed on a dbt task; a dbt task is a "
                "black-box handoff and carries no engine fields (fail-loud)."
            )
    partitioning = task.get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("enabled"):
        raise error_cls(
            "partitioning is not allowed on a dbt task (fail-loud)."
        )


def validate_dbt_task_fields(task: dict, *, error_cls=ValueError) -> dict:
    """Validate and normalize the dbt_* fields of a task dict (fail-loud).

    Returns the narrow normalized dict: required keys always present,
    optional keys (target/threads/vars) only when explicitly set.
    """
    if not isinstance(task, dict):
        raise error_cls("dbt task definition must be a mapping.")
    _reject_incompatible_fields(task, error_cls=error_cls)

    project_ref = _validate_project_ref(
        _clean_str(task, "dbt_project_ref", error_cls=error_cls, required=True),
        error_cls=error_cls,
    )
    command = _clean_str(task, "dbt_command", error_cls=error_cls, required=True)
    if command not in VALID_DBT_COMMANDS:
        raise error_cls(
            f"Unsupported dbt_command: '{command}'. Valid values: "
            f"{sorted(VALID_DBT_COMMANDS)}. There is no silent fallback "
            "(fail-loud)."
        )
    select = _clean_str(task, "dbt_select", error_cls=error_cls, required=True)

    execution = _clean_str(
        task, "dbt_execution", error_cls=error_cls, required=False
    )
    if execution is None:
        execution = DEFAULT_DBT_EXECUTION
    elif execution not in VALID_DBT_EXECUTION_MODES:
        raise error_cls(
            f"Unsupported dbt_execution: '{execution}'. Valid values: "
            f"{sorted(VALID_DBT_EXECUTION_MODES)}. There is no silent "
            "fallback between modes (fail-loud)."
        )

    normalized: dict[str, Any] = {
        "dbt_project_ref": project_ref,
        "dbt_command": command,
        "dbt_select": select,
        "dbt_execution": execution,
    }

    behavior = _clean_str(
        task, "dbt_test_behavior", error_cls=error_cls, required=False
    )
    if behavior is not None:
        if behavior not in VALID_DBT_TEST_BEHAVIORS:
            raise error_cls(
                f"Unsupported dbt_test_behavior: '{behavior}'. Valid values: "
                f"{sorted(VALID_DBT_TEST_BEHAVIORS)} (fail-loud)."
            )
        if execution != "cosmos":
            raise error_cls(
                "dbt_test_behavior requires dbt_execution='cosmos'; the "
                "task fallback runs one black-box dbt command and cannot "
                "split tests (fail-loud)."
            )
        normalized["dbt_test_behavior"] = behavior

    raw_emit = task.get("emit_datasets")
    if raw_emit is not None:
        if not isinstance(raw_emit, bool):
            raise error_cls("emit_datasets must be a boolean (fail-loud).")
        if raw_emit and execution != "cosmos":
            raise error_cls(
                "emit_datasets=true requires dbt_execution='cosmos'; the "
                "task fallback publishes no Airflow Assets (fail-loud)."
            )
        normalized["emit_datasets"] = raw_emit
    target = _clean_str(task, "dbt_target", error_cls=error_cls, required=False)
    if target is not None:
        normalized["dbt_target"] = target
    threads = _validate_threads(task, error_cls=error_cls)
    if threads is not None:
        normalized["dbt_threads"] = threads
    vars_map = _validate_vars(task, error_cls=error_cls)
    if vars_map is not None:
        normalized["dbt_vars"] = vars_map
    return normalized


def dbt_vars_expression_text(task: dict) -> str:
    """Deterministic text of dbt_vars values for param-reference extraction.

    Joined sorted-by-key so the DAG parameter compiler (and its API/UI
    mirrors) see a stable expression source containing every
    ``{{ dag.<param> }}`` token the task references.
    """
    raw = task.get("dbt_vars")
    if not isinstance(raw, dict) or not raw:
        return ""
    return "\n".join(str(raw[key]) for key in sorted(raw))
