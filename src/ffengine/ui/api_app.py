"""Airflow 3 FastAPI app for Flow Studio."""

from __future__ import annotations

import os
import math
import re
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

from ffengine.airflow.task_type_registry import has_task_type_provider
from ffengine.config.dag_param_flow import (
    compile_dag_parameter_flow,
    validate_binding_target_is_custom,
)
from ffengine.config.dbt_contract import (
    dbt_vars_expression_text,
    validate_dbt_task_fields,
)
from ffengine.config.schema import (
    VALID_COLUMN_MAPPING_MODES,
    VALID_LOAD_METHODS,
    VALID_PARTITION_MODES,
    VALID_SOURCE_TYPES,
    FILE_SOURCE_TYPES,
    VALID_JSON_MODES,
    VALID_TARGET_TYPES,
)
from ffengine.errors import http_status_for, normalize_exception
from ffengine.ui.studio_service import (
    STUDIO_DAG_MARKER,
    create_or_update_dag,
    delete_dag_bundle,
    discover_dag_explorer_items,
    search_dag_explorer_items,
    discover_dag_dependency_options,
    discover_timezones,
    get_airflow_default_timezone_name,
    discover_connections,
    discover_columns,
    discover_hierarchy_options,
    discover_airflow_variables,
    discover_schemas,
    discover_tables,
    fetch_timeline_runs,
    generate_mapping_preview,
    parse_mapping_columns,
    get_dag_revisions,
    list_dbt_asset_options,
    normalize_notifications,
    normalize_scheduler,
    promote_dag_revision,
    resolve_dag_config_for_update,
)
from ffengine.airflow.notification_template import (
    DEFAULT_TEMPLATE_NAME,
    PLACEHOLDERS,
    delete_template,
    load_templates,
    render_template,
    sample_meta,
    save_template,
)

_FOLDER_PATH_REQUIRED_MESSAGE = "Select a project and DAG path."


def _validate_folder_path_segment(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(_FOLDER_PATH_REQUIRED_MESSAGE)
    return normalized


def _raise_http_from_exception(exc: Exception) -> None:
    """C10: domain exception normalize edip tutarlÄ± HTTP yanÄ±tÄ± Ã¼retir."""
    if isinstance(exc, HTTPException):
        raise exc
    norm = normalize_exception(exc)
    raise HTTPException(status_code=http_status_for(norm), detail=norm.message) from exc


def _optional_api_key_dep(
    x_flow_studio_api_key: str | None = Header(
        None,
        alias="X-Flow-Studio-API-Key",
        description="Flow Studio API key (required if FLOW_STUDIO_API_KEY environment variable is set)",
    ),
) -> None:
    """T12: Optional API key validation for mutation endpoints."""
    expected = os.getenv("FLOW_STUDIO_API_KEY", "").strip()
    if not expected:
        return
    if not x_flow_studio_api_key or x_flow_studio_api_key.strip() != expected:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing X-Flow-Studio-API-Key header.",
        )


_BINDING_VAR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BINDING_PARAM_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_DAG_PARAM_RE = re.compile(r"\{\{\s*dag\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_AIRFLOW_PARAM_RE = re.compile(r"\{\{\s*airflow\.([^\s{}]+)\s*\}\}")
_OBSOLETE_BINDING_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_CUSTOM_TAG_MAX_COUNT = 10
_VALID_TASK_TYPES = {"source_target", "script_run", "dag", "binding", "dbt"}
_VALID_SCRIPT_RUN_ENVIRONMENTS = {"source", "target"}
_VALID_DAG_PARAM_TYPES = {"string", "integer", "number", "boolean"}


def _extract_binding_params(expression: str | None) -> set[str]:
    text = str(expression or "")
    obsolete = _OBSOLETE_BINDING_PARAM_RE.search(text)
    if obsolete:
        name = obsolete.group(1)
        raise ValueError(
            f"Obsolete parameter syntax; replace :{name} with {{{{ {name} }}}}."
        )
    return set(_BINDING_PARAM_RE.findall(text))


def _extract_dag_params(expression: str | None) -> set[str]:
    return set(_DAG_PARAM_RE.findall(str(expression or "")))


def _extract_airflow_variable_keys(expression: str | None) -> set[str]:
    return set(_AIRFLOW_PARAM_RE.findall(str(expression or "")))


def _normalize_upsert_match_columns(raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("upsert_match_columns must be a list.")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        col = str(item or "").strip()
        if not col or col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _validate_bindings_expression_contract(
    expression: str | None,
    bindings: list["BindingPayload"] | None,
    *,
    expression_label: str,
) -> None:
    expression_params = _extract_binding_params(expression)
    items = list(bindings or [])
    if not items:
        return
    binding_names = {item.variable_name for item in items}
    unused = sorted(binding_names - expression_params)
    if unused:
        raise ValueError(
            f"Binding definition exists but parameter(s) are unused in {expression_label}: "
            + ", ".join(unused)
        )


def _dbt_vars_text(dbt_vars: dict | None) -> str:
    return dbt_vars_expression_text({"dbt_vars": dict(dbt_vars or {})})


def _validate_dbt_task_payload(payload: Any) -> None:
    """F3.2 — dbt task payload rules (shared by both payload models).

    The dbt runner is Enterprise-only: without a registered task-type
    provider the request is rejected (HTTP 422), which is the backend half
    of the edition gate (the UI half hides the option).
    """
    from ffengine.core.edition import is_enterprise_enabled

    if not is_enterprise_enabled() or not has_task_type_provider("dbt"):
        raise ValueError(
            "task_type='dbt' requires Enterprise edition and the dbt "
            "provider; both gates must be enabled."
        )
    conflicts = {
        "script_run_environment": payload.script_run_environment,
        "script_sql": payload.script_sql,
        "dag_task_dag_id": payload.dag_task_dag_id,
        "inline_sql": payload.inline_sql,
        "mapping_file": payload.mapping_file,
        "mapping_content": payload.mapping_content,
        "where": payload.where,
    }
    for field_name, value in conflicts.items():
        if str(value or "").strip():
            raise ValueError(
                f"{field_name} is not allowed when task_type='dbt'."
            )
    if payload.bindings:
        raise ValueError(
            "dbt tasks do not support local bindings; assign DAG parameters "
            "with an upstream 'binding' task."
        )
    if payload.partitioning_enabled:
        raise ValueError("partitioning is not allowed when task_type='dbt'.")
    if payload.use_bulk_api:
        raise ValueError("use_bulk_api is not allowed when task_type='dbt'.")
    validate_dbt_task_fields(
        {
            "dbt_project_ref": payload.dbt_project_ref,
            "dbt_command": payload.dbt_command,
            "dbt_select": payload.dbt_select,
            "dbt_target": payload.dbt_target,
            "dbt_threads": payload.dbt_threads,
            "dbt_vars": payload.dbt_vars,
            "dbt_execution": payload.dbt_execution,
            "dbt_test_behavior": payload.dbt_test_behavior,
            "emit_datasets": payload.emit_datasets,
        }
    )


def _dag_param_value_matches(value: Any, param_type: str) -> bool:
    if value is None:
        return True
    if param_type == "string":
        return isinstance(value, str)
    if param_type == "boolean":
        return isinstance(value, bool)
    if param_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _binding_default_matches(value: str | None, param_type: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if param_type == "string":
        return True
    if param_type == "integer":
        return bool(re.fullmatch(r"-?\d+", text))
    if param_type == "boolean":
        return text.lower() in {"true", "false"}
    try:
        number = float(text)
    except ValueError:
        return False
    return math.isfinite(number)


class DagParamPayload(BaseModel):
    model_config = {"extra": "forbid"}

    name: str = Field(..., min_length=1)
    type: str = "string"
    default: Any = None
    required: bool = False
    description: str | None = None
    enum: list[Any] | None = None

    @field_validator("name")
    @classmethod
    def _v_name(cls, value: str) -> str:
        name = str(value or "").strip()
        if not _BINDING_VAR_RE.fullmatch(name):
            raise ValueError("Invalid DAG parameter name format.")
        return name

    @field_validator("type")
    @classmethod
    def _v_type(cls, value: str) -> str:
        param_type = str(value or "").strip().lower()
        if param_type not in _VALID_DAG_PARAM_TYPES:
            raise ValueError("DAG parameter type must be string/integer/number/boolean.")
        return param_type

    @model_validator(mode="after")
    def _v_contract(self) -> "DagParamPayload":
        provided = set(self.model_fields_set)
        if self.name != "log_level" and provided & {"default", "required", "enum"}:
            raise ValueError(
                "custom DAG parameters do not support default or required; "
                "assign values with a Binding task."
            )
        if not _dag_param_value_matches(self.default, self.type):
            raise ValueError(f"DAG parameter '{self.name}' default does not match type.")
        values = list(self.enum or [])
        if values and any(not _dag_param_value_matches(v, self.type) for v in values):
            raise ValueError(f"DAG parameter '{self.name}' enum does not match type.")
        if values and self.default is not None and self.default not in values:
            raise ValueError(f"DAG parameter '{self.name}' default must be in enum.")
        if self.name == "log_level":
            if self.type != "string" or self.default not in {"default", "DEBUG"}:
                raise ValueError("log_level must be string with default default or DEBUG.")
        return self


class BindingPayload(BaseModel):
    model_config = {"extra": "forbid"}

    variable_name: str = Field(..., min_length=1)
    binding_source: str
    default_value: str | None = None
    sql: str | None = None
    airflow_variable_key: str | None = None

    @field_validator("variable_name")
    @classmethod
    def _v_variable_name(cls, v: str) -> str:
        name = str(v or "").strip()
        if not _BINDING_VAR_RE.fullmatch(name):
            raise ValueError("Invalid variable_name format.")
        return name

    @field_validator("binding_source")
    @classmethod
    def _v_binding_source(cls, v: str) -> str:
        allowed = {"source", "target", "default", "airflow_variable"}
        if v not in allowed:
            raise ValueError(f"Invalid binding_source: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_source_specific_fields(self) -> "BindingPayload":
        if self.binding_source == "default" and not (self.default_value or "").strip():
            raise ValueError("default_value is required when binding_source='default'.")
        if self.binding_source in {"source", "target"} and not (self.sql or "").strip():
            raise ValueError("sql is required when binding_source='source|target'.")
        if (
            self.binding_source == "airflow_variable"
            and not (self.airflow_variable_key or "").strip()
        ):
            raise ValueError(
                "airflow_variable_key is required when binding_source='airflow_variable'."
            )
        return self


class FlowTaskPayload(BaseModel):
    model_config = {"extra": "forbid"}

    task_type: str = "source_target"
    task_group_id: str | None = Field(default=None, min_length=1)
    source_schema: str | None = Field(default=None, min_length=1)
    source_table: str | None = Field(default=None, min_length=1)
    source_type: str = "table"
    inline_sql: str | None = None
    script_run_environment: str | None = None
    script_sql: str | None = None
    dag_task_dag_id: str | None = None
    target_schema: str | None = Field(default=None, min_length=1)
    target_table: str | None = Field(default=None, min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    upsert_match_columns: list[str] | None = None
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    mapping_content: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    # F2.1 — native bulk API (Enterprise). Off by default; method is explicit
    # (no "auto") and validated against the bulk-provider registry + edition.
    use_bulk_api: bool = False
    bulk_api_method: str | None = None
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto_numeric"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_distinct_limit: int | None = Field(default=None, ge=1, le=1_000_000)
    partitioning_ranges: list[Any] | None = None
    bindings: list[BindingPayload] | None = None
    depends_on: list[str] | None = None
    # F3.2 — dbt task (Enterprise-run; Community carries the contract only)
    dbt_project_ref: str | None = None
    dbt_command: str | None = None
    dbt_select: str | None = None
    dbt_target: str | None = None
    dbt_threads: int | None = None
    dbt_vars: dict[str, Any] | None = None
    # F3.2b (Cosmos) — execution mode + cosmos-only knobs
    dbt_execution: str | None = None
    dbt_test_behavior: str | None = None
    emit_datasets: bool | None = None
    # F1.4/F1.5 — file source (csv/json) + file target
    file_path: str | None = None
    delimiter: str | None = None
    encoding: str | None = None
    quotechar: str | None = None
    header: bool | None = None
    json_mode: str | None = None
    target_type: str = "db"
    target_file_path: str | None = None
    target_delimiter: str | None = None
    target_encoding: str | None = None
    target_header: bool | None = None

    @field_validator("depends_on")
    @classmethod
    def _v_depends_on(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in v:
            dep = str(raw or "").strip()
            if not dep or dep in seen:
                continue
            seen.add(dep)
            normalized.append(dep)
        return normalized or []

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql", "csv", "json"}:
            raise ValueError(
                "source_type must be one of: table, view, sql, csv, json."
            )
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

    @field_validator("target_type")
    @classmethod
    def _v_target_type(cls, v: str) -> str:
        if v not in VALID_TARGET_TYPES:
            raise ValueError(f"Invalid target_type: {v!r}. Use 'db' or 'file'.")
        return v

    @field_validator("task_type")
    @classmethod
    def _v_task_type(cls, v: str) -> str:
        value = str(v or "").strip()
        if value not in _VALID_TASK_TYPES:
            raise ValueError(
                "task_type must be source_target, script_run, dag, binding, "
                "or dbt."
            )
        return value

    @field_validator("load_method")
    @classmethod
    def _v_load_method(cls, v: str) -> str:
        if v not in VALID_LOAD_METHODS:
            raise ValueError(f"Invalid load_method: {v!r}")
        return v

    @field_validator("column_mapping_mode")
    @classmethod
    def _v_col_map(cls, v: str) -> str:
        if v not in VALID_COLUMN_MAPPING_MODES:
            raise ValueError(f"Invalid column_mapping_mode: {v!r}")
        return v

    @field_validator("partitioning_mode")
    @classmethod
    def _v_part_mode(cls, v: str) -> str:
        if v not in VALID_PARTITION_MODES:
            raise ValueError(f"Invalid partitioning.mode: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_mapping(self) -> FlowTaskPayload:
        normalized_upsert_match_columns = _normalize_upsert_match_columns(
            self.upsert_match_columns
        )
        self.upsert_match_columns = normalized_upsert_match_columns or None
        if self.task_type == "source_target":
            if self.target_type == "file":
                if not (self.target_file_path or "").strip():
                    raise ValueError(
                        "target_file_path is required when target_type='file'."
                    )
            elif (
                not (self.target_schema or "").strip()
                or not (self.target_table or "").strip()
            ):
                raise ValueError(
                    "target_schema and target_table are required when task_type='source_target'."
                )
            if self.source_type in FILE_SOURCE_TYPES:
                if not (self.file_path or "").strip():
                    raise ValueError(
                        "file_path is required when source_type=csv|json."
                    )
                if self.source_type == "json":
                    mode = str(self.json_mode or "flat").strip().lower()
                    if mode not in VALID_JSON_MODES:
                        raise ValueError(
                            "json_mode must be 'flat' (raw not supported — F1.4b)."
                        )
            if self.source_type == "sql" and self.column_mapping_mode != "mapping_file":
                raise ValueError(
                    "column_mapping_mode='mapping_file' is required when source_type='sql'."
                )
            if self.source_type in {"table", "view"}:
                if (
                    not (self.source_schema or "").strip()
                    or not (self.source_table or "").strip()
                ):
                    raise ValueError(
                        "source_schema and source_table are required when source_type=table|view."
                    )
            if self.source_type == "sql" and not (self.inline_sql or "").strip():
                raise ValueError("inline_sql is required when source_type='sql'.")
            if self.load_method == "upsert" and not normalized_upsert_match_columns:
                raise ValueError(
                    "upsert_match_columns is required when load_method='upsert'."
                )
        elif self.load_method == "upsert":
            raise ValueError("load_method='upsert' is only valid for source_target tasks.")
        elif normalized_upsert_match_columns:
            raise ValueError(
                "upsert_match_columns is only supported when task_type='source_target'."
            )
        elif self.task_type == "script_run":
            environment = str(self.script_run_environment or "").strip()
            if environment not in _VALID_SCRIPT_RUN_ENVIRONMENTS:
                raise ValueError(
                    "script_run_environment must be one of: 'source' or 'target'."
                )
            if not (self.script_sql or "").strip():
                raise ValueError("script_sql is required when task_type='script_run'.")
        elif self.task_type == "dag":
            dag_id = str(self.dag_task_dag_id or "").strip()
            if not dag_id:
                raise ValueError("dag_task_dag_id is required when task_type='dag'.")
        elif self.task_type == "dbt":
            _validate_dbt_task_payload(self)
        items = list(self.bindings or [])
        names = [item.variable_name for item in items]
        if len(names) != len(set(names)):
            raise ValueError("bindings.variable_name must be unique within a task.")
        task_group_id = str(self.task_group_id or "").strip()
        if task_group_id:
            for dep in list(self.depends_on or []):
                if dep == task_group_id:
                    raise ValueError("depends_on cannot include task_group_id itself.")
        if self.task_type == "binding":
            if not items:
                raise ValueError("bindings is required when task_type='binding'.")
            if self.where or self.script_sql or self.dag_task_dag_id:
                raise ValueError("binding tasks only support bindings and depends_on.")
            return self
        binding_expression = self.where
        binding_expression_label = "Where Clause"
        if self.task_type == "script_run":
            binding_expression = self.script_sql
            binding_expression_label = "Script SQL / Stored Procedure"
        elif self.task_type == "dbt":
            binding_expression = _dbt_vars_text(self.dbt_vars)
            binding_expression_label = "dbt vars"
        _validate_bindings_expression_contract(
            binding_expression,
            items,
            expression_label=binding_expression_label,
        )
        return self


class SchedulerPayload(BaseModel):
    model_config = {"extra": "forbid"}

    cron_expression: str | None = None
    timezone: str | None = None
    active: bool | None = None
    start_date: str | None = None
    # F3.2b (Cosmos) — additive trigger contract; asset mode is
    # Enterprise-gated inside normalize_scheduler (backend 422 half).
    trigger_type: str | None = None
    assets: list[str] | None = None

    @model_validator(mode="after")
    def _v_scheduler(self) -> "SchedulerPayload":
        normalized = normalize_scheduler(self.model_dump(exclude_none=True))
        self.cron_expression = normalized["cron_expression"]
        self.timezone = normalized["timezone"]
        self.active = normalized["active"]
        self.start_date = normalized["start_date"]
        self.trigger_type = normalized.get("trigger_type")
        self.assets = normalized.get("assets")
        return self


class NotificationsPayload(BaseModel):
    model_config = {"extra": "forbid"}

    notify_on: list[str] | None = None
    notify_emails: list[str] | None = None
    notify_conn_id: str | None = None
    notify_template: str | None = None
    notify_deadline_minutes: int | None = None

    @model_validator(mode="after")
    def _v_notifications(self) -> "NotificationsPayload":
        normalized = normalize_notifications(self.model_dump(exclude_none=True))
        if normalized is None:
            self.notify_on = None
            self.notify_emails = None
            self.notify_conn_id = None
            self.notify_template = None
            self.notify_deadline_minutes = None
        else:
            self.notify_on = normalized["notify_on"]
            self.notify_emails = normalized["notify_emails"]
            self.notify_conn_id = normalized["notify_conn_id"]
            self.notify_template = normalized.get("notify_template")
            self.notify_deadline_minutes = normalized.get("notify_deadline_minutes")
        return self


class MailTemplatePayload(BaseModel):
    model_config = {"extra": "forbid"}

    name: str
    subject: str
    html_body: str


class MailTemplateDeletePayload(BaseModel):
    model_config = {"extra": "forbid"}

    name: str


class MailTemplatePreviewPayload(BaseModel):
    model_config = {"extra": "forbid"}

    subject: str = ""
    html_body: str = ""
    kind: str = "failure"


class DagDependenciesPayload(BaseModel):
    model_config = {"extra": "forbid"}

    upstream_dag_ids: list[str] | None = None

    @field_validator("upstream_dag_ids")
    @classmethod
    def _v_upstream_dag_ids(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in v:
            dag_id = str(raw or "").strip()
            if not dag_id or dag_id in seen:
                continue
            seen.add(dag_id)
            normalized.append(dag_id)
        return normalized


def _validate_dag_params_and_binding_tasks(
    params: list[DagParamPayload],
    tasks: list[FlowTaskPayload],
    scheduler: SchedulerPayload | None,
) -> None:
    names = [item.name for item in params]
    if len(names) != len(set(names)):
        raise ValueError("dag_params.name must be unique.")
    if params and "log_level" not in names:
        raise ValueError("dag_params must include the built-in log_level parameter.")
    declared = set(names)
    declarations = {item.name: item for item in params}
    for task in tasks:
        if task.task_type == "binding":
            items = list(task.bindings or [])
            invalid_sources = sorted(
                {
                    item.binding_source
                    for item in items
                    if item.binding_source not in {"source", "target", "default"}
                }
            )
            if invalid_sources:
                raise ValueError(
                    "Binding tasks support source, target, or default binding sources."
                )
            local = {item.variable_name for item in items}
            for name in local:
                validate_binding_target_is_custom(name)
            undeclared = sorted(local - declared)
            if undeclared:
                raise ValueError(
                    "Binding target must be a declared DAG parameter: "
                    + ", ".join(undeclared)
                )
            invalid_defaults = sorted(
                item.variable_name
                for item in items
                if item.binding_source == "default"
                and not _binding_default_matches(
                    item.default_value,
                    declarations[item.variable_name].type,
                )
            )
            if invalid_defaults:
                raise ValueError(
                    "Binding default does not match DAG parameter type: "
                    + ", ".join(invalid_defaults)
                )
            continue

    for task in tasks:
        if task.task_type == "binding":
            continue
        local = {item.variable_name for item in list(task.bindings or [])}
        if task.task_type == "script_run":
            expression = task.script_sql
        elif task.task_type == "dbt":
            # Mirror of dag_param_flow._reference_expression (F3.2).
            expression = _dbt_vars_text(task.dbt_vars)
        else:
            expression = task.where
        simple_refs = _extract_binding_params(expression)
        legacy_dag_refs = (simple_refs - local) & declared
        missing = sorted(simple_refs - local - declared)
        if missing:
            raise ValueError(
                "Expression parameter must have a local binding or declared DAG parameter: "
                + ", ".join(missing)
            )
        dag_refs = _extract_dag_params(expression) | legacy_dag_refs
        undeclared = sorted(dag_refs - declared)
        if undeclared:
            raise ValueError(
                "DAG parameter reference is not declared: " + ", ".join(undeclared)
            )
        # Parse keys here so malformed namespace use cannot silently pass through.
        _extract_airflow_variable_keys(expression)

    task_defs = [item.model_dump(exclude_none=True) for item in tasks]
    if all(str(item.get("task_group_id") or "").strip() for item in task_defs):
        compile_dag_parameter_flow(
            [item.model_dump(exclude_none=True) for item in params],
            task_defs,
        )


def _validate_single_task_params(
    expression: str | None,
    bindings: list[BindingPayload],
    params: list[DagParamPayload],
) -> None:
    local = {item.variable_name for item in bindings}
    declared = {item.name for item in params}
    missing = sorted(_extract_binding_params(expression) - local - declared)
    if missing:
        raise ValueError(
            "Expression parameter must have a local binding or declared DAG parameter: "
            + ", ".join(missing)
        )


class DagUpsertPayload(BaseModel):
    model_config = {"extra": "forbid"}

    project: str = Field(..., min_length=1)
    domain: str = Field(..., min_length=1)
    level: str = Field(..., min_length=1)
    flow: str = Field(..., min_length=1)
    source_conn_id: str = Field(..., min_length=1)
    target_conn_id: str = Field(..., min_length=1)
    source_schema: str | None = Field(default=None, min_length=1)
    source_table: str | None = Field(default=None, min_length=1)
    source_type: str = "table"
    inline_sql: str | None = None
    task_type: str = "source_target"
    script_run_environment: str | None = None
    script_sql: str | None = None
    dag_task_dag_id: str | None = None
    target_schema: str | None = Field(default=None, min_length=1)
    target_table: str | None = Field(default=None, min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    upsert_match_columns: list[str] | None = None
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    mapping_content: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    # F2.1 — native bulk API (Enterprise). Off by default; method is explicit
    # (no "auto") and validated against the bulk-provider registry + edition.
    use_bulk_api: bool = False
    bulk_api_method: str | None = None
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto_numeric"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_distinct_limit: int | None = Field(default=None, ge=1, le=1_000_000)
    partitioning_ranges: list[Any] | None = None
    bindings: list[BindingPayload] | None = None
    # F3.2 — dbt task (single-task DAG path)
    dbt_project_ref: str | None = None
    dbt_command: str | None = None
    dbt_select: str | None = None
    dbt_target: str | None = None
    dbt_threads: int | None = None
    dbt_vars: dict[str, Any] | None = None
    # F3.2b (Cosmos) — execution mode + cosmos-only knobs
    dbt_execution: str | None = None
    dbt_test_behavior: str | None = None
    emit_datasets: bool | None = None
    # F1.4/F1.5 — file source (csv/json) + file target (single-task DAG path)
    file_path: str | None = None
    delimiter: str | None = None
    encoding: str | None = None
    quotechar: str | None = None
    header: bool | None = None
    json_mode: str | None = None
    target_type: str = "db"
    target_file_path: str | None = None
    target_delimiter: str | None = None
    target_encoding: str | None = None
    target_header: bool | None = None
    task_group_id: str | None = Field(default=None, min_length=1)
    flow_tasks: list[FlowTaskPayload] | None = None
    custom_tags: list[str] | None = None
    scheduler: SchedulerPayload | None = None
    dag_dependencies: DagDependenciesPayload | None = None
    dag_params: list[DagParamPayload] | None = None
    notifications: NotificationsPayload | None = None

    @field_validator("project", "domain", "level", "flow", mode="before")
    @classmethod
    def _v_folder_path_segment(cls, v: Any) -> str:
        return _validate_folder_path_segment(v)

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql", "csv", "json"}:
            raise ValueError(
                "source_type must be one of: table, view, sql, csv, json."
            )
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

    @field_validator("target_type")
    @classmethod
    def _v_target_type(cls, v: str) -> str:
        if v not in VALID_TARGET_TYPES:
            raise ValueError(f"Invalid target_type: {v!r}. Use 'db' or 'file'.")
        return v

    @field_validator("task_type")
    @classmethod
    def _v_task_type(cls, v: str) -> str:
        value = str(v or "").strip()
        if value not in _VALID_TASK_TYPES:
            raise ValueError(
                "task_type must be source_target, script_run, dag, binding, "
                "or dbt."
            )
        return value

    @field_validator("load_method")
    @classmethod
    def _v_load_method(cls, v: str) -> str:
        if v not in VALID_LOAD_METHODS:
            raise ValueError(f"Invalid load_method: {v!r}")
        return v

    @field_validator("column_mapping_mode")
    @classmethod
    def _v_col_map(cls, v: str) -> str:
        if v not in VALID_COLUMN_MAPPING_MODES:
            raise ValueError(f"Invalid column_mapping_mode: {v!r}")
        return v

    @field_validator("partitioning_mode")
    @classmethod
    def _v_part_mode(cls, v: str) -> str:
        if v not in VALID_PARTITION_MODES:
            raise ValueError(f"Invalid partitioning.mode: {v!r}")
        return v

    @field_validator("custom_tags")
    @classmethod
    def _v_custom_tags(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        if len(v) > _CUSTOM_TAG_MAX_COUNT:
            raise ValueError(
                f"custom_tags can contain at most {_CUSTOM_TAG_MAX_COUNT} items."
            )
        return v

    @model_validator(mode="after")
    def _v_mapping(self) -> DagUpsertPayload:
        has_task_list = isinstance(self.flow_tasks, list) and len(self.flow_tasks) > 0
        if has_task_list:
            _validate_dag_params_and_binding_tasks(
                list(self.dag_params or []),
                list(self.flow_tasks or []),
                self.scheduler,
            )
            return self
        normalized_upsert_match_columns = _normalize_upsert_match_columns(
            self.upsert_match_columns
        )
        self.upsert_match_columns = normalized_upsert_match_columns or None
        parsed_task_type = str(self.task_type or "").strip()
        if parsed_task_type == "source_target":
            if self.load_method == "upsert" and not normalized_upsert_match_columns:
                raise ValueError(
                    "upsert_match_columns is required when load_method='upsert'."
                )
        else:
            if self.load_method == "upsert":
                raise ValueError(
                    "load_method='upsert' is only valid for source_target tasks."
                )
            if normalized_upsert_match_columns:
                raise ValueError(
                    "upsert_match_columns is only supported when task_type='source_target'."
                )

        if parsed_task_type == "script_run":
            if (
                str(self.script_run_environment or "").strip()
                not in _VALID_SCRIPT_RUN_ENVIRONMENTS
            ):
                raise ValueError(
                    "script_run_environment must be one of: 'source' or 'target'."
                )
            if not (self.script_sql or "").strip():
                raise ValueError("script_sql is required when task_type='script_run'.")
            items = list(self.bindings or [])
            names = [item.variable_name for item in items]
            if len(names) != len(set(names)):
                raise ValueError("bindings.variable_name must be unique within a task.")
            _validate_bindings_expression_contract(
                self.script_sql,
                items,
                expression_label="Script SQL / Stored Procedure",
            )
            _validate_single_task_params(
                self.script_sql, items, list(self.dag_params or [])
            )
            return self

        if parsed_task_type == "dag":
            if not (self.dag_task_dag_id or "").strip():
                raise ValueError("dag_task_dag_id is required when task_type='dag'.")
            items = list(self.bindings or [])
            names = [item.variable_name for item in items]
            if len(names) != len(set(names)):
                raise ValueError("bindings.variable_name must be unique within a task.")
            _validate_bindings_expression_contract(
                self.where,
                items,
                expression_label="Where Clause",
            )
            _validate_single_task_params(
                self.where, items, list(self.dag_params or [])
            )
            return self

        if parsed_task_type == "dbt":
            _validate_dbt_task_payload(self)
            vars_text = _dbt_vars_text(self.dbt_vars)
            _validate_single_task_params(
                vars_text, [], list(self.dag_params or [])
            )
            declared = {item.name for item in list(self.dag_params or [])}
            undeclared = sorted(_extract_dag_params(vars_text) - declared)
            if undeclared:
                raise ValueError(
                    "DAG parameter reference is not declared: "
                    + ", ".join(undeclared)
                )
            return self

        if self.target_type == "file" and not (self.target_file_path or "").strip():
            raise ValueError("target_file_path is required when target_type='file'.")
        target_required_ok = (self.target_type == "file") or all(
            [(self.target_schema or "").strip(), (self.target_table or "").strip()]
        )
        if self.source_type in {"table", "view"}:
            source_required_ok = all(
                [(self.source_schema or "").strip(), (self.source_table or "").strip()]
            )
            if not (source_required_ok and target_required_ok):
                raise ValueError(
                    "When flow_tasks is not provided and source_type=table|view, "
                    "source_schema/source_table/target_schema/target_table are required."
                )
        elif self.source_type in FILE_SOURCE_TYPES:
            if not (self.file_path or "").strip():
                raise ValueError("file_path is required when source_type=csv|json.")
            if not target_required_ok:
                raise ValueError(
                    "target_schema/target_table (or target_type='file') are required."
                )
        elif not target_required_ok:
            raise ValueError(
                "When flow_tasks is not provided, target_schema/target_table are required."
            )
        if self.source_type == "sql" and self.column_mapping_mode != "mapping_file":
            raise ValueError(
                "column_mapping_mode='mapping_file' is required when source_type='sql'."
            )
        if self.source_type == "sql" and not (self.inline_sql or "").strip():
            raise ValueError("inline_sql is required when source_type='sql'.")
        items = list(self.bindings or [])
        names = [item.variable_name for item in items]
        if len(names) != len(set(names)):
            raise ValueError("bindings.variable_name must be unique within a task.")
        _validate_bindings_expression_contract(
            self.where,
            items,
            expression_label="Where Clause",
        )
        _validate_single_task_params(
            self.where, items, list(self.dag_params or [])
        )
        return self


class MappingGeneratePayload(BaseModel):
    model_config = {"extra": "forbid"}

    project: str = Field(..., min_length=1)
    domain: str = Field(..., min_length=1)
    level: str = Field(..., min_length=1)
    flow: str = Field(..., min_length=1)
    source_conn_id: str = Field(..., min_length=1)
    target_conn_id: str = Field(..., min_length=1)
    source_type: str = "table"
    source_schema: str | None = Field(default=None, min_length=1)
    source_table: str | None = Field(default=None, min_length=1)
    inline_sql: str | None = None
    task_group_id: str | None = Field(default=None, min_length=1)
    task_no: int = Field(1, ge=1)
    version: str = "v1.1"
    # F1.4/F1.5 — file source preview inputs
    file_path: str | None = None
    delimiter: str | None = None
    encoding: str | None = None
    quotechar: str | None = None
    header: bool | None = None
    json_mode: str | None = None

    @field_validator("project", "domain", "level", "flow", mode="before")
    @classmethod
    def _v_folder_path_segment(cls, v: Any) -> str:
        return _validate_folder_path_segment(v)

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql", "csv", "json"}:
            raise ValueError(
                "source_type must be one of: table, view, sql, csv, json."
            )
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_required_fields(self) -> "MappingGeneratePayload":
        if self.source_type in {"table", "view"}:
            if (
                not (self.source_schema or "").strip()
                or not (self.source_table or "").strip()
            ):
                raise ValueError(
                    "source_schema and source_table are required when source_type=table|view."
                )
        if self.source_type == "sql" and not (self.inline_sql or "").strip():
            raise ValueError("inline_sql is required when source_type='sql'.")
        if self.source_type in FILE_SOURCE_TYPES and not (self.file_path or "").strip():
            raise ValueError("file_path is required when source_type=csv|json.")
        return self


flow_studio_app = FastAPI(title="Flow Studio", version="1.1.0")
flow_studio_app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).resolve().parent / "static")),
    name="static",
)


def _load_index_html() -> str:
    template_path = (
        Path(__file__).resolve().parent / "templates" / "flow_studio" / "index.html"
    )
    return template_path.read_text(encoding="utf-8")


def _load_dag_explorer_html() -> str:
    template_path = (
        Path(__file__).resolve().parent / "templates" / "dag_explorer" / "index.html"
    )
    return template_path.read_text(encoding="utf-8")


@flow_studio_app.get("/", response_class=HTMLResponse)
def studio_index(response: Response) -> str:
    response.headers["Cache-Control"] = "no-store"
    # F2.1 — inject the edition so the UI can gate Enterprise-only surfaces
    # (INV-8 layer 1). Community hides use_bulk_api; the backend still rejects it.
    from ffengine.core.edition import edition

    return _load_index_html().replace("__FFENGINE_EDITION__", edition())


@flow_studio_app.get("/dag-explorer", response_class=HTMLResponse)
def dag_explorer_index(response: Response) -> str:
    response.headers["Cache-Control"] = "no-store"
    return _load_dag_explorer_html()


def _load_mail_templates_html() -> str:
    template_path = (
        Path(__file__).resolve().parent / "templates" / "mail_templates" / "index.html"
    )
    return template_path.read_text(encoding="utf-8")


@flow_studio_app.get("/mail-templates", response_class=HTMLResponse)
def mail_templates_index(response: Response) -> str:
    response.headers["Cache-Control"] = "no-store"
    return _load_mail_templates_html()


@flow_studio_app.get("/api/mail-templates")
def api_mail_templates_list() -> dict[str, Any]:
    try:
        templates = load_templates()
        names = sorted(
            templates.keys(),
            key=lambda n: (n != DEFAULT_TEMPLATE_NAME, n.lower()),
        )
        return {
            "ok": True,
            "templates": templates,
            "names": names,
            "default_name": DEFAULT_TEMPLATE_NAME,
            "placeholders": PLACEHOLDERS,
        }
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/mail-templates")
def api_mail_template_save(
    payload: MailTemplatePayload,
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        template = save_template(payload.name, payload.subject, payload.html_body)
        return {"ok": True, "name": payload.name.strip(), "template": template}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/mail-templates/delete")
def api_mail_template_delete(
    payload: MailTemplateDeletePayload,
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        delete_template(payload.name)
        return {"ok": True, "name": payload.name.strip()}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/mail-templates/preview")
def api_mail_template_preview(payload: MailTemplatePreviewPayload) -> dict[str, Any]:
    try:
        kind = "failure" if payload.kind == "failure" else "success"
        label = "FAILED" if kind == "failure" else "SUCCEEDED"
        template = {"subject": payload.subject, "html_body": payload.html_body}
        subject, html = render_template(template, sample_meta(kind), label)
        return {"ok": True, "subject": subject, "html": html}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "flow-studio", "dag_marker": STUDIO_DAG_MARKER}


@flow_studio_app.get("/api/dag-explorer")
def api_dag_explorer() -> dict[str, Any]:
    try:
        data = discover_dag_explorer_items()
        return {"ok": True, **data}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/dag-search")
def api_dag_search(q: str | None = None) -> dict[str, Any]:
    """Case-insensitive keyword search inside each DAG's content (.py + YAML)."""
    try:
        data = search_dag_explorer_items(q or "")
        return {"ok": True, **data}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/dbt-assets")
def api_dbt_assets(_: None = Depends(_optional_api_key_dep)) -> dict[str, Any]:
    """F3.2b (EX-D016) — Asset picker catalog: URIs derivable from emitting
    cosmos dbt producers. Double-gated (Enterprise AND provider) -> 422."""
    try:
        data = list_dbt_asset_options()
        return {"ok": True, **data}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/schemas")
def api_schemas(
    conn_id: str = Query(..., min_length=1),
    q: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        items = discover_schemas(conn_id, search=(q or "").strip() or None, limit=limit)
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/connections")
def api_connections() -> dict[str, Any]:
    try:
        items = discover_connections()
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/airflow-variables")
def api_airflow_variables(
    q: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    exact: bool = Query(False),
) -> dict[str, Any]:
    try:
        items = discover_airflow_variables(
            search=(q or "").strip() or None,
            limit=limit,
            exact=exact,
        )
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/timezones")
def api_timezones(
    q: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        items = discover_timezones(search=(q or "").strip() or None, limit=limit)
        return {
            "ok": True,
            "items": items,
            "count": len(items),
            "default_timezone": get_airflow_default_timezone_name(),
        }
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/folder-options")
def api_folder_options(
    project: str | None = Query(None),
    domain: str | None = Query(None),
    level: str | None = Query(None),
    source: str | None = Query(None),
) -> dict[str, Any]:
    try:
        data = discover_hierarchy_options(
            project=project,
            domain=domain,
            level=level,
            source=source,
        )
        return {"ok": True, **data}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/dag-options")
def api_dag_options(
    project: str = Query(..., min_length=1),
    domain: str = Query(..., min_length=1),
    level: str = Query(..., min_length=1),
    flow: str = Query(..., min_length=1),
    dag_id: str | None = Query(None),
) -> dict[str, Any]:
    try:
        data = discover_dag_dependency_options(
            project=project,
            domain=domain,
            level=level,
            flow=flow,
            dag_id=dag_id,
        )
        return {"ok": True, **data}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/tables")
def api_tables(
    conn_id: str = Query(..., min_length=1),
    schema: str = Query(..., min_length=1),
    q: str | None = None,
    limit: int = Query(50, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    if q and len(q.strip()) < 2:
        raise HTTPException(
            status_code=400,
            detail="Enter at least 2 characters for typeahead.",
        )
    try:
        data = discover_tables(
            conn_id=conn_id,
            schema=schema,
            search=(q or "").strip() or None,
            limit=limit,
            offset=offset,
        )
        return {"ok": True, **data}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/columns")
def api_columns(
    conn_id: str = Query(..., min_length=1),
    schema: str = Query(..., min_length=1),
    table: str = Query(..., min_length=1),
) -> dict[str, Any]:
    try:
        items = discover_columns(conn_id=conn_id, schema=schema, table=table)
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


def _payload_to_service_dict(payload: DagUpsertPayload) -> dict[str, Any]:
    """Convert Pydantic model to service-layer dict."""
    data = payload.model_dump(exclude_none=True)
    if payload.dag_params is not None:
        normalized_params: list[dict[str, Any]] = []
        for item in payload.dag_params:
            value = item.model_dump(exclude_none=True)
            if item.name != "log_level":
                for field in ("default", "required", "enum"):
                    value.pop(field, None)
            normalized_params.append(value)
        data["dag_params"] = normalized_params
    return data


@flow_studio_app.post("/api/create-dag", status_code=201)
def api_create_dag(
    payload: DagUpsertPayload,
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = create_or_update_dag(_payload_to_service_dict(payload), update=False)
        return {"ok": True, **result}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/update-dag")
def api_update_dag(
    payload: DagUpsertPayload,
    dag_id: str = Query(..., min_length=1, description="DAG id to update"),
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = create_or_update_dag(
            _payload_to_service_dict(payload),
            update=True,
            dag_id=dag_id,
        )
        return {"ok": True, **result}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/mapping/generate")
def api_mapping_generate(payload: MappingGeneratePayload) -> dict[str, Any]:
    try:
        result = generate_mapping_preview(payload.model_dump(exclude_none=True))
        return {"ok": True, **result}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


class MappingParsePayload(BaseModel):
    model_config = {"extra": "forbid"}

    mapping_content: str = ""


@flow_studio_app.post("/api/mapping/parse")
def api_mapping_parse(payload: MappingParsePayload) -> dict[str, Any]:
    try:
        result = parse_mapping_columns(payload.mapping_content)
        return {"ok": True, **result}
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@flow_studio_app.get("/api/timeline")
def api_timeline(
    limit: int = Query(50, ge=1, le=200),
    dag_id: str | None = Query(None, description="DagRun dag_id filtresi"),
    state: str | None = Query(None, description="DagRun state filtresi"),
) -> dict[str, Any]:
    try:
        items = fetch_timeline_runs(limit=limit, dag_id=dag_id, state=state)
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/dag-config")
def api_dag_config(dag_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    try:
        result = resolve_dag_config_for_update(dag_id=dag_id)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.get("/api/dag-revisions")
def api_dag_revisions(dag_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    try:
        result = get_dag_revisions(dag_id=dag_id)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.post("/api/dag-revisions/promote")
def api_promote_dag_revision(
    dag_id: str = Query(..., min_length=1),
    revision_id: str = Query(..., min_length=1),
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = promote_dag_revision(dag_id=dag_id, revision_id=revision_id)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)


@flow_studio_app.delete("/api/delete-dag")
def api_delete_dag(
    dag_id: str = Query(..., min_length=1),
    cleanup_references: bool = Query(False),
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = delete_dag_bundle(dag_id=dag_id, cleanup_references=cleanup_references)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)
