"""Airflow 3 FastAPI app for Flow Studio."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator, model_validator

from ffengine.config.schema import (
    VALID_COLUMN_MAPPING_MODES,
    VALID_LOAD_METHODS,
    VALID_PARTITION_MODES,
    VALID_SOURCE_TYPES,
)
from ffengine.errors import http_status_for, normalize_exception
from ffengine.ui.studio_service import (
    STUDIO_DAG_MARKER,
    create_or_update_dag,
    delete_dag_bundle,
    discover_connections,
    discover_columns,
    discover_hierarchy_options,
    discover_airflow_variables,
    discover_schemas,
    discover_tables,
    fetch_timeline_runs,
    generate_mapping_preview,
    get_dag_revisions,
    promote_dag_revision,
    resolve_dag_config_for_update,
)


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
_WHERE_PARAM_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")
_CUSTOM_TAG_MAX_COUNT = 10


def _extract_where_params(where_clause: str | None) -> set[str]:
    return set(_WHERE_PARAM_RE.findall(str(where_clause or "")))


def _validate_bindings_where_contract(
    where_clause: str | None,
    bindings: list["BindingPayload"] | None,
) -> None:
    where_params = _extract_where_params(where_clause)
    items = list(bindings or [])
    if where_params and not items:
        raise ValueError(
            "Where Clause contains parameter(s) without binding definition: "
            + ", ".join(sorted(where_params))
        )
    if not items:
        return
    binding_names = {item.variable_name for item in items}
    missing = sorted(where_params - binding_names)
    unused = sorted(binding_names - where_params)
    if missing:
        raise ValueError(
            "Where Clause contains parameter(s) without binding definition: "
            + ", ".join(missing)
        )
    if unused:
        raise ValueError(
            "Binding definition exists but parameter(s) are unused in Where Clause: "
            + ", ".join(unused)
        )


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
        if self.binding_source == "airflow_variable" and not (self.airflow_variable_key or "").strip():
            raise ValueError("airflow_variable_key is required when binding_source='airflow_variable'.")
        return self


class FlowTaskPayload(BaseModel):
    model_config = {"extra": "forbid"}

    task_group_id: str | None = Field(default=None, min_length=1)
    source_schema: str | None = Field(default=None, min_length=1)
    source_table: str | None = Field(default=None, min_length=1)
    source_type: str = "table"
    inline_sql: str | None = None
    target_schema: str = Field(..., min_length=1)
    target_table: str = Field(..., min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    mapping_content: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_distinct_limit: int | None = Field(default=None, ge=1, le=1_000_000)
    partitioning_ranges: list[Any] | None = None
    bindings: list[BindingPayload] | None = None

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql"}:
            raise ValueError("source_type must be one of: 'table', 'view', or 'sql'.")
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

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
        if self.source_type == "sql" and self.column_mapping_mode != "mapping_file":
            raise ValueError("column_mapping_mode='mapping_file' is required when source_type='sql'.")
        if self.source_type in {"table", "view"}:
            if not (self.source_schema or "").strip() or not (self.source_table or "").strip():
                raise ValueError("source_schema and source_table are required when source_type=table|view.")
        if self.source_type == "sql" and not (self.inline_sql or "").strip():
            raise ValueError("inline_sql is required when source_type='sql'.")
        items = list(self.bindings or [])
        names = [item.variable_name for item in items]
        if len(names) != len(set(names)):
            raise ValueError("bindings.variable_name must be unique within a task.")
        _validate_bindings_where_contract(self.where, items)
        return self


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
    target_schema: str | None = Field(default=None, min_length=1)
    target_table: str | None = Field(default=None, min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    mapping_content: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_distinct_limit: int | None = Field(default=None, ge=1, le=1_000_000)
    partitioning_ranges: list[Any] | None = None
    bindings: list[BindingPayload] | None = None
    task_group_id: str | None = Field(default=None, min_length=1)
    flow_tasks: list[FlowTaskPayload] | None = None
    custom_tags: list[str] | None = None

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql"}:
            raise ValueError("source_type must be one of: 'table', 'view', or 'sql'.")
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

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
            raise ValueError(f"custom_tags can contain at most {_CUSTOM_TAG_MAX_COUNT} items.")
        return v

    @model_validator(mode="after")
    def _v_mapping(self) -> DagUpsertPayload:
        has_task_list = isinstance(self.flow_tasks, list) and len(self.flow_tasks) > 0
        if has_task_list:
            return self
        target_required_ok = all([(self.target_schema or "").strip(), (self.target_table or "").strip()])
        if self.source_type in {"table", "view"}:
            source_required_ok = all([(self.source_schema or "").strip(), (self.source_table or "").strip()])
            if not (source_required_ok and target_required_ok):
                raise ValueError(
                    "When flow_tasks is not provided and source_type=table|view, source_schema/source_table/target_schema/target_table are required."
                )
        elif not target_required_ok:
            raise ValueError(
                "When flow_tasks is not provided, target_schema/target_table are required."
            )
        if self.source_type == "sql" and self.column_mapping_mode != "mapping_file":
            raise ValueError("column_mapping_mode='mapping_file' is required when source_type='sql'.")
        if self.source_type == "sql" and not (self.inline_sql or "").strip():
            raise ValueError("inline_sql is required when source_type='sql'.")
        items = list(self.bindings or [])
        names = [item.variable_name for item in items]
        if len(names) != len(set(names)):
            raise ValueError("bindings.variable_name must be unique within a task.")
        _validate_bindings_where_contract(self.where, items)
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
    version: str = "v1"

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view", "sql"}:
            raise ValueError("source_type must be one of: 'table', 'view', or 'sql'.")
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"Invalid source_type: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_required_fields(self) -> "MappingGeneratePayload":
        if self.source_type in {"table", "view"}:
            if not (self.source_schema or "").strip() or not (self.source_table or "").strip():
                raise ValueError("source_schema and source_table are required when source_type=table|view.")
        if self.source_type == "sql" and not (self.inline_sql or "").strip():
            raise ValueError("inline_sql is required when source_type='sql'.")
        return self


flow_studio_app = FastAPI(title="Flow Studio", version="1.1.0")
flow_studio_app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).resolve().parent / "static")),
    name="static"
)


def _load_index_html() -> str:
    template_path = (
        Path(__file__).resolve().parent / "templates" / "flow_studio" / "index.html"
    )
    return template_path.read_text(encoding="utf-8")


@flow_studio_app.get("/", response_class=HTMLResponse)
def studio_index(response: Response) -> str:
    response.headers["Cache-Control"] = "no-store"
    return _load_index_html()


@flow_studio_app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "flow-studio", "dag_marker": STUDIO_DAG_MARKER}


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
) -> dict[str, Any]:
    try:
        items = discover_airflow_variables(search=(q or "").strip() or None, limit=limit)
        return {"ok": True, "items": items, "count": len(items)}
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
    return payload.model_dump(exclude_none=True)


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
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = delete_dag_bundle(dag_id=dag_id)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)

