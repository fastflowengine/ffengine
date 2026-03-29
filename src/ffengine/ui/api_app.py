"""Airflow 3 FastAPI app for ETL Studio."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
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
    resolve_dag_config_for_update,
    discover_connections,
    discover_columns,
    discover_hierarchy_options,
    discover_schemas,
    discover_tables,
    fetch_timeline_runs,
)


def _raise_http_from_exception(exc: Exception) -> None:
    """C10: domain exception normalize edip tutarlı HTTP yanıtı üretir."""
    if isinstance(exc, HTTPException):
        raise exc
    norm = normalize_exception(exc)
    raise HTTPException(status_code=http_status_for(norm), detail=norm.message) from exc


def _optional_api_key_dep(
    x_etl_studio_api_key: str | None = Header(
        None,
        alias="X-ETL-Studio-API-Key",
        description="ETL Studio API anahtari (ETL_STUDIO_API_KEY ortam degiskeni ayarliysa zorunlu)",
    ),
) -> None:
    """T12: Mutasyon endpointleri icin opsiyonel API anahtari dogrulamasi."""
    expected = os.getenv("ETL_STUDIO_API_KEY", "").strip()
    if not expected:
        return
    if not x_etl_studio_api_key or x_etl_studio_api_key.strip() != expected:
        raise HTTPException(
            status_code=401,
            detail="Gecersiz veya eksik X-ETL-Studio-API-Key basligi.",
        )


class EtlTaskPayload(BaseModel):
    model_config = {"extra": "forbid"}

    task_group_id: str | None = Field(default=None, min_length=1)
    source_schema: str = Field(..., min_length=1)
    source_table: str = Field(..., min_length=1)
    source_type: str = "table"
    target_schema: str = Field(..., min_length=1)
    target_table: str = Field(..., min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_ranges: list[Any] | None = None

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view"}:
            raise ValueError("source_type yalnizca 'table' veya 'view' olabilir.")
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"source_type gecersiz: {v!r}")
        return v

    @field_validator("load_method")
    @classmethod
    def _v_load_method(cls, v: str) -> str:
        if v not in VALID_LOAD_METHODS:
            raise ValueError(f"load_method gecersiz: {v!r}")
        return v

    @field_validator("column_mapping_mode")
    @classmethod
    def _v_col_map(cls, v: str) -> str:
        if v not in VALID_COLUMN_MAPPING_MODES:
            raise ValueError(f"column_mapping_mode gecersiz: {v!r}")
        return v

    @field_validator("partitioning_mode")
    @classmethod
    def _v_part_mode(cls, v: str) -> str:
        if v not in VALID_PARTITION_MODES:
            raise ValueError(f"partitioning.mode gecersiz: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_mapping(self) -> EtlTaskPayload:
        if self.column_mapping_mode == "mapping_file" and not (self.mapping_file or "").strip():
            raise ValueError("column_mapping_mode='mapping_file' icin mapping_file yolu gerekir.")
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
    target_schema: str | None = Field(default=None, min_length=1)
    target_table: str | None = Field(default=None, min_length=1)
    load_method: str = "create_if_not_exists_or_truncate"
    column_mapping_mode: str = "source"
    mapping_file: str | None = None
    where: str | None = None
    batch_size: int = Field(10000, ge=1, le=1_000_000)
    partitioning_enabled: bool = False
    partitioning_mode: str = "auto"
    partitioning_column: str | None = None
    partitioning_parts: int = Field(2, ge=1, le=10_000)
    partitioning_ranges: list[Any] | None = None
    task_group_id: str | None = Field(default=None, min_length=1)
    etl_tasks: list[EtlTaskPayload] | None = None

    @field_validator("source_type")
    @classmethod
    def _v_source_type(cls, v: str) -> str:
        if v not in {"table", "view"}:
            raise ValueError("source_type yalnizca 'table' veya 'view' olabilir.")
        if v not in VALID_SOURCE_TYPES:
            raise ValueError(f"source_type gecersiz: {v!r}")
        return v

    @field_validator("load_method")
    @classmethod
    def _v_load_method(cls, v: str) -> str:
        if v not in VALID_LOAD_METHODS:
            raise ValueError(f"load_method gecersiz: {v!r}")
        return v

    @field_validator("column_mapping_mode")
    @classmethod
    def _v_col_map(cls, v: str) -> str:
        if v not in VALID_COLUMN_MAPPING_MODES:
            raise ValueError(f"column_mapping_mode gecersiz: {v!r}")
        return v

    @field_validator("partitioning_mode")
    @classmethod
    def _v_part_mode(cls, v: str) -> str:
        if v not in VALID_PARTITION_MODES:
            raise ValueError(f"partitioning.mode gecersiz: {v!r}")
        return v

    @model_validator(mode="after")
    def _v_mapping(self) -> DagUpsertPayload:
        has_task_list = isinstance(self.etl_tasks, list) and len(self.etl_tasks) > 0
        if has_task_list:
            return self
        if not all(
            [
                (self.source_schema or "").strip(),
                (self.source_table or "").strip(),
                (self.target_schema or "").strip(),
                (self.target_table or "").strip(),
            ]
        ):
            raise ValueError(
                "etl_tasks verilmediyse source_schema/source_table/target_schema/target_table zorunludur."
            )
        if self.column_mapping_mode == "mapping_file" and not (self.mapping_file or "").strip():
            raise ValueError("column_mapping_mode='mapping_file' icin mapping_file yolu gerekir.")
        return self


etl_studio_app = FastAPI(title="ETL Studio", version="1.1.0")


def _load_index_html() -> str:
    template_path = (
        Path(__file__).resolve().parent / "templates" / "etl_studio" / "index.html"
    )
    return template_path.read_text(encoding="utf-8")


@etl_studio_app.get("/", response_class=HTMLResponse)
def studio_index() -> str:
    return _load_index_html()


@etl_studio_app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "etl-studio", "dag_marker": STUDIO_DAG_MARKER}


@etl_studio_app.get("/api/schemas")
def api_schemas(conn_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    try:
        items = discover_schemas(conn_id)
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@etl_studio_app.get("/api/connections")
def api_connections() -> dict[str, Any]:
    try:
        items = discover_connections()
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:
        _raise_http_from_exception(exc)


@etl_studio_app.get("/api/folder-options")
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


@etl_studio_app.get("/api/tables")
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
            detail="Typeahead icin en az 2 karakter girin.",
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


@etl_studio_app.get("/api/columns")
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
    """Pydantic modelden servis katmanina dict donustur."""
    return payload.model_dump(exclude_none=True)


@etl_studio_app.post("/api/create-dag", status_code=201)
def api_create_dag(
    payload: DagUpsertPayload,
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = create_or_update_dag(_payload_to_service_dict(payload), update=False)
        return {"ok": True, **result}
    except Exception as exc:
        _raise_http_from_exception(exc)


@etl_studio_app.post("/api/update-dag")
def api_update_dag(
    payload: DagUpsertPayload,
    _: None = Depends(_optional_api_key_dep),
) -> dict[str, Any]:
    try:
        result = create_or_update_dag(_payload_to_service_dict(payload), update=True)
        return {"ok": True, **result}
    except Exception as exc:
        _raise_http_from_exception(exc)


@etl_studio_app.get("/api/timeline")
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


@etl_studio_app.get("/api/dag-config")
def api_dag_config(dag_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    try:
        result = resolve_dag_config_for_update(dag_id=dag_id)
        return {"ok": True, **result}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        _raise_http_from_exception(exc)
