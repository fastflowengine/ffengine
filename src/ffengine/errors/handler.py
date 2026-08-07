"""C10 - Central error normalization and classification helpers."""

from __future__ import annotations

import re
from typing import Any

from ffengine.errors.exceptions import (
    CheckpointError,
    ConfigError,
    ConnectionError,
    DeliveryPolicyError,
    DialectError,
    EngineError,
    FFEngineError,
    MappingError,
    PartitionError,
    ReconciliationError,
    ValidationError,
)

_HTTP_STATUS_MAP: dict[type[FFEngineError], int] = {
    ConfigError: 400,
    ValidationError: 400,
    MappingError: 400,
    PartitionError: 400,
    ConnectionError: 502,
    DialectError: 500,
    CheckpointError: 500,
    DeliveryPolicyError: 500,
    EngineError: 500,
    ReconciliationError: 500,
}

_DB_DRIVER_HINTS = (
    "psycopg",
    "psycopg2",
    "sqlalchemy",
    "pyodbc",
    "oracledb",
    "cx_oracle",
    "pymssql",
)


def normalize_exception(exc: Exception) -> FFEngineError:
    """
    Convert non-domain exceptions into FFEngine domain exceptions.

    Rules:
    - FFEngineError -> unchanged
    - ValueError/TypeError -> ValidationError
    - KeyError -> ConfigError
    - Others -> EngineError
    """
    if isinstance(exc, FFEngineError):
        return exc
    if isinstance(exc, (ValueError, TypeError)):
        return ValidationError.wrap(exc)
    if isinstance(exc, KeyError):
        return ConfigError.wrap(exc)
    return EngineError.wrap(exc)


def sanitize_sql_preview(sql: str | None, *, max_len: int = 300) -> str | None:
    """
    Build a short SQL preview safe for logs.

    - Collapses whitespace
    - Masks single-quoted string literals as '?'
    - Truncates to max_len
    """
    raw = str(sql or "").strip()
    if not raw:
        return None
    normalized = re.sub(r"\s+", " ", raw)
    masked = re.sub(r"'(?:''|[^'])*'", "'?'", normalized)
    if len(masked) > max_len:
        return f"{masked[:max_len]}..."
    return masked


def _unwrap_exception_chain(exc: Exception) -> list[Exception]:
    chain: list[Exception] = []
    seen: set[int] = set()
    current: Exception | None = exc

    while isinstance(current, Exception) and id(current) not in seen:
        chain.append(current)
        seen.add(id(current))

        # SQLAlchemy-style wrappers keep DBAPI error in `.orig`.
        orig = getattr(current, "orig", None)
        if isinstance(orig, Exception) and id(orig) not in seen:
            current = orig
            continue

        next_exc = getattr(current, "cause", None) or getattr(
            current, "__cause__", None
        )
        current = next_exc if isinstance(next_exc, Exception) else None

    return chain


def _extract_sqlstate(exc: Exception) -> str | None:
    for attr in ("sqlstate", "pgcode"):
        val = getattr(exc, attr, None)
        if val:
            return str(val)
    diag = getattr(exc, "diag", None)
    if diag is not None:
        val = getattr(diag, "sqlstate", None)
        if val:
            return str(val)
    return None


def _is_db_exception(exc: Exception) -> bool:
    module_name = str(type(exc).__module__ or "").lower()
    if any(hint in module_name for hint in _DB_DRIVER_HINTS):
        return True
    if _extract_sqlstate(exc):
        return True
    return False


def extract_db_error_details(exc: Exception) -> dict[str, Any]:
    """Extract DB-oriented diagnostics from wrapped exception chains."""
    norm = normalize_exception(exc)
    chain = _unwrap_exception_chain(norm)
    db_exc = next((item for item in chain if _is_db_exception(item)), None)
    if db_exc is None:
        return {}

    root = chain[-1] if chain else db_exc
    details: dict[str, Any] = {
        "db_exception_type": type(db_exc).__name__,
        "db_error_message": str(db_exc),
        "db_driver": str(type(db_exc).__module__ or "").split(".")[0],
        "db_root_cause": str(root),
    }
    sqlstate = _extract_sqlstate(db_exc)
    if sqlstate:
        details["db_sqlstate"] = sqlstate
    return details


def http_status_for(exc: Exception, default: int = 500) -> int:
    """Return suggested HTTP status code by normalized exception type."""
    norm = normalize_exception(exc)
    for cls, status in _HTTP_STATUS_MAP.items():
        if isinstance(norm, cls):
            return status
    return default


def error_payload(exc: Exception) -> dict[str, Any]:
    """Build a stable error payload for logs/API responses."""
    norm = normalize_exception(exc)
    details = dict(norm.details or {})
    db_details = extract_db_error_details(norm)
    for key, value in db_details.items():
        details.setdefault(key, value)
    return {
        "error_type": type(norm).__name__,
        "error_code": norm.code,
        "message": norm.message,
        "details": details,
    }
