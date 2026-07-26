"""
Shared mapping type-contract helpers.

This module centralizes:
- Type literal rendering from ColumnInfo metadata.
- Global strict mapping-file validation rules.
"""

from __future__ import annotations

import re
from typing import Any

from ffengine.dialects.base import ColumnInfo

_TYPE_PATTERN = re.compile(r"^([A-Z][A-Z0-9_ ]*?)(?:\((.+)\))?$")

LENGTH_BEARING_TYPES: frozenset[str] = frozenset(
    {
        "VARCHAR",
        "NVARCHAR",
        "CHAR",
        "NCHAR",
        "CHARACTER",
        "CHARACTER VARYING",
        "VARCHAR2",
        "NVARCHAR2",
        "RAW",
        "VARBINARY",
        "BINARY",
    }
)

NUMERIC_PARAM_TYPES: frozenset[str] = frozenset({"DECIMAL", "NUMERIC", "NUMBER"})


def parse_type(raw: str) -> tuple[str, str | None]:
    normalized = str(raw or "").strip().upper()
    m = _TYPE_PATTERN.match(normalized)
    if not m:
        return normalized, None
    return m.group(1).strip(), m.group(2)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def render_type_literal(
    *,
    base_type: str,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    """
    Render a deterministic type literal from base type + metadata.

    - Keeps explicit incoming params as-is.
    - Adds precision/scale for numeric families when present.
    - Adds length for length-bearing families when present.
    - Keeps parameterless families untouched.
    """
    base, params = parse_type(base_type)
    if not base:
        return "TEXT"
    if params is not None:
        return str(base_type or "").strip().upper()

    p = _as_int(precision)
    s = _as_int(scale)

    if base in NUMERIC_PARAM_TYPES and p is not None and p > 0:
        if s is not None and s >= 0:
            return f"{base}({p},{s})"
        return f"{base}({p})"

    if base in LENGTH_BEARING_TYPES and p is not None and p > 0:
        return f"{base}({p})"

    return base


def render_type_literal_from_column(col: ColumnInfo) -> str:
    return render_type_literal(
        base_type=col.data_type,
        precision=col.precision,
        scale=col.scale,
    )


def _raise(error_cls: type[Exception], message: str) -> None:
    raise error_cls(message)


def validate_mapping_object_strict(
    mapping_obj: Any,
    *,
    valid_versions: frozenset[str] | set[str],
    error_cls: type[Exception] = ValueError,
    context: str = "mapping",
) -> dict[str, Any]:
    """
    Validate mapping YAML contract under global strict policy.

    Strict rules:
    - Length-bearing target types must include explicit length params.
    - DECIMAL/NUMERIC/NUMBER target types must include explicit precision/scale params.
    """
    if not isinstance(mapping_obj, dict):
        _raise(error_cls, f"Invalid mapping ({context}): root must be a dict.")

    # Same-vs-cross Connection Type (dialect) governs the "max size" rule:
    # when source and target dialects are identical, an unsized (bare) numeric /
    # length target is a lossless "use max size" passthrough (provably non-
    # narrowing), so the explicit-params requirement is waived. A missing or
    # unequal pair keeps the strict fail-loud behaviour (no silent narrowing).
    _sd = str(mapping_obj.get("source_dialect") or "").strip().lower()
    _td = str(mapping_obj.get("target_dialect") or "").strip().lower()
    same_dialect = bool(_sd) and _sd == _td

    version = mapping_obj.get("version")
    if version not in valid_versions:
        _raise(
            error_cls,
            f"Unsupported mapping file version ({context}): {version!r}. "
            f"Valid: {sorted(valid_versions)}",
        )

    entries = mapping_obj.get("columns")
    if not isinstance(entries, list) or not entries:
        _raise(error_cls, f"Invalid mapping ({context}): columns must be a non-empty list.")

    seen_targets: set[str] = set()
    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            _raise(error_cls, f"Invalid mapping ({context}): columns[{idx-1}] must be a dict.")

        src_col = str(entry.get("source_name") or "").strip()
        tgt_col = str(entry.get("target_name") or "").strip()
        tgt_type = str(entry.get("target_type") or "").strip()
        expr = str(entry.get("expression") or "").strip()

        if not tgt_col:
            _raise(
                error_cls,
                f"Invalid mapping ({context}): columns[{idx-1}] target_name cannot be empty.",
            )
        if not tgt_type:
            _raise(
                error_cls,
                f"Invalid mapping ({context}): columns[{idx-1}] target_type cannot be empty.",
            )

        # v1.1 XOR: a column is either a direct mapping (source_name) or a
        # derived push-down column (expression) - exactly one, never both.
        if src_col and expr:
            _raise(
                error_cls,
                f"Invalid mapping ({context}): column '{tgt_col}' sets both "
                "source_name and expression; use exactly one.",
            )
        if not src_col and not expr:
            _raise(
                error_cls,
                f"Invalid mapping ({context}): column '{tgt_col}' must set exactly "
                "one of source_name or expression.",
            )
        if expr and version == "v1":
            _raise(
                error_cls,
                f"Invalid mapping ({context}): column '{tgt_col}' uses expression, "
                "which requires mapping version v1.1.",
            )
        if tgt_col in seen_targets:
            _raise(
                error_cls,
                f"Invalid mapping ({context}): duplicate target_name '{tgt_col}'.",
            )
        seen_targets.add(tgt_col)

        base, params = parse_type(tgt_type)
        if base in LENGTH_BEARING_TYPES and params is None and not same_dialect:
            _raise(
                error_cls,
                "Global strict mapping validation failed: "
                f"column '{tgt_col}' target_type '{tgt_type}' requires explicit length "
                "(example: CHAR(3), VARCHAR2(140)). Regenerate mapping.",
            )
        if base in NUMERIC_PARAM_TYPES and params is None and not same_dialect:
            _raise(
                error_cls,
                "Global strict mapping validation failed: "
                f"column '{tgt_col}' target_type '{tgt_type}' requires explicit precision/scale "
                "(example: NUMBER(10), NUMERIC(18,4)). Regenerate mapping.",
            )

    return mapping_obj
