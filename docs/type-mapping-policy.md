# Type Mapping Policy (Community)

This document defines the FFEngine type-conversion contract for cross-dialect ETL.

## Core Rules

- Priority order: `exact_match -> safe_widening -> fail`.
- Silent narrowing is not allowed.
- If the target DB cannot represent the source type without data-loss risk, mapping fails explicitly.
- DDL parameter rendering is type-family aware:
  - Integer/date/time/uuid families stay parameterless.
  - Precision/scale is applied only to numeric precision families.
  - Length is applied only to length-bearing text/binary families.

## DDL Safety Rules

- Existing explicit type parameters are respected as-is (example: `DECIMAL(18,4)`, `NVARCHAR(MAX)`).
- Metadata precision/scale is not duplicated when type already contains parameters.
- Text fallback is staged:
  - Use known length when available.
  - Otherwise use a safe default length.
  - Use `MAX`/`CLOB` only when truly unbounded.

## Numeric Overflow Policy

- MSSQL `DECIMAL/NUMERIC` precision must be `<= 38`.
- Precision overflow raises explicit mapping error.
- Auto-clamp is not allowed.

## Type Matrix (Current Canonical Mapping Examples)

| Canonical | Postgres | MSSQL | Oracle |
|---|---|---|---|
| `INTEGER` | `INTEGER` | `INT` | `NUMBER(10)` |
| `BIGINT` | `BIGINT` | `BIGINT` | `NUMBER(19)` |
| `SMALLINT` | `SMALLINT` | `SMALLINT` | `NUMBER(5)` |
| `NUMERIC` | `NUMERIC(p,s)` | `DECIMAL(p,s)` | `NUMBER(p,s)` |
| `VARCHAR` | `VARCHAR(n)` | `NVARCHAR(n)` | `VARCHAR2(n)` |
| `TEXT` | `TEXT` | `NVARCHAR(MAX)` | `CLOB` |
| `TIMESTAMP` | `TIMESTAMP` | `DATETIME2` | `TIMESTAMP` |
| `TIMESTAMP_TZ` | `TIMESTAMP WITH TIME ZONE` | `DATETIMEOFFSET` | `TIMESTAMP WITH TIME ZONE` |
| `UUID` | `UUID` | `UNIQUEIDENTIFIER` | `VARCHAR2(36)` |

## Runtime Source of Truth

- Runtime policy and mapping logic live in:
  - `src/ffengine/dialects/type_mapper.py`
  - dialect DDL builders (`src/ffengine/dialects/*.py`)
  - mapping normalization (`src/ffengine/mapping/resolver.py`)
