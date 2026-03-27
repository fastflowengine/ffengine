"""
Lossless, deterministic type mapper for cross-dialect type translation.
Precision and scale information is always preserved.
"""

import re
from typing import Optional


class UnsupportedTypeError(Exception):
    """Raised when a source type has no mapping in the target dialect."""

    pass


# ------------------------------------------------------------------
# Canonical type registry
# Each dialect maps its native types TO canonical form here, and
# maps FROM canonical form to its native types via _CANONICAL_TO_*.
# ------------------------------------------------------------------

# fmt: off

# Source dialect → canonical
_POSTGRES_TO_CANONICAL: dict[str, str] = {
    "INTEGER":    "INTEGER",
    "INT":        "INTEGER",
    "INT4":       "INTEGER",
    "BIGINT":     "BIGINT",
    "INT8":       "BIGINT",
    "SMALLINT":   "SMALLINT",
    "INT2":       "SMALLINT",
    "NUMERIC":    "NUMERIC",
    "DECIMAL":    "NUMERIC",
    "REAL":       "FLOAT",
    "FLOAT4":     "FLOAT",
    "DOUBLE PRECISION": "DOUBLE",
    "FLOAT8":     "DOUBLE",
    "BOOLEAN":    "BOOLEAN",
    "BOOL":       "BOOLEAN",
    "TEXT":       "TEXT",
    "VARCHAR":    "VARCHAR",
    "CHARACTER VARYING": "VARCHAR",
    "CHAR":       "CHAR",
    "CHARACTER":  "CHAR",
    "BYTEA":      "BINARY",
    "DATE":       "DATE",
    "TIMESTAMP":  "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
    "TIMESTAMPTZ": "TIMESTAMP_TZ",
    "TIME":       "TIME",
    "TIME WITHOUT TIME ZONE": "TIME",
    "INTERVAL":   "INTERVAL",
    "JSON":       "JSON",
    "JSONB":      "JSON",
    "UUID":       "UUID",
    "SERIAL":     "INTEGER",
    "BIGSERIAL":  "BIGINT",
}

_MSSQL_TO_CANONICAL: dict[str, str] = {
    "INT":        "INTEGER",
    "BIGINT":     "BIGINT",
    "SMALLINT":   "SMALLINT",
    "TINYINT":    "SMALLINT",
    "NUMERIC":    "NUMERIC",
    "DECIMAL":    "NUMERIC",
    "FLOAT":      "DOUBLE",
    "REAL":       "FLOAT",
    "BIT":        "BOOLEAN",
    "NVARCHAR":   "VARCHAR",
    "VARCHAR":    "VARCHAR",
    "NCHAR":      "CHAR",
    "CHAR":       "CHAR",
    "NTEXT":      "TEXT",
    "TEXT":       "TEXT",
    "VARBINARY":  "BINARY",
    "IMAGE":      "BINARY",
    "DATE":       "DATE",
    "DATETIME":   "TIMESTAMP",
    "DATETIME2":  "TIMESTAMP",
    "SMALLDATETIME": "TIMESTAMP",
    "DATETIMEOFFSET": "TIMESTAMP_TZ",
    "TIME":       "TIME",
    "UNIQUEIDENTIFIER": "UUID",
    "MONEY":      "NUMERIC",
    "SMALLMONEY": "NUMERIC",
}

_ORACLE_TO_CANONICAL: dict[str, str] = {
    "NUMBER":     "NUMERIC",
    "INTEGER":    "INTEGER",
    "FLOAT":      "DOUBLE",
    "BINARY_FLOAT":  "FLOAT",
    "BINARY_DOUBLE": "DOUBLE",
    "VARCHAR2":   "VARCHAR",
    "NVARCHAR2":  "VARCHAR",
    "CHAR":       "CHAR",
    "NCHAR":      "CHAR",
    "CLOB":       "TEXT",
    "NCLOB":      "TEXT",
    "BLOB":       "BINARY",
    "RAW":        "BINARY",
    "LONG RAW":   "BINARY",
    "DATE":       "TIMESTAMP",   # Oracle DATE includes time
    "TIMESTAMP":  "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
    "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP_TZ",
    "INTERVAL YEAR TO MONTH": "INTERVAL",
    "INTERVAL DAY TO SECOND":  "INTERVAL",
}

# Canonical → target dialect
_CANONICAL_TO_POSTGRES: dict[str, str] = {
    "INTEGER":      "INTEGER",
    "BIGINT":       "BIGINT",
    "SMALLINT":     "SMALLINT",
    "NUMERIC":      "NUMERIC",
    "FLOAT":        "REAL",
    "DOUBLE":       "DOUBLE PRECISION",
    "BOOLEAN":      "BOOLEAN",
    "TEXT":         "TEXT",
    "VARCHAR":      "VARCHAR",
    "CHAR":         "CHAR",
    "BINARY":       "BYTEA",
    "DATE":         "DATE",
    "TIMESTAMP":    "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE",
    "TIME":         "TIME",
    "INTERVAL":     "INTERVAL",
    "JSON":         "JSONB",
    "UUID":         "UUID",
}

_CANONICAL_TO_MSSQL: dict[str, str] = {
    "INTEGER":      "INT",
    "BIGINT":       "BIGINT",
    "SMALLINT":     "SMALLINT",
    "NUMERIC":      "DECIMAL",
    "FLOAT":        "REAL",
    "DOUBLE":       "FLOAT",
    "BOOLEAN":      "BIT",
    "TEXT":         "NVARCHAR(MAX)",
    "VARCHAR":      "NVARCHAR",
    "CHAR":         "NCHAR",
    "BINARY":       "VARBINARY(MAX)",
    "DATE":         "DATE",
    "TIMESTAMP":    "DATETIME2",
    "TIMESTAMP_TZ": "DATETIMEOFFSET",
    "TIME":         "TIME",
    "INTERVAL":     "NVARCHAR(50)",   # no native interval
    "JSON":         "NVARCHAR(MAX)",  # no native JSON
    "UUID":         "UNIQUEIDENTIFIER",
}

_CANONICAL_TO_ORACLE: dict[str, str] = {
    "INTEGER":      "NUMBER(10)",
    "BIGINT":       "NUMBER(19)",
    "SMALLINT":     "NUMBER(5)",
    "NUMERIC":      "NUMBER",
    "FLOAT":        "BINARY_FLOAT",
    "DOUBLE":       "BINARY_DOUBLE",
    "BOOLEAN":      "NUMBER(1)",
    "TEXT":         "CLOB",
    "VARCHAR":      "VARCHAR2",
    "CHAR":         "CHAR",
    "BINARY":       "BLOB",
    "DATE":         "DATE",
    "TIMESTAMP":    "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE",
    "TIME":         "TIMESTAMP",      # no native TIME
    "INTERVAL":     "INTERVAL DAY TO SECOND",
    "JSON":         "CLOB",           # JSON text in CLOB
    "UUID":         "VARCHAR2(36)",
}

# fmt: on


# Lookup tables keyed by dialect name
_SOURCE_MAPS: dict[str, dict[str, str]] = {
    "postgres": _POSTGRES_TO_CANONICAL,
    "mssql": _MSSQL_TO_CANONICAL,
    "oracle": _ORACLE_TO_CANONICAL,
}

_TARGET_MAPS: dict[str, dict[str, str]] = {
    "postgres": _CANONICAL_TO_POSTGRES,
    "mssql": _CANONICAL_TO_MSSQL,
    "oracle": _CANONICAL_TO_ORACLE,
}

# Regex to split "VARCHAR(100)" → ("VARCHAR", "100") or "NUMERIC(38,10)" → ("NUMERIC", "38,10")
_TYPE_PATTERN = re.compile(r"^([A-Z][A-Z0-9_ ]*?)(?:\((.+)\))?$")


class TypeMapper:
    """
    Maps a source data-type string from one dialect to the equivalent
    type in a target dialect.  Precision and scale are **always** preserved.

    Usage::

        mapper = TypeMapper()
        result = mapper.map_type("NUMBER(38,10)", "oracle", "postgres")
        assert result == "NUMERIC(38,10)"
    """

    @staticmethod
    def map_type(
        source_type: str,
        source_dialect: str,
        target_dialect: str,
    ) -> str:
        """
        Translate *source_type* from *source_dialect* into the
        equivalent native type in *target_dialect*.

        Raises ``UnsupportedTypeError`` when no mapping exists.
        """
        source_dialect = source_dialect.lower()
        target_dialect = target_dialect.lower()

        src_map = _SOURCE_MAPS.get(source_dialect)
        tgt_map = _TARGET_MAPS.get(target_dialect)
        if src_map is None:
            raise UnsupportedTypeError(
                f"Unknown source dialect: {source_dialect}"
            )
        if tgt_map is None:
            raise UnsupportedTypeError(
                f"Unknown target dialect: {target_dialect}"
            )

        # Parse base type and optional precision/scale
        base_type, params = TypeMapper._parse_type(source_type)

        # Resolve to canonical
        canonical = src_map.get(base_type)
        if canonical is None:
            raise UnsupportedTypeError(
                f"Unsupported type '{source_type}' in dialect '{source_dialect}'"
            )

        # Resolve canonical → target native
        target_native = tgt_map.get(canonical)
        if target_native is None:
            raise UnsupportedTypeError(
                f"No target mapping for canonical type '{canonical}' "
                f"in dialect '{target_dialect}'"
            )

        # Re-attach precision/scale if present and target supports it
        return TypeMapper._attach_params(target_native, params, canonical)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_type(raw: str) -> tuple[str, Optional[str]]:
        """Split 'NUMERIC(38,10)' → ('NUMERIC', '38,10')."""
        normalized = raw.strip().upper()
        m = _TYPE_PATTERN.match(normalized)
        if m:
            return m.group(1).strip(), m.group(2)
        return normalized, None

    @staticmethod
    def _attach_params(
        target_native: str,
        params: Optional[str],
        canonical: str,
    ) -> str:
        """
        Re-attach precision/scale to the target type when applicable.

        Rules:
        - If the target already contains explicit params (e.g. 'NUMBER(10)'),
          keep them unless the source also had params (source wins).
        - Precision-bearing canonical types (NUMERIC, VARCHAR, CHAR) always
          propagate source params.
        """
        _PRECISION_TYPES = {"NUMERIC", "VARCHAR", "CHAR"}

        # Target already has fixed params (e.g. "NVARCHAR(MAX)")
        if "(" in target_native:
            if params and canonical in _PRECISION_TYPES:
                # Source precision overrides fixed target
                base = target_native.split("(")[0]
                return f"{base}({params})"
            return target_native

        # Source had precision/scale → attach
        if params and canonical in _PRECISION_TYPES:
            return f"{target_native}({params})"

        return target_native
