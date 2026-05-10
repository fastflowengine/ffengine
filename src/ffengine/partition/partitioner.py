"""
C06 - Partition planner.

Partitioner.plan(task_config, src_conn, src_dialect)
returns list[{"part_id": int, "where": str | None}].

Strategies:
  explicit     : User-provided WHERE list.
  auto_numeric : MIN/MAX based equal-width ranges.
  auto_datetime: MIN/MAX based equal-width datetime ranges.
  percentile   : PERCENTILE_CONT boundaries; falls back to auto_numeric.
  hash_mod     : MOD(col, parts) = i (no DB sampling query).
  distinct     : DISTINCT values grouped into IN(...) clauses.
"""

import logging
import math
from datetime import date, datetime, timezone

from ffengine.errors.exceptions import PartitionError

_log = logging.getLogger(__name__)

# Modes that require partitioning.column
_COLUMN_MODES = frozenset(
    {"auto_numeric", "auto_datetime", "percentile", "hash_mod", "distinct"}
)


class Partitioner:
    """
    Produces partition specs consumed by the runtime.

    specs example:
        [{"part_id": 0, "where": '"id" >= 1 AND "id" <= 250'}, ...]
    """

    def plan(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config.get("partitioning", {})
        if not part.get("enabled", False):
            return self._plan_single_partition()

        mode = part.get("mode", "auto_numeric")
        if mode == "explicit":
            return self._plan_explicit(part)
        if mode == "auto_numeric":
            return self._plan_auto_numeric(task_config, src_conn, src_dialect)
        if mode == "auto_datetime":
            return self._plan_auto_datetime(task_config, src_conn, src_dialect)
        if mode == "percentile":
            return self._plan_percentile(task_config, src_conn, src_dialect)
        if mode == "hash_mod":
            return self._plan_hash_mod(task_config, src_dialect)
        if mode == "distinct":
            return self._plan_distinct(task_config, src_conn, src_dialect)

        raise PartitionError(f"Bilinmeyen partition modu: '{mode}'")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _col(self, part: dict) -> str:
        col = part.get("column")
        if not col:
            raise PartitionError(
                "Bu partition modu icin 'partitioning.column' zorunludur."
            )
        return col

    def _parts(self, part: dict) -> int:
        n = part.get("parts", 4)
        if not isinstance(n, int) or n < 1:
            raise PartitionError(f"partitioning.parts >= 1 olmali, su an: {n!r}")
        return n

    def _distinct_limit(self, part: dict) -> int:
        n = part.get("distinct_limit", 16)
        if not isinstance(n, int) or n < 1:
            raise PartitionError(
                f"partitioning.distinct_limit >= 1 olmali, su an: {n!r}"
            )
        return n

    def _planned_where(self, task_config: dict) -> str | None:
        resolved = str(task_config.get("_resolved_where") or "").strip()
        if resolved:
            return resolved
        raw = str(task_config.get("where") or "").strip()
        return raw or None

    def _source_relation(self, task_config: dict, src_dialect) -> str:
        source_type = str(task_config.get("source_type") or "table").strip().lower()
        if source_type == "sql":
            inline_sql = str(task_config.get("inline_sql") or "").strip()
            if not inline_sql:
                raise PartitionError(
                    "partition planning for source_type='sql' requires inline_sql."
                )
            base_sql = inline_sql.rstrip().rstrip(";")
            return f"({base_sql}) AS ffengine_inline_sql"

        schema = str(task_config.get("source_schema") or "").strip()
        table = str(task_config.get("source_table") or "").strip()
        if not table:
            raise PartitionError("source_table bos olamaz.")
        q_table = src_dialect.quote_identifier(table)
        if schema:
            q_schema = src_dialect.quote_identifier(schema)
            return f"{q_schema}.{q_table}"
        return q_table

    @staticmethod
    def _append_where(sql: str, where_clause: str | None) -> str:
        where_text = str(where_clause or "").strip()
        if not where_text:
            return sql
        return f"{sql} WHERE {where_text}"

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def _plan_single_partition(self) -> list[dict]:
        return [{"part_id": 0, "where": None}]

    def _plan_explicit(self, part: dict) -> list[dict]:
        raw_ranges = part.get("ranges")
        if not isinstance(raw_ranges, list) or not raw_ranges:
            raise PartitionError(
                "partitioning.mode='explicit' icin 'partitioning.ranges' listesi bos olamaz."
            )
        ranges: list[str] = []
        for clause in raw_ranges:
            if not isinstance(clause, str) or not clause.strip():
                raise PartitionError(
                    "partitioning.mode='explicit' icin 'partitioning.ranges' yalnizca dolu string ifadeler icermelidir."
                )
            ranges.append(clause.strip())
        return [{"part_id": i, "where": clause} for i, clause in enumerate(ranges)]

    def _plan_auto_numeric(
        self, task_config: dict, src_conn, src_dialect
    ) -> list[dict]:
        return self._plan_auto_equal_width(
            task_config=task_config,
            src_conn=src_conn,
            src_dialect=src_dialect,
            mode_label="auto_numeric",
            value_kind="numeric",
        )

    def _plan_auto_datetime(
        self, task_config: dict, src_conn, src_dialect
    ) -> list[dict]:
        return self._plan_auto_equal_width(
            task_config=task_config,
            src_conn=src_conn,
            src_dialect=src_dialect,
            mode_label="auto_datetime",
            value_kind="datetime",
        )

    def _plan_auto_equal_width(
        self,
        task_config: dict,
        src_conn,
        src_dialect,
        *,
        mode_label: str,
        value_kind: str,
    ) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)

        q_col = src_dialect.quote_identifier(col)
        relation = self._source_relation(task_config, src_dialect)
        planned_where = self._planned_where(task_config)

        sql = self._append_where(
            f"SELECT MIN({q_col}), MAX({q_col}) FROM {relation}",
            planned_where,
        )

        cursor = src_conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        finally:
            cursor.close()

        if row is None or row[0] is None or row[0] == row[1]:
            return self._plan_single_partition()

        min_val, max_val = row[0], row[1]
        try:
            chunk = (max_val - min_val) / n
        except Exception as exc:  # noqa: BLE001
            raise PartitionError(
                f"partitioning.mode='{mode_label}' icin MIN/MAX degerleri aralik bolmeye uygun degil: {exc}"
            ) from exc

        # Build deterministic boundaries from MIN and chunk; force last boundary to MAX
        # to avoid rounding drift gaps (especially on datetime/timedelta arithmetic).
        boundaries = [min_val + i * chunk for i in range(n)]
        boundaries.append(max_val)

        specs = []
        for i in range(n):
            lo = boundaries[i]
            hi = boundaries[i + 1]
            op_hi = "<=" if i == n - 1 else "<"
            lo_lit = self._partition_value_literal(
                lo, src_dialect, value_kind=value_kind
            )
            hi_lit = self._partition_value_literal(
                hi, src_dialect, value_kind=value_kind
            )
            specs.append(
                {
                    "part_id": i,
                    "where": f"{q_col} >= {lo_lit} AND {q_col} {op_hi} {hi_lit}",
                }
            )
        return specs

    @staticmethod
    def _quoted_text_literal(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _partition_value_literal(self, value, src_dialect, *, value_kind: str) -> str:
        if value_kind == "numeric":
            return str(value)
        if value_kind == "datetime":
            return self._datetime_literal(value, src_dialect)
        raise PartitionError(f"Bilinmeyen value_kind: {value_kind!r}")

    def _datetime_literal(self, value, src_dialect) -> str:
        dialect_name = type(src_dialect).__name__.strip().lower()

        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(timezone.utc)
                tz_suffix = dt_utc.strftime("%z")
                tz_suffix = tz_suffix[:3] + ":" + tz_suffix[3:]
                text_tz = dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f") + tz_suffix
                quoted_tz = self._quoted_text_literal(text_tz)
                if dialect_name in {"postgresdialect", "postgresqldialect"}:
                    return f"TIMESTAMPTZ {quoted_tz}"
                if dialect_name == "mssqldialect":
                    return f"CAST({quoted_tz} AS DATETIMEOFFSET)"
                if dialect_name == "oracledialect":
                    return (
                        "TO_TIMESTAMP_TZ("
                        f"{quoted_tz}, "
                        "'YYYY-MM-DD HH24:MI:SS.FF6TZH:TZM'"
                        ")"
                    )
                dt = dt_utc.replace(tzinfo=None)

            text = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            quoted = self._quoted_text_literal(text)
            if dialect_name == "mssqldialect":
                return f"CAST({quoted} AS DATETIME2)"
            if dialect_name == "oracledialect":
                return "TO_TIMESTAMP(" f"{quoted}, " "'YYYY-MM-DD HH24:MI:SS.FF6'" ")"
            return f"TIMESTAMP {quoted}"

        if isinstance(value, date):
            text = value.isoformat()
            quoted = self._quoted_text_literal(text)
            if dialect_name == "mssqldialect":
                return f"CAST({quoted} AS DATE)"
            return f"DATE {quoted}"

        raise PartitionError(
            "auto_datetime modu datetime/date kolon degeri bekler; "
            f"alinan tip: {type(value).__name__}"
        )

    def _plan_percentile(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)

        q_col = src_dialect.quote_identifier(col)
        relation = self._source_relation(task_config, src_dialect)
        planned_where = self._planned_where(task_config)
        dialect_name = type(src_dialect).__name__
        fractions = [i / n for i in range(1, n)]

        try:
            boundaries = self._query_percentiles(
                dialect_name=dialect_name,
                fractions=fractions,
                q_col=q_col,
                relation=relation,
                planned_where=planned_where,
                src_conn=src_conn,
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                "percentile sorgusu basarisiz (%s), auto_numeric'e dusuluyor: %s",
                dialect_name,
                exc,
            )
            return self._plan_auto_numeric(task_config, src_conn, src_dialect)

        if not boundaries:
            return self._plan_single_partition()

        minmax_sql = self._append_where(
            f"SELECT MIN({q_col}), MAX({q_col}) FROM {relation}",
            planned_where,
        )
        cursor = src_conn.cursor()
        try:
            cursor.execute(minmax_sql)
            row = cursor.fetchone()
        finally:
            cursor.close()

        if row is None or row[0] is None:
            return self._plan_single_partition()

        all_bounds = [row[0]] + list(boundaries) + [row[1]]
        specs = []
        for i in range(len(all_bounds) - 1):
            lo = all_bounds[i]
            hi = all_bounds[i + 1]
            op_hi = "<=" if i == len(all_bounds) - 2 else "<"
            specs.append(
                {
                    "part_id": i,
                    "where": f"{q_col} >= {lo} AND {q_col} {op_hi} {hi}",
                }
            )
        return specs

    def _query_percentiles(
        self,
        dialect_name: str,
        fractions: list[float],
        q_col: str,
        relation: str,
        planned_where: str | None,
        src_conn,
    ) -> list:
        """Runs dialect-specific PERCENTILE_CONT queries."""
        results = []
        normalized_dialect = str(dialect_name or "").strip().lower()
        for frac in fractions:
            if normalized_dialect in {"postgresdialect", "postgresqldialect"}:
                sql = (
                    f"SELECT PERCENTILE_CONT({frac}) WITHIN GROUP "
                    f"(ORDER BY {q_col}) "
                    f"FROM {relation}"
                )
            elif normalized_dialect == "mssqldialect":
                sql = (
                    f"SELECT TOP 1 PERCENTILE_CONT({frac}) WITHIN GROUP "
                    f"(ORDER BY {q_col}) OVER () AS p "
                    f"FROM {relation}"
                )
            elif normalized_dialect == "oracledialect":
                sql = (
                    f"SELECT PERCENTILE_CONT({frac}) WITHIN GROUP "
                    f"(ORDER BY {q_col}) FROM {relation}"
                )
            else:
                raise NotImplementedError(f"percentile desteklenmiyor: {dialect_name}")
            sql = self._append_where(sql, planned_where)
            cursor = src_conn.cursor()
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    results.append(row[0])
            finally:
                cursor.close()
        return results

    def _plan_hash_mod(self, task_config: dict, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)

        q_col = src_dialect.quote_identifier(col)
        dialect_name = type(src_dialect).__name__
        if dialect_name == "MSSQLDialect":
            clause_tmpl = f"{q_col} % {n} = {{i}}"
        else:
            clause_tmpl = f"MOD({q_col}, {n}) = {{i}}"

        return [{"part_id": i, "where": clause_tmpl.format(i=i)} for i in range(n)]

    def _plan_distinct(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)
        distinct_limit = self._distinct_limit(part)

        q_col = src_dialect.quote_identifier(col)
        relation = self._source_relation(task_config, src_dialect)
        planned_where = self._planned_where(task_config)

        base_query = f"SELECT DISTINCT {q_col} FROM {relation}"
        base_query = self._append_where(base_query, planned_where)
        base_query = f"{base_query} ORDER BY {q_col}"
        query = src_dialect.get_pagination_query(base_query, distinct_limit, 0)

        cursor = src_conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()

        values = [row[0] for row in rows]
        if not values:
            return self._plan_single_partition()

        chunk_size = math.ceil(len(values) / n)
        specs = []
        for i in range(0, len(values), chunk_size):
            group = values[i:i + chunk_size]
            if isinstance(group[0], str):
                in_list = ", ".join(f"'{v}'" for v in group)
            else:
                in_list = ", ".join(str(v) for v in group)
            specs.append(
                {
                    "part_id": len(specs),
                    "where": f"{q_col} IN ({in_list})",
                }
            )
        return specs
