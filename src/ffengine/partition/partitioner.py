"""
C06 — Partition planlayıcı.

Partitioner.plan(task_config, src_conn, src_dialect) → list[{"part_id": int, "where": str | None}]

Stratejiler:
  explicit     : Kullanıcı tarafından sağlanan WHERE listesi.
  auto_numeric : MIN/MAX sorgusuna dayalı eşit genişlikli aralıklar.
  percentile   : PERCENTILE_CONT sorgusu; desteklenmiyorsa auto_numeric'e düşer.
  hash_mod     : MOD(col, parts) = i — DB sorgusu gerekmez.
  distinct     : DISTINCT değerler IN(...) gruplarına bölünür.
"""

import logging
import math

from ffengine.errors.exceptions import PartitionError

_log = logging.getLogger(__name__)

# Kolon gerektiren modlar
_COLUMN_MODES = frozenset({"auto_numeric", "percentile", "hash_mod", "distinct"})


class Partitioner:
    """
    Partition planı üretir.

    Kullanım::

        specs = Partitioner().plan(task_config, src_conn, src_dialect)
        # specs: [{"part_id": 0, "where": "id >= 1 AND id <= 250"}, ...]

    FlowManager.run_flow_task() her spec için ayrı çağrılır.
    """

    def plan(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        """
        Parameters
        ----------
        task_config  : ConfigLoader.load() çıktısı (normalize edilmiş).
        src_conn     : Ham DB bağlantısı (DBSession.conn).
        src_dialect  : BaseDialect implementasyonu.

        Returns
        -------
        [{"part_id": int, "where": str | None}, ...]
        """
        part = task_config.get("partitioning", {})
        if not part.get("enabled", False):
            return self._plan_single_partition()

        mode = part.get("mode", "auto")
        if mode == "auto":
            mode = "auto_numeric"

        if mode == "explicit":
            return self._plan_explicit(part)
        if mode == "auto_numeric":
            return self._plan_auto_numeric(task_config, src_conn, src_dialect)
        if mode == "percentile":
            return self._plan_percentile(task_config, src_conn, src_dialect)
        if mode == "hash_mod":
            return self._plan_hash_mod(task_config, src_dialect)
        if mode == "distinct":
            return self._plan_distinct(task_config, src_conn, src_dialect)

        raise PartitionError(f"Bilinmeyen partition modu: '{mode}'")

    # ------------------------------------------------------------------
    # Private helpers — yardımcılar
    # ------------------------------------------------------------------

    def _col(self, part: dict) -> str:
        col = part.get("column")
        if not col:
            raise PartitionError(
                "Bu partition modu için 'partitioning.column' zorunludur."
            )
        return col

    def _parts(self, part: dict) -> int:
        n = part.get("parts", 4)
        if not isinstance(n, int) or n < 1:
            raise PartitionError(
                f"partitioning.parts >= 1 olmalıdır, şu an: {n!r}"
            )
        return n

    def _distinct_limit(self, part: dict) -> int:
        n = part.get("distinct_limit", 16)
        if not isinstance(n, int) or n < 1:
            raise PartitionError(
                f"partitioning.distinct_limit >= 1 olmalıdır, şu an: {n!r}"
            )
        return n

    # ------------------------------------------------------------------
    # Stratejiler
    # ------------------------------------------------------------------

    def _plan_single_partition(self) -> list[dict]:
        return [{"part_id": 0, "where": None}]

    def _plan_explicit(self, part: dict) -> list[dict]:
        raw_ranges = part.get("ranges")
        if not isinstance(raw_ranges, list) or not raw_ranges:
            raise PartitionError(
                "partitioning.mode='explicit' için 'partitioning.ranges' listesi boş olamaz."
            )
        ranges: list[str] = []
        for clause in raw_ranges:
            if not isinstance(clause, str) or not clause.strip():
                raise PartitionError(
                    "partitioning.mode='explicit' için 'partitioning.ranges' yalnızca dolu string ifadeler içermelidir."
                )
            ranges.append(clause.strip())
        return [{"part_id": i, "where": clause} for i, clause in enumerate(ranges)]

    def _plan_auto_numeric(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)
        schema = task_config.get("source_schema", "")
        table = task_config.get("source_table", "")

        q_col = src_dialect.quote_identifier(col)
        q_schema = src_dialect.quote_identifier(schema)
        q_table = src_dialect.quote_identifier(table)

        cursor = src_conn.cursor()
        try:
            cursor.execute(
                f"SELECT MIN({q_col}), MAX({q_col}) FROM {q_schema}.{q_table}"
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

        if row is None or row[0] is None or row[0] == row[1]:
            return self._plan_single_partition()

        min_val, max_val = row[0], row[1]
        chunk = (max_val - min_val) / n
        specs = []
        for i in range(n):
            lo = min_val + i * chunk
            hi = min_val + (i + 1) * chunk
            op_hi = "<=" if i == n - 1 else "<"
            specs.append({
                "part_id": i,
                "where": f"{q_col} >= {lo} AND {q_col} {op_hi} {hi}",
            })
        return specs

    def _plan_percentile(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)
        schema = task_config.get("source_schema", "")
        table = task_config.get("source_table", "")

        q_col = src_dialect.quote_identifier(col)
        q_schema = src_dialect.quote_identifier(schema)
        q_table = src_dialect.quote_identifier(table)

        dialect_name = type(src_dialect).__name__

        # Yüzdelik noktaları hesapla (0 ve 1 hariç — sınır değerleri MIN/MAX'tır)
        fractions = [i / n for i in range(1, n)]

        try:
            boundaries = self._query_percentiles(
                dialect_name, fractions, q_col, q_schema, q_table, src_conn
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                "percentile sorgusu başarısız (%s), auto_numeric'e düşülüyor: %s",
                dialect_name,
                exc,
            )
            return self._plan_auto_numeric(task_config, src_conn, src_dialect)

        if not boundaries:
            return self._plan_single_partition()

        # MIN ve MAX'ı ekle
        cursor = src_conn.cursor()
        try:
            cursor.execute(
                f"SELECT MIN({q_col}), MAX({q_col}) FROM {q_schema}.{q_table}"
            )
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
            specs.append({
                "part_id": i,
                "where": f"{q_col} >= {lo} AND {q_col} {op_hi} {hi}",
            })
        return specs

    def _query_percentiles(
        self,
        dialect_name: str,
        fractions: list[float],
        q_col: str,
        q_schema: str,
        q_table: str,
        src_conn,
    ) -> list:
        """Dialect'e özgü PERCENTILE_CONT sorgusunu çalıştırır."""
        results = []
        for frac in fractions:
            if dialect_name in ("PostgreSQLDialect", "MSSQLDialect"):
                sql = (
                    f"SELECT PERCENTILE_CONT({frac}) WITHIN GROUP "
                    f"(ORDER BY {q_col}) OVER () AS p "
                    f"FROM {q_schema}.{q_table} LIMIT 1"
                )
            elif dialect_name == "OracleDialect":
                sql = (
                    f"SELECT PERCENTILE_CONT({frac}) WITHIN GROUP "
                    f"(ORDER BY {q_col}) FROM {q_schema}.{q_table}"
                )
            else:
                raise NotImplementedError(
                    f"percentile desteklenmiyor: {dialect_name}"
                )
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
            # MSSQL: % operatörü
            clause_tmpl = f"{q_col} % {n} = {{i}}"
        else:
            # PostgreSQL, Oracle ve diğerleri: MOD()
            clause_tmpl = f"MOD({q_col}, {n}) = {{i}}"

        return [
            {"part_id": i, "where": clause_tmpl.format(i=i)}
            for i in range(n)
        ]

    def _plan_distinct(self, task_config: dict, src_conn, src_dialect) -> list[dict]:
        part = task_config["partitioning"]
        col = self._col(part)
        n = self._parts(part)
        distinct_limit = self._distinct_limit(part)
        schema = task_config.get("source_schema", "")
        table = task_config.get("source_table", "")

        q_col = src_dialect.quote_identifier(col)
        q_schema = src_dialect.quote_identifier(schema)
        q_table = src_dialect.quote_identifier(table)

        base_query = f"SELECT DISTINCT {q_col} FROM {q_schema}.{q_table} ORDER BY {q_col}"
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

        # Değerleri n gruba böl
        chunk_size = math.ceil(len(values) / n)
        specs = []
        for i in range(0, len(values), chunk_size):
            group = values[i: i + chunk_size]
            if isinstance(group[0], str):
                in_list = ", ".join(f"'{v}'" for v in group)
            else:
                in_list = ", ".join(str(v) for v in group)
            specs.append({
                "part_id": len(specs),
                "where": f"{q_col} IN ({in_list})",
            })
        return specs
