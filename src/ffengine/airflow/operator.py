"""
C07 — FFEngineOperator + yardımcı fonksiyonlar.

FFEngineOperator, Airflow ortamında FFEngine ETL pipeline'ını orkestre eder:
  plan → prepare → run (3-fazlı iç orkestrasyon).
"""

import logging

from ffengine.core.base_engine import ETLResult
from ffengine.errors.exceptions import ConfigError

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dialect çözümleme
# ---------------------------------------------------------------------------

_CONN_TYPE_TO_DIALECT: dict[str, str] = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mssql": "mssql",
    "tds": "mssql",
    "oracle": "oracle",
}


def _get_dialect_class(dialect_key: str):
    """Dialect key'e göre sınıf döndürür (lazy import)."""
    from ffengine.dialects import PostgresDialect, MSSQLDialect, OracleDialect

    _map = {
        "postgres": PostgresDialect,
        "mssql": MSSQLDialect,
        "oracle": OracleDialect,
    }
    return _map[dialect_key]


def resolve_dialect(conn_type: str):
    """
    Airflow conn_type string'ini BaseDialect örneğine çözer.

    Raises
    ------
    ConfigError : Bilinmeyen conn_type.
    """
    key = _CONN_TYPE_TO_DIALECT.get(conn_type.lower() if conn_type else "")
    if key is None:
        raise ConfigError(
            f"Desteklenmeyen Airflow connection tipi: {conn_type!r}. "
            f"Geçerli değerler: {sorted(_CONN_TYPE_TO_DIALECT)}"
        )
    return _get_dialect_class(key)()


# ---------------------------------------------------------------------------
# WHERE kombinasyonu
# ---------------------------------------------------------------------------


def combine_where(base_where: str | None, partition_where: str | None) -> str | None:
    """
    Base WHERE ve partition WHERE'i AND ile birleştirir.

    Her ikisi de varsa ``(base) AND (partition)`` döner.
    Yalnız biri varsa o döner. İkisi de None ise None döner.
    """
    if base_where and partition_where:
        return f"({base_where}) AND ({partition_where})"
    return base_where or partition_where or None


# ---------------------------------------------------------------------------
# Sonuç birleştirme
# ---------------------------------------------------------------------------


def aggregate_results(results: list[ETLResult]) -> ETLResult:
    """
    Birden fazla partition sonucunu tek bir ETLResult'a birleştirir.

    duration_seconds en uzun partition süresidir (wall-clock).
    """
    if not results:
        return ETLResult(
            rows=0,
            duration_seconds=0.0,
            throughput=0.0,
            partitions_completed=0,
            errors=[],
        )

    total_rows = sum(r.rows for r in results)
    max_duration = max(r.duration_seconds for r in results)
    throughput = total_rows / max_duration if max_duration > 0 else 0.0
    all_errors = [e for r in results for e in r.errors]

    return ETLResult(
        rows=total_rows,
        duration_seconds=round(max_duration, 3),
        throughput=round(throughput, 2),
        partitions_completed=len(results),
        errors=all_errors,
    )


# ---------------------------------------------------------------------------
# Airflow Variable proxy
# ---------------------------------------------------------------------------


class _AirflowVarProxy(dict):
    """BindingResolver context olarak Airflow Variable'larını lazy okur."""

    def __contains__(self, key):
        if super().__contains__(key):
            return True
        try:
            from airflow.models import Variable

            Variable.get(key)
            return True
        except Exception:
            return False

    def __getitem__(self, key):
        if super().__contains__(key):
            return super().__getitem__(key)
        from airflow.models import Variable

        val = Variable.get(key)
        self[key] = val
        return val


def build_airflow_variable_context() -> dict:
    """Airflow Variable'larından BindingResolver context'i oluşturur."""
    return _AirflowVarProxy()


# ---------------------------------------------------------------------------
# FFEngineOperator
# ---------------------------------------------------------------------------


class FFEngineOperator:
    """
    FFEngine ETL pipeline'ını Airflow ortamında orkestre eden operatör.

    3-fazlı iç orkestrasyon:
      1. plan   — config yükle, binding çöz, mapping çöz, partition planla
      2. prepare — TargetWriter.prepare() (bir kez)
      3. run    — her partition için ETLManager.run_etl_task(skip_prepare=True)

    Parameters
    ----------
    config_path      : YAML config dosya yolu.
    task_group_id    : Çalıştırılacak task kimliği.
    source_conn_id   : Airflow kaynak Connection ID.
    target_conn_id   : Airflow hedef Connection ID.
    engine           : Engine tercihi ("auto", "community", "enterprise").
    airflow_context  : BindingResolver context dict (test/CLI için).
    """

    # Airflow template rendering desteği
    template_fields = (
        "config_path",
        "task_group_id",
        "source_conn_id",
        "target_conn_id",
    )

    def __init__(
        self,
        *,
        config_path: str,
        task_group_id: str,
        source_conn_id: str,
        target_conn_id: str,
        engine: str = "auto",
        airflow_context: dict | None = None,
        task_id: str = "ffengine_etl",
        **kwargs,
    ):
        # Airflow BaseOperator uyumluluğu — kurulu ise miras al
        try:
            from airflow.models import BaseOperator

            if not isinstance(self, BaseOperator):
                # Runtime'da BaseOperator mixin
                pass
        except ImportError:
            pass

        self.config_path = config_path
        self.task_group_id = task_group_id
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.engine = engine
        self._airflow_context = airflow_context
        self.task_id = task_id
        # kwargs Airflow BaseOperator'a iletilir
        self._kwargs = kwargs

    def execute(self, context: dict | None = None) -> dict:
        """
        3-fazlı pipeline orkestrasyon.

        Returns
        -------
        dict : Toplam sonuç (rows, duration_seconds, throughput, partitions_completed, errors).
        """
        from ffengine.config.loader import ConfigLoader
        from ffengine.config.binding_resolver import BindingResolver
        from ffengine.db.airflow_adapter import AirflowConnectionAdapter
        from ffengine.db.session import DBSession
        from ffengine.mapping import MappingResolver
        from ffengine.partition import Partitioner
        from ffengine.pipeline.target_writer import TargetWriter
        from ffengine.core.etl_manager import ETLManager

        context = context or {}

        # ---- Phase 1: PLAN ----
        _log.info(
            "C07 Plan fazı: config=%s task=%s",
            self.config_path,
            self.task_group_id,
        )

        # 1. Config yükle
        task_config = ConfigLoader().load(self.config_path, self.task_group_id)

        # 2. Connection parametreleri
        src_params = AirflowConnectionAdapter.get_connection_params(
            self.source_conn_id
        )
        tgt_params = AirflowConnectionAdapter.get_connection_params(
            self.target_conn_id
        )

        # 3. Dialect çöz
        src_dialect = resolve_dialect(src_params["conn_type"])
        tgt_dialect = resolve_dialect(tgt_params["conn_type"])

        # 4. Binding çöz
        airflow_ctx = self._airflow_context or build_airflow_variable_context()
        task_config = BindingResolver().resolve(task_config, airflow_ctx)

        # 5. Session'lar aç, mapping çöz, partition planla, çalıştır
        with DBSession(src_params, src_dialect) as src_session:
            with DBSession(tgt_params, tgt_dialect) as tgt_session:
                # 6. Mapping çöz (C09 entegrasyonu)
                mapping = MappingResolver().resolve(
                    task_config,
                    src_session.conn,
                    src_dialect,
                    tgt_dialect,
                )
                task_config["source_columns"] = mapping.source_columns
                task_config["target_columns"] = mapping.target_columns
                task_config["target_columns_meta"] = mapping.target_columns_meta

                # 7. Partition planla
                specs = Partitioner().plan(
                    task_config, src_session.conn, src_dialect
                )

                # ---- Phase 2: PREPARE ----
                _log.info("C07 Prepare fazı: load_method=%s", task_config.get("load_method"))
                writer = TargetWriter(tgt_session, tgt_dialect)
                writer.prepare(task_config)

                # ---- Phase 3: RUN ----
                _log.info("C07 Run fazı: %d partition", len(specs))
                base_where = task_config.get("_resolved_where")
                results: list[ETLResult] = []
                manager = ETLManager()

                for spec in specs:
                    effective = dict(task_config)
                    effective["_resolved_where"] = combine_where(
                        base_where, spec.get("where")
                    )

                    result = manager.run_etl_task(
                        src_session=src_session,
                        tgt_session=tgt_session,
                        src_dialect=src_dialect,
                        tgt_dialect=tgt_dialect,
                        task_config=effective,
                        partition_spec=None,
                        skip_prepare=True,
                    )
                    results.append(result)

        # ---- Aggregate + XCom ----
        aggregated = aggregate_results(results)

        # XCom push (Airflow context varsa)
        ti = context.get("ti")
        if ti is not None:
            ti.xcom_push(key="rows_transferred", value=aggregated.rows)
            ti.xcom_push(key="duration_seconds", value=aggregated.duration_seconds)
            ti.xcom_push(key="rows_per_second", value=aggregated.throughput)

        summary = {
            "rows": aggregated.rows,
            "duration_seconds": aggregated.duration_seconds,
            "throughput": aggregated.throughput,
            "partitions_completed": aggregated.partitions_completed,
            "errors": aggregated.errors,
        }
        _log.info("C07 Tamamlandı: %s", summary)
        return summary
