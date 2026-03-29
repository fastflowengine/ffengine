"""
C07 — FFEngineOperator + yardımcı fonksiyonlar.

FFEngineOperator, Airflow ortamında FFEngine ETL pipeline'ını orkestre eder:
  plan → prepare → run (3-fazlı iç orkestrasyon).
"""

import logging
import os
from datetime import UTC, datetime

try:
    from airflow.sdk.bases.operator import BaseOperator
except Exception:  # pragma: no cover - airflow olmayan ortamlarda import fallback
    try:
        from airflow.models.baseoperator import BaseOperator
    except Exception:
        class BaseOperator:  # type: ignore[no-redef]
            template_fields: tuple[str, ...] = ()

            def __init__(self, *args, **kwargs):
                self.task_id = kwargs.get("task_id")

from ffengine.core.base_engine import ETLResult
from ffengine.errors import error_payload, normalize_exception
from ffengine.errors.exceptions import ConfigError

_log = logging.getLogger(__name__)
_DEBUG_BREAK_DAG_ID = "ffengine_config_group_12_public_ff_test_data_to_dbo_ff_test_data_psql_v12"


def _log_structured(
    *,
    level: int,
    stage: str,
    message: str,
    task_group_id: str,
    source_db: str,
    target_db: str,
    rows: int = 0,
    duration_seconds: float = 0.0,
    **optional,
) -> None:
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": logging.getLevelName(level),
        "logger": __name__,
        "stage": stage,
        "task_group_id": task_group_id,
        "source_db": source_db,
        "target_db": target_db,
        "rows": rows,
        "duration_seconds": round(float(duration_seconds), 3),
        "message": message,
    }
    for k, v in optional.items():
        if v is not None:
            payload[k] = v
    _log.log(level, "%s", payload)

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


def _maybe_debug_breakpoint(context: dict, *, enabled_once: bool) -> bool:
    if enabled_once:
        return True
    if os.getenv("ENABLE_DEBUG", "0") != "1":
        return False
    dag = (context or {}).get("dag")
    ti = (context or {}).get("ti")
    dag_id = getattr(dag, "dag_id", "") or getattr(ti, "dag_id", "")
    if dag_id != _DEBUG_BREAK_DAG_ID:
        return False
    try:
        import debugpy

        if not debugpy.is_client_connected():
            _log.warning(
                "Debug breakpoint skipped: debug client not connected (dag_id=%s, port=%s)",
                dag_id,
                os.getenv("DEBUGPY_PORT", ""),
            )
            return False
        debugpy.breakpoint()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# FFEngineOperator
# ---------------------------------------------------------------------------


class FFEngineOperator(BaseOperator):
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
        super().__init__(task_id=task_id, **kwargs)

        self.config_path = config_path
        self.task_group_id = task_group_id
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.engine = engine
        self._airflow_context = airflow_context

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

        retry_telemetry = self._retry_telemetry(context)
        ti = context.get("ti")
        source_db = "unknown"
        target_db = "unknown"
        try:
            # ---- Phase 1: PLAN ----
            _log_structured(
                level=logging.INFO,
                stage="airflow",
                message="Operator plan phase started.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                retry_telemetry=retry_telemetry,
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
            source_db = src_params.get("conn_type", "unknown")
            target_db = tgt_params.get("conn_type", "unknown")

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
                    _log_structured(
                        level=logging.INFO,
                        stage="airflow",
                        message="Operator prepare phase.",
                        task_group_id=self.task_group_id,
                        source_db=source_db,
                        target_db=target_db,
                    )
                    writer = TargetWriter(tgt_session, tgt_dialect)
                    writer.prepare(task_config)

                    # ---- Phase 3: RUN ----
                    _log_structured(
                        level=logging.INFO,
                        stage="airflow",
                        message="Operator run phase.",
                        task_group_id=self.task_group_id,
                        source_db=source_db,
                        target_db=target_db,
                        partition_id=len(specs),
                    )
                    base_where = task_config.get("_resolved_where")
                    results: list[ETLResult] = []
                    manager = ETLManager()
                    debug_break_done = False

                    for spec in specs:
                        debug_break_done = _maybe_debug_breakpoint(
                            context, enabled_once=debug_break_done
                        )
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
            if ti is not None:
                ti.xcom_push(key="rows_transferred", value=aggregated.rows)
                ti.xcom_push(key="duration_seconds", value=aggregated.duration_seconds)
                ti.xcom_push(key="rows_per_second", value=aggregated.throughput)
                ti.xcom_push(key="retry_telemetry", value=retry_telemetry)

            summary = {
                "rows": aggregated.rows,
                "duration_seconds": aggregated.duration_seconds,
                "throughput": aggregated.throughput,
                "partitions_completed": aggregated.partitions_completed,
                "errors": aggregated.errors,
                "retry_telemetry": retry_telemetry,
            }
            _log_structured(
                level=logging.INFO,
                stage="airflow",
                message="Operator completed.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                rows=aggregated.rows,
                duration_seconds=aggregated.duration_seconds,
                throughput=aggregated.throughput,
                delivery_semantics="best_effort",
            )
            return summary
        except Exception as exc:
            norm = normalize_exception(exc)
            payload = error_payload(norm)
            payload["retry_telemetry"] = retry_telemetry
            _log_structured(
                level=logging.ERROR,
                stage="airflow",
                message="Operator failed.",
                task_group_id=self.task_group_id,
                source_db=source_db,
                target_db=target_db,
                error_type=payload.get("error_type"),
                error_message=payload.get("message"),
                retry_telemetry=retry_telemetry,
            )
            if ti is not None:
                ti.xcom_push(key="error_summary", value=payload)
                ti.xcom_push(key="retry_telemetry", value=retry_telemetry)
            raise norm from exc

    def _retry_telemetry(self, context: dict) -> dict:
        """Task retry bilgilerini context'ten normalize eder."""
        ti = (context or {}).get("ti")
        if ti is None:
            return {"try_number": None, "max_tries": None}
        try_number = getattr(ti, "try_number", None)
        max_tries = getattr(ti, "max_tries", None)
        return {"try_number": try_number, "max_tries": max_tries}
