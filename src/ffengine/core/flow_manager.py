"""
FlowManager + PythonEngine — Community pipeline orkestratörü.

PythonEngine, BaseEngine kontratını implement eder:
  - is_available() → True (her zaman)
  - run(config_path, task_group_id) → FlowResult

FlowManager, tek bir task_config dict'i alarak pipeline'ı çalıştırır
ve partitionlı senaryolarda çağrılabilir.
"""

import logging
import time
from datetime import UTC, datetime

from ffengine.core.base_engine import BaseEngine, FlowResult
from ffengine.errors import FFEngineError, normalize_exception
from ffengine.pipeline.source_reader import SourceReader
from ffengine.pipeline.streamer import Streamer
from ffengine.pipeline.target_writer import TargetWriter
from ffengine.pipeline.transformer import Transformer

_log = logging.getLogger(__name__)


def _dialect_name(dialect) -> str:
    raw = type(dialect).__name__.lower()
    return raw.replace("dialect", "") or raw


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


class FlowManager:
    """
    Tek bir Flow task'ını çalıştırır.

    Doğrudan task_config dict kabul eder; config yükleme sorumluluğu
    çağırana aittir (C05 tamamlandığında PythonEngine._load_task bağlar).
    """

    def run_flow_task(
        self,
        src_session,
        tgt_session,
        src_dialect,
        tgt_dialect,
        task_config: dict,
        partition_spec: dict | None = None,
        skip_prepare: bool = False,
    ) -> FlowResult:
        """
        Parameters
        ----------
        src_session    : Kaynak DBSession (açık).
        tgt_session    : Hedef DBSession (açık).
        src_dialect    : Kaynak BaseDialect implementasyonu.
        tgt_dialect    : Hedef BaseDialect implementasyonu.
        task_config    : Flow görev konfigürasyonu.
        partition_spec : {"part_id": int, "where": str | None} veya None.

        Returns
        -------
        FlowResult
        """
        start = time.monotonic()
        task_group_id = str(task_config.get("task_group_id") or "")
        source_db = _dialect_name(src_dialect)
        target_db = _dialect_name(tgt_dialect)
        _log_structured(
            level=logging.INFO,
            stage="extract",
            message="Flow task started.",
            task_group_id=task_group_id,
            source_db=source_db,
            target_db=target_db,
            partition_id=(partition_spec or {}).get("part_id"),
        )

        # Partition spec varsa WHERE koşulunu config'e ekle
        effective_config = dict(task_config)
        if partition_spec and partition_spec.get("where"):
            effective_config["_resolved_where"] = partition_spec["where"]

        reader = SourceReader(src_session, effective_config, src_dialect)
        writer = TargetWriter(tgt_session, tgt_dialect)
        transformer = Transformer()
        streamer = Streamer()

        if not skip_prepare:
            writer.prepare(effective_config)

        # Streamer hata anında rollback_batch() çağırır ve exception'ı
        # yeniden fırlatır — burada tekrar rollback çağırmıyoruz.
        try:
            result = streamer.stream(
                reader.read(),
                writer=writer,
                transformer=transformer,
                task_config=effective_config,
            )
        except Exception as exc:
            norm = normalize_exception(exc)
            if isinstance(norm, FFEngineError):
                norm.details.setdefault(
                    "task_group_id",
                    str(task_config.get("task_group_id") or ""),
                )
                if partition_spec:
                    norm.details.setdefault("partition_spec", dict(partition_spec))
            _log_structured(
                level=logging.ERROR,
                stage="load",
                message="Flow task failed.",
                task_group_id=task_group_id,
                source_db=source_db,
                target_db=target_db,
                rows=0,
                error_type=type(norm).__name__,
                error_message=str(norm),
                partition_id=(partition_spec or {}).get("part_id"),
            )
            raise norm from exc

        elapsed = time.monotonic() - start
        rows = result["rows"]
        throughput = rows / elapsed if elapsed > 0 else 0.0
        _log_structured(
            level=logging.INFO,
            stage="load",
            message="Flow task completed.",
            task_group_id=task_group_id,
            source_db=source_db,
            target_db=target_db,
            rows=rows,
            duration_seconds=elapsed,
            throughput=round(throughput, 2),
            partition_id=(partition_spec or {}).get("part_id"),
        )

        return FlowResult(
            rows=rows,
            duration_seconds=round(elapsed, 3),
            throughput=round(throughput, 2),
            partitions_completed=1,
            errors=[],
        )


class PythonEngine(BaseEngine):
    """
    Community Python Engine — BaseEngine implementasyonu.

    run() C05 tamamlandığında config_path'ten task_config yükler.
    Şu an için doğrudan task_config dict ile FlowManager'a devreder.
    """

    def is_available(self) -> bool:
        return True

    def run(self, config_path: str, task_group_id: str) -> FlowResult:
        """
        config_path'ten task konfigürasyonunu yükle ve pipeline'ı çalıştır.

        C05 (YAML Config) tamamlandığında gerçek loader buraya bağlanır.
        Şu an config_path ve task_group_id birer stub'dır.
        """
        task_config = self._load_task(config_path, task_group_id)

        src_session = task_config.pop("_src_session", None)
        tgt_session = task_config.pop("_tgt_session", None)
        src_dialect = task_config.pop("_src_dialect", None)
        tgt_dialect = task_config.pop("_tgt_dialect", None)

        if any(v is None for v in (src_session, tgt_session, src_dialect, tgt_dialect)):
            from ffengine.errors.exceptions import ConfigError
            raise ConfigError(
                "PythonEngine.run() için session/dialect enjeksiyonu gerekli: "
                "_src_session, _tgt_session, _src_dialect, _tgt_dialect "
                "task_config içinde bulunmalıdır (C07 AirflowDBAdapter tarafından sağlanır)."
            )

        manager = FlowManager()
        return manager.run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=src_dialect,
            tgt_dialect=tgt_dialect,
            task_config=task_config,
        )

    def _load_task(self, config_path: str, task_group_id: str) -> dict:
        """
        Config dosyasını yükle ve normalize edilmiş task dict döndür.

        C05 (YAML Config) tamamlandığında aktif edildi.
        """
        from ffengine.config.loader import ConfigLoader

        return ConfigLoader().load(config_path, task_group_id)
