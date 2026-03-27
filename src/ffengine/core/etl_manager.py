"""
ETLManager + PythonEngine — Community pipeline orkestratörü.

PythonEngine, BaseEngine kontratını implement eder:
  - is_available() → True (her zaman)
  - run(config_path, task_group_id) → ETLResult

ETLManager, tek bir task_config dict'i alarak pipeline'ı çalıştırır
ve partitionlı senaryolarda çağrılabilir.
"""

import time

from ffengine.core.base_engine import BaseEngine, ETLResult
from ffengine.pipeline.source_reader import SourceReader
from ffengine.pipeline.streamer import Streamer
from ffengine.pipeline.target_writer import TargetWriter
from ffengine.pipeline.transformer import Transformer


class ETLManager:
    """
    Tek bir ETL task'ını çalıştırır.

    Doğrudan task_config dict kabul eder; config yükleme sorumluluğu
    çağırana aittir (C05 tamamlandığında PythonEngine._load_task bağlar).
    """

    def run_etl_task(
        self,
        src_session,
        tgt_session,
        src_dialect,
        tgt_dialect,
        task_config: dict,
        partition_spec: dict | None = None,
        skip_prepare: bool = False,
    ) -> ETLResult:
        """
        Parameters
        ----------
        src_session    : Kaynak DBSession (açık).
        tgt_session    : Hedef DBSession (açık).
        src_dialect    : Kaynak BaseDialect implementasyonu.
        tgt_dialect    : Hedef BaseDialect implementasyonu.
        task_config    : ETL görev konfigürasyonu.
        partition_spec : {"part_id": int, "where": str | None} veya None.

        Returns
        -------
        ETLResult
        """
        start = time.monotonic()

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
        result = streamer.stream(
            reader.read(),
            writer=writer,
            transformer=transformer,
            task_config=effective_config,
        )

        elapsed = time.monotonic() - start
        rows = result["rows"]
        throughput = rows / elapsed if elapsed > 0 else 0.0

        return ETLResult(
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
    Şu an için doğrudan task_config dict ile ETLManager'a devreder.
    """

    def is_available(self) -> bool:
        return True

    def run(self, config_path: str, task_group_id: str) -> ETLResult:
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

        manager = ETLManager()
        return manager.run_etl_task(
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
