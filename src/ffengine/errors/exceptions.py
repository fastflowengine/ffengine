"""
FFEngine exception hiyerarşisi.

EXCEPTION_MODEL.md'deki domain exception sözleşmesini uygular.
"""

from __future__ import annotations

from typing import Any


class FFEngineError(Exception):
    """Tüm FFEngine domain exception'larının taban sınıfı."""

    default_code = "ffengine_error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
        code: str | None = None,
    ) -> None:
        self.message = message or self.__class__.__name__
        self.details = details or {}
        self.cause = cause
        self.code = code or self.default_code
        super().__init__(self.message)

    @classmethod
    def wrap(
        cls,
        exc: Exception,
        message: str | None = None,
        *,
        details: dict[str, Any] | None = None,
    ) -> "FFEngineError":
        """Dış exception'ı domain exception'a sarar."""
        return cls(message or str(exc), details=details, cause=exc)


class ConfigError(FFEngineError):
    """Config yükleme / parse hatası."""

    default_code = "config_error"


class ValidationError(FFEngineError):
    """Config doğrulama hatası (whitelist, zorunlu alan, koşullu kural)."""

    default_code = "validation_error"


class ConnectionError(FFEngineError):
    """Veritabanı bağlantı hatası."""

    default_code = "connection_error"


class DialectError(FFEngineError):
    """Dialect işlem hatası."""

    default_code = "dialect_error"


class MappingError(FFEngineError):
    """Kolon mapping hatası."""

    default_code = "mapping_error"


class EngineError(FFEngineError):
    """Pipeline çalışma zamanı hatası."""

    default_code = "engine_error"


class DeliveryPolicyError(FFEngineError):
    """Delivery semantics ihlali."""

    default_code = "delivery_policy_error"


class CheckpointError(FFEngineError):
    """Checkpoint okuma/yazma hatası."""

    default_code = "checkpoint_error"


class PartitionError(FFEngineError):
    """Partition planlama hatası (eksik kolon, geçersiz mod, boş aralık)."""

    default_code = "partition_error"


class ReconciliationError(FFEngineError):
    """Aktarım muhasebesi denkliği tutmadı (F3.3 K1).

    ``rows_read != rows_written + rows_rejected``. Partition fail-loud olur;
    sessiz düzeltme veya sonraki partition ile mahsuplaşma YOKTUR. Details
    yalnız sayaç/kimlik taşır — veri satırı ya da SQL değeri içermez.
    """

    default_code = "reconciliation_error"


class ReconciliationUnavailableError(ReconciliationError):
    """Otoritatif sayaç yokken muhasebe garantisi istendi (F4.3E).

    ``counter_source = unavailable`` + ``require_reconciliation = true``
    birleşimi sessiz garanti düşürme olurdu; task **başında**, kaynağa hiç
    bağlanılmadan fail-loud olunur. Bilinçli vazgeçiş yalnız kök config'te
    ``require_reconciliation: false`` ile yapılır (sayaçlar açık ``null``,
    ``reconciliation_status = not_applicable``).
    """

    default_code = "reconciliation_unavailable"


class FileSourceError(FFEngineError):
    """Dosya kaynağı okuma/parse hatası (F1.4 — bozuk satır, nested, vb.)."""

    default_code = "file_source_error"


class FileTransportError(FFEngineError):
    """Dosya transport hatası (F1.5 — SFTP/local stream/rename)."""

    default_code = "file_transport_error"


class FileTargetError(FFEngineError):
    """Dosya hedefi yazma hatası (F1.5 — kolon/M=1/atomiklik)."""

    default_code = "file_target_error"
