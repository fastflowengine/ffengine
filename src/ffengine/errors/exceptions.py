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
