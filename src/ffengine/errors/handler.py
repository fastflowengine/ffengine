"""C10 — Merkezi hata normalizasyonu ve sınıflandırma yardımcıları."""

from __future__ import annotations

from typing import Any

from ffengine.errors.exceptions import (
    CheckpointError,
    ConfigError,
    ConnectionError,
    DeliveryPolicyError,
    DialectError,
    EngineError,
    FFEngineError,
    MappingError,
    PartitionError,
    ValidationError,
)

_HTTP_STATUS_MAP: dict[type[FFEngineError], int] = {
    ConfigError: 400,
    ValidationError: 400,
    MappingError: 400,
    PartitionError: 400,
    ConnectionError: 502,
    DialectError: 500,
    CheckpointError: 500,
    DeliveryPolicyError: 500,
    EngineError: 500,
}


def normalize_exception(exc: Exception) -> FFEngineError:
    """
    Çıplak exception'ları FFEngine domain exception'ına çevir.

    Kurallar:
    - FFEngineError ise aynen döner.
    - ValueError/TypeError -> ValidationError
    - KeyError -> ConfigError
    - Diğer tümü -> EngineError
    """
    if isinstance(exc, FFEngineError):
        return exc
    if isinstance(exc, (ValueError, TypeError)):
        return ValidationError.wrap(exc)
    if isinstance(exc, KeyError):
        return ConfigError.wrap(exc)
    return EngineError.wrap(exc)


def http_status_for(exc: Exception, default: int = 500) -> int:
    """Exception tipine göre önerilen HTTP status kodunu döner."""
    norm = normalize_exception(exc)
    for cls, status in _HTTP_STATUS_MAP.items():
        if isinstance(norm, cls):
            return status
    return default


def error_payload(exc: Exception) -> dict[str, Any]:
    """Log/API katmanı için standart hata payload'ı üretir."""
    norm = normalize_exception(exc)
    return {
        "error_type": type(norm).__name__,
        "error_code": norm.code,
        "message": norm.message,
        "details": norm.details,
    }
