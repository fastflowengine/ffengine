"""
F2.1 — Bulk strategy provider registry (TAD v26.5 A4.2/A4.3/A4.6).

Native bulk API writers (PostgreSQL COPY, Oracle direct-path, ...) are
**Enterprise** providers registered here, keyed by ``(conn_type, bulk_api_method)``.
Community ships **no** providers, so any ``use_bulk_api=True`` fails loud
(developer-controlled, no silent fallback to ``executemany``).

Design constraints (review-corrected):
  * The dialect resolver (``ffengine.airflow.operator.resolve_dialect``) is
    **unchanged** — bulk is a separate *strategy* layer keyed by
    ``(conn_type, method)``, not a dialect override. ``use_bulk_api=False`` never
    touches this layer.
  * Duplicate ``(conn_type, method)`` registration is **fail-loud** — there is no
    silent "last registration wins".
  * Enterprise / customer provider packages are discovered via the
    ``"ffengine.bulk_providers"`` entry-point group, so a separate wheel (e.g. a
    customer MSSQL BCP provider) can register without touching core. Community
    does **not** import or name any Enterprise package.
"""

from __future__ import annotations

import importlib.metadata as _md
from typing import Optional

from ffengine.errors.exceptions import ConfigError

ENTRY_POINT_GROUP = "ffengine.bulk_providers"

# (conn_type, method) -> provider class
_REGISTRY: dict[tuple[str, str], type] = {}
_ENTRY_POINTS_LOADED = False


def _key(conn_type: str, method: str) -> tuple[str, str]:
    return (str(conn_type or "").strip().lower(), str(method or "").strip().lower())


def register_bulk_provider(conn_type: str, method: str, provider_cls: type) -> None:
    """Register a native bulk provider for ``(conn_type, bulk_api_method)``.

    Called at import time by Enterprise/customer provider packages (directly or
    via the ``ffengine.bulk_providers`` entry point). A second registration for
    the same ``(conn_type, method)`` with a different class is **rejected**
    (fail-loud, no "last-wins").
    """
    if not callable(provider_cls):
        raise ConfigError(
            f"Bulk provider callable/class olmalı: {provider_cls!r} "
            f"(conn_type={conn_type!r}, method={method!r})."
        )
    key = _key(conn_type, method)
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not provider_cls:
        raise ConfigError(
            "Bulk provider zaten kayıtlı: "
            f"conn_type={key[0]!r}, method={key[1]!r} -> "
            f"{existing.__module__}.{getattr(existing, '__qualname__', existing)}. "
            "Aynı (conn_type, method) için ikinci kayıt reddedilir (son-kazanır yok)."
        )
    _REGISTRY[key] = provider_cls


def _load_entry_points() -> None:
    """Lazily discover provider packages via the entry-point group (once)."""
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    for ep in _md.entry_points(group=ENTRY_POINT_GROUP):
        # An entry point resolves to a zero-arg callable that performs its own
        # register_bulk_provider(...) calls. Failures are fail-loud (a broken
        # provider must not silently disable bulk).
        ep.load()()


def get_bulk_provider(conn_type: str, method: str) -> Optional[type]:
    """Return the provider class for ``(conn_type, method)`` or ``None``."""
    _load_entry_points()
    return _REGISTRY.get(_key(conn_type, method))


def is_bulk_supported(conn_type: str, method: str) -> bool:
    """True iff a provider is registered for this connection type + method."""
    return get_bulk_provider(conn_type, method) is not None


def valid_methods_for(conn_type: str) -> set[str]:
    """Registered bulk methods available for a given connection type."""
    _load_entry_points()
    ct = str(conn_type or "").strip().lower()
    return {m for (c, m) in _REGISTRY if c == ct}


def registered_methods() -> set[str]:
    """All registered bulk method names (capability-driven; no static enum)."""
    _load_entry_points()
    return {m for (_c, m) in _REGISTRY}


def clear_bulk_providers() -> None:
    """Reset the registry (test/dev use only)."""
    global _ENTRY_POINTS_LOADED
    _REGISTRY.clear()
    _ENTRY_POINTS_LOADED = False
