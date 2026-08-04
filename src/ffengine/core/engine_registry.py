"""
F2.3 — Engine provider registry (capability-based detection).

Enterprise engines (PipelineEngine, SparkEngine — implementations arrive
with F4.x) register here by canonical name (``"pipeline"``, ``"spark"``),
directly or via the ``"ffengine.engine_providers"`` entry-point group.
Community ships **no** providers: ``standard``/``auto`` resolve to
StandardEngine and an explicit ``pipeline``/``spark`` fails loud.

Mirrors the F2.1 bulk-provider registry constraints: duplicate
registration is fail-loud (no "last wins") and Community never imports or
names any Enterprise package.
"""

from __future__ import annotations

import importlib.metadata as _md
from typing import Optional

from ffengine.errors.exceptions import ConfigError

ENTRY_POINT_GROUP = "ffengine.engine_providers"

_REGISTRY: dict[str, type] = {}
_ENTRY_POINTS_LOADED = False


def _key(name: str) -> str:
    return str(name or "").strip().lower()


def register_engine_provider(name: str, provider_cls: type) -> None:
    """Register an engine provider class under a canonical engine name."""
    if not callable(provider_cls):
        raise ConfigError(
            f"Engine provider callable/class olmalı: {provider_cls!r} "
            f"(name={name!r})."
        )
    key = _key(name)
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not provider_cls:
        raise ConfigError(
            f"Engine provider zaten kayıtlı: {key!r} -> "
            f"{existing.__module__}.{getattr(existing, '__qualname__', existing)}. "
            "Aynı isim için ikinci kayıt reddedilir (son-kazanır yok)."
        )
    _REGISTRY[key] = provider_cls


def _load_entry_points() -> None:
    """Lazily discover provider packages via the entry-point group (once)."""
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    for ep in _md.entry_points(group=ENTRY_POINT_GROUP):
        # Resolves to a zero-arg callable performing its own
        # register_engine_provider(...) calls; failures are fail-loud.
        ep.load()()


def get_engine_provider(name: str) -> Optional[type]:
    """Return the provider class registered under ``name`` or ``None``."""
    _load_entry_points()
    return _REGISTRY.get(_key(name))


def registered_engines() -> set[str]:
    _load_entry_points()
    return set(_REGISTRY)


def clear_engine_providers() -> None:
    """Reset the registry (test/dev use only)."""
    global _ENTRY_POINTS_LOADED
    _REGISTRY.clear()
    _ENTRY_POINTS_LOADED = False
