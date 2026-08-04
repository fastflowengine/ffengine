"""
F3.2 — Task-type provider registry (Community extension seam).

Extension task types whose runners live OUTSIDE Community (the first one is
``dbt``, an **Enterprise** feature per FAD v26.3 / EX-D012) plug in here.
Community owns the seam only: the enum surface, the config contract and the
generated-DAG dispatch. The provider — registered by an Enterprise/customer
package — is the sole owner of operator construction and task runtime.

Model mirrors the F2.1 bulk-provider registry (``ffengine.bulk_providers``):
  * Keyed by normalized ``task_type``.
  * Duplicate registration with a different provider is **fail-loud**
    (no silent "last-wins").
  * Provider packages are discovered lazily via the
    ``"ffengine.task_type_providers"`` entry-point group — Community never
    imports or names an Enterprise package, and DAG parse performs no
    file/network I/O for absent providers (lookup is in-process package
    metadata, same as the bulk registry).

Provider calling convention (duck-typed; enforced by the factory, not here):
``provider(task_id=..., task_def=..., source_conn_id=..., target_conn_id=...,
binding_sources=...) -> Airflow operator/task``.
"""

from __future__ import annotations

import importlib.metadata as _md
from typing import Callable, Optional

from ffengine.errors.exceptions import ConfigError

ENTRY_POINT_GROUP = "ffengine.task_type_providers"

_REGISTRY: dict[str, Callable] = {}
_ENTRY_POINTS_LOADED = False


def _key(task_type: str) -> str:
    return str(task_type or "").strip().lower()


def register_task_type_provider(task_type: str, provider: Callable) -> None:
    """Register an operator provider for an extension ``task_type``.

    Called at import time by Enterprise/customer packages (directly or via
    the ``ffengine.task_type_providers`` entry point). A second registration
    for the same task_type with a different provider is **rejected**
    (fail-loud, no "last-wins").
    """
    key = _key(task_type)
    if not key:
        raise ConfigError(
            "task_type provider kaydı için boş task_type verilemez."
        )
    if not callable(provider):
        raise ConfigError(
            f"Task-type provider callable olmalı: {provider!r} "
            f"(task_type={task_type!r})."
        )
    existing = _REGISTRY.get(key)
    if existing is not None and existing is not provider:
        raise ConfigError(
            f"Task-type provider zaten kayıtlı: task_type={key!r} -> "
            f"{existing.__module__}."
            f"{getattr(existing, '__qualname__', existing)}. "
            "Aynı task_type için ikinci kayıt reddedilir (son-kazanır yok)."
        )
    _REGISTRY[key] = provider


def _load_entry_points() -> None:
    """Lazily discover provider packages via the entry-point group (once)."""
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    for ep in _md.entry_points(group=ENTRY_POINT_GROUP):
        # An entry point resolves to a zero-arg callable that performs its
        # own register_task_type_provider(...) calls. Failures are fail-loud
        # (a broken provider must not silently disable its task type).
        ep.load()()


def get_task_type_provider(task_type: str) -> Optional[Callable]:
    """Return the provider for ``task_type`` or ``None`` when unregistered."""
    _load_entry_points()
    return _REGISTRY.get(_key(task_type))


def has_task_type_provider(task_type: str) -> bool:
    """True iff a provider is registered for this task type."""
    return get_task_type_provider(task_type) is not None


def clear_task_type_providers() -> None:
    """Reset the registry (test/dev use only)."""
    global _ENTRY_POINTS_LOADED
    _REGISTRY.clear()
    _ENTRY_POINTS_LOADED = False
