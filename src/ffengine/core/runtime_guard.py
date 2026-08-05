"""
F2.3 — Runtime guard seam.

Guards run at **task start** (``FFEngineOperator.execute``) before any
real work — never at DAG parse. Community registers no guards, so the
hook is a no-op here; Enterprise registers its license guard via the
``"ffengine.runtime_guards"`` entry-point group. A guard that raises
stops the task fail-loud.

Contract (F2.3): the guard context carries run metadata only
(``dag_id``, ``task_id``, ``run_id``, ``dag_run_start_date``); guards
must not reach into DB/LDAP/network on this path.
"""

from __future__ import annotations

import importlib.metadata as _md
from typing import Callable

from ffengine.errors.exceptions import ConfigError

ENTRY_POINT_GROUP = "ffengine.runtime_guards"

_GUARDS: list[Callable[[dict], None]] = []
_ENTRY_POINTS_LOADED = False


def register_runtime_guard(guard: Callable[[dict], None]) -> None:
    """Register a guard callable; registering the same one is idempotent."""
    if not callable(guard):
        raise ConfigError(f"Runtime guard callable olmalı: {guard!r}.")
    if guard not in _GUARDS:
        _GUARDS.append(guard)


def _load_entry_points() -> None:
    global _ENTRY_POINTS_LOADED
    if _ENTRY_POINTS_LOADED:
        return
    _ENTRY_POINTS_LOADED = True
    from ffengine.core.extension_bootstrap import load_extension_bootstraps

    load_extension_bootstraps()
    for ep in _md.entry_points(group=ENTRY_POINT_GROUP):
        # Resolves to a zero-arg callable performing its own
        # register_runtime_guard(...) calls; failures are fail-loud.
        ep.load()()


def run_runtime_guards(context: dict) -> None:
    """Run all registered guards; exceptions propagate (fail-loud)."""
    _load_entry_points()
    for guard in list(_GUARDS):
        guard(context)


def clear_runtime_guards() -> None:
    """Reset the guard list (test/dev use only)."""
    global _ENTRY_POINTS_LOADED
    _GUARDS.clear()
    _ENTRY_POINTS_LOADED = False
