"""Explicit source-mount extension bootstrap for local development."""

from __future__ import annotations

import importlib
import os

from ffengine.errors.exceptions import ConfigError

BOOTSTRAP_ENV = "FFENGINE_EXTENSION_BOOTSTRAPS"
_loaded: set[str] = set()


def load_extension_bootstraps() -> None:
    """Load trusted ``module:callable`` targets once per process."""
    targets = (item.strip() for item in os.getenv(BOOTSTRAP_ENV, "").split(","))
    for target in filter(None, targets):
        if target in _loaded:
            continue
        module_name, separator, attr = target.partition(":")
        if not separator or not module_name or not attr:
            raise ConfigError(
                f"Invalid {BOOTSTRAP_ENV} target {target!r}; expected "
                "'module:callable'."
            )
        callback = getattr(importlib.import_module(module_name), attr, None)
        if not callable(callback):
            raise ConfigError(
                f"Invalid {BOOTSTRAP_ENV} target {target!r}: callable not found."
            )
        callback()
        _loaded.add(target)


def clear_extension_bootstraps() -> None:
    """Reset process-local state (test/dev only)."""
    _loaded.clear()
