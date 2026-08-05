"""Source-mount extension bootstraps are explicit and fail-loud."""

import sys
import types

import pytest

from ffengine.core import extension_bootstrap as bootstrap
from ffengine.errors.exceptions import ConfigError


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    bootstrap.clear_extension_bootstraps()
    monkeypatch.delenv(bootstrap.BOOTSTRAP_ENV, raising=False)
    yield
    bootstrap.clear_extension_bootstraps()


def test_source_mount_bootstrap_runs_once(monkeypatch):
    calls = []
    module = types.ModuleType("test_ffengine_extension")
    module.register = lambda: calls.append("registered")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv(
        bootstrap.BOOTSTRAP_ENV, "test_ffengine_extension:register"
    )
    bootstrap.load_extension_bootstraps()
    bootstrap.load_extension_bootstraps()
    assert calls == ["registered"]


def test_invalid_source_mount_bootstrap_fails_loud(monkeypatch):
    monkeypatch.setenv(bootstrap.BOOTSTRAP_ENV, "missing-separator")
    with pytest.raises(ConfigError, match="module:callable"):
        bootstrap.load_extension_bootstraps()
