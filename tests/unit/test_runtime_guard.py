"""F2.3-C — runtime guard seam: no-op in Community, fail-loud propagation.

The guard hook runs at task start (FFEngineOperator.execute) — never at
DAG parse. Entry-point discovery is lazy: importing the operator module
must not touch importlib.metadata.
"""

from __future__ import annotations

import pytest

from ffengine.core import runtime_guard
from ffengine.errors.exceptions import ConfigError


@pytest.fixture(autouse=True)
def _clean_guards():
    runtime_guard.clear_runtime_guards()
    yield
    runtime_guard.clear_runtime_guards()


def test_no_guards_is_noop():
    runtime_guard.run_runtime_guards({"dag_id": "d1"})  # must not raise


def test_registered_guard_receives_context():
    seen: list[dict] = []
    runtime_guard.register_runtime_guard(seen.append)
    context = {"dag_id": "d1", "dag_run_start_date": None}
    runtime_guard.run_runtime_guards(context)
    assert seen == [context]


def test_guard_exception_propagates_fail_loud():
    def guard(_context: dict) -> None:
        raise RuntimeError("license refused")

    runtime_guard.register_runtime_guard(guard)
    with pytest.raises(RuntimeError, match="license refused"):
        runtime_guard.run_runtime_guards({})


def test_same_guard_registered_once():
    seen: list[dict] = []

    def guard(context: dict) -> None:
        seen.append(context)

    runtime_guard.register_runtime_guard(guard)
    runtime_guard.register_runtime_guard(guard)  # idempotent
    runtime_guard.run_runtime_guards({})
    assert len(seen) == 1


def test_non_callable_guard_rejected():
    with pytest.raises(ConfigError):
        runtime_guard.register_runtime_guard("not-callable")  # type: ignore


def test_operator_import_does_not_trigger_discovery():
    """DAG parse safety: importing the operator must not load OUR entry
    points (Airflow's own entry-point reads during import are out of
    scope — the assertion targets the runtime_guard lazy flag directly)."""
    import importlib

    import ffengine.airflow.operator as operator_module

    runtime_guard.clear_runtime_guards()
    importlib.reload(operator_module)
    assert runtime_guard._ENTRY_POINTS_LOADED is False

    runtime_guard.run_runtime_guards({})  # first run performs discovery
    assert runtime_guard._ENTRY_POINTS_LOADED is True
