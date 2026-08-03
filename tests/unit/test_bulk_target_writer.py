"""
F2.1 — BulkTargetWriter orchestrator + factory + effective-M.

Community ships no bulk provider, so tests register a fake provider to exercise
the stateful lifecycle and the fail-loud guards. Maps to:
  * T-F2.1-2 (Oracle M>1 -> fail-loud): effective-M guard mechanism.
  * T-F2.1-3 (bulk off -> unchanged): factory returns the plain TargetWriter.
  * T-F2.1-4 (unsupported target -> fail-loud): no provider / bad load_method.
"""

import pytest

from ffengine.core.flow_manager import build_target_writer
from ffengine.errors.exceptions import ConfigError
from ffengine.pipeline import bulk_registry as reg
from ffengine.pipeline.bulk_target_writer import (
    BulkTargetWriter,
    build_bulk_target_writer,
    conn_type_of,
    resolve_effective_writers,
)
from ffengine.pipeline.target_writer import TargetWriter


class PostgresDialect:
    pass


class OracleDialect:
    pass


class _FakeProvider:
    MAX_WRITERS = 1
    DEFAULT_WRITERS = 1
    SUPPORTED_LOAD_METHODS = frozenset({"append", "replace"})

    def __init__(self, session, dialect, task_config, effective_writers):
        self.session = session
        self.dialect = dialect
        self.effective_writers = effective_writers
        self.calls = []

    def open(self):
        self.calls.append("open")

    def write_batch(self, rows, task_config):
        self.calls.append(("write", len(rows)))
        return len(rows)

    def finalize(self):
        self.calls.append("finalize")

    def abort(self):
        self.calls.append("abort")


@pytest.fixture(autouse=True)
def _clean_registry():
    reg.clear_bulk_providers()
    yield
    reg.clear_bulk_providers()


# ---------------------------------------------------------------------------
# conn_type_of
# ---------------------------------------------------------------------------


def test_conn_type_of_strips_dialect_suffix():
    assert conn_type_of(PostgresDialect()) == "postgres"
    assert conn_type_of(OracleDialect()) == "oracle"


# ---------------------------------------------------------------------------
# Factory (flow_manager.build_target_writer) — T-F2.1-3 backward-compat
# ---------------------------------------------------------------------------


def test_factory_off_returns_plain_target_writer():
    writer = build_target_writer(object(), PostgresDialect(), {})
    assert isinstance(writer, TargetWriter)
    assert not isinstance(writer, BulkTargetWriter)


def test_factory_explicit_false_returns_plain_target_writer():
    writer = build_target_writer(object(), PostgresDialect(), {"use_bulk_api": False})
    assert isinstance(writer, TargetWriter)


def test_factory_on_registered_returns_bulk_writer():
    reg.register_bulk_provider("postgres", "postgres_copy", _FakeProvider)
    writer = build_target_writer(
        object(),
        PostgresDialect(),
        {"use_bulk_api": True, "bulk_api_method": "postgres_copy"},
    )
    assert isinstance(writer, BulkTargetWriter)


# ---------------------------------------------------------------------------
# build_bulk_target_writer fail-loud — T-F2.1-4
# ---------------------------------------------------------------------------


def test_build_missing_method_is_fail_loud():
    with pytest.raises(ConfigError, match="bulk_api_method zorunludur"):
        build_bulk_target_writer(object(), PostgresDialect(), {"use_bulk_api": True})


def test_build_unregistered_target_is_fail_loud():
    # No provider registered for (postgres, postgres_copy) -> no silent fallback.
    with pytest.raises(ConfigError, match="desteklenmiyor"):
        build_bulk_target_writer(
            object(),
            PostgresDialect(),
            {"use_bulk_api": True, "bulk_api_method": "postgres_copy"},
        )


# ---------------------------------------------------------------------------
# Lifecycle: prepare(open) -> write_batch -> finalize / rollback(abort)
# ---------------------------------------------------------------------------


def _writer_for(load_method="append", **cfg):
    reg.register_bulk_provider("postgres", "postgres_copy", _FakeProvider)
    task = {"use_bulk_api": True, "bulk_api_method": "postgres_copy",
            "load_method": load_method, **cfg}
    writer = build_bulk_target_writer(object(), PostgresDialect(), task)
    return writer, task


def test_lifecycle_prepare_write_finalize():
    writer, task = _writer_for()
    writer.prepare(task)
    assert writer._provider.calls == ["open"]
    assert writer.write_batch([(1,), (2,)], task) == 2
    writer.finalize()
    assert writer._provider.calls == ["open", ("write", 2), "finalize"]


def test_write_batch_empty_returns_zero_without_provider_call():
    writer, task = _writer_for()
    writer.prepare(task)
    assert writer.write_batch([], task) == 0
    assert writer._provider.calls == ["open"]


def test_write_batch_before_prepare_is_fail_loud():
    writer, task = _writer_for()
    with pytest.raises(ConfigError, match="prepare"):
        writer.write_batch([(1,)], task)


def test_rollback_calls_provider_abort():
    writer, task = _writer_for()
    writer.prepare(task)
    writer.rollback_batch()
    assert writer._provider.calls == ["open", "abort"]


def test_prepare_unsupported_load_method_is_fail_loud():
    # provider supports append/replace only; upsert -> reject before any write.
    writer, task = _writer_for(load_method="upsert")
    with pytest.raises(ConfigError, match="load_method"):
        writer.prepare(task)


# ---------------------------------------------------------------------------
# Effective-M (D-G) — T-F2.1-2 mechanism (Oracle direct-path / PG COPY = M=1)
# ---------------------------------------------------------------------------


def test_effective_writers_auto_uses_capability_default():
    # writer_workers None (auto) -> provider DEFAULT_WRITERS, never the legacy 5.
    assert resolve_effective_writers(
        {"writer_workers": None}, max_writers=1, default_writers=1, method="m"
    ) == 1


def test_effective_writers_explicit_within_max():
    assert resolve_effective_writers(
        {"writer_workers": 3}, max_writers=4, default_writers=1, method="m"
    ) == 3


def test_effective_writers_over_max_is_fail_loud():
    # Explicit M > provider max (e.g. Oracle direct-path exclusive lock, max=1).
    with pytest.raises(ConfigError, match="desteklenmiyor"):
        resolve_effective_writers(
            {"writer_workers": 2}, max_writers=1, default_writers=1, method="oracle_dp"
        )


def test_effective_writers_invalid_type_is_fail_loud():
    with pytest.raises(ConfigError, match="tam sayi"):
        resolve_effective_writers(
            {"writer_workers": True}, max_writers=4, default_writers=1, method="m"
        )


def test_prepare_over_max_writers_is_fail_loud_end_to_end():
    # Oracle-like provider (MAX_WRITERS=1) + explicit writer_workers=2 -> fail-loud
    # through the full build+prepare path (T-F2.1-2 mechanism, Community-side).
    reg.register_bulk_provider("oracle", "oracle_direct_path", _FakeProvider)
    task = {
        "use_bulk_api": True,
        "bulk_api_method": "oracle_direct_path",
        "load_method": "append",
        "writer_workers": 2,
    }
    writer = build_bulk_target_writer(object(), OracleDialect(), task)
    with pytest.raises(ConfigError, match="desteklenmiyor"):
        writer.prepare(task)
