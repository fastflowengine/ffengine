"""C10_T01 — Exception modeli ve hiyerarşi testleri."""

from __future__ import annotations

import pytest

from ffengine.errors import (
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
    error_payload,
    http_status_for,
    normalize_exception,
)
from ffengine.errors.handler import sanitize_sql_preview


@pytest.mark.parametrize(
    "exc_cls, expected_code",
    [
        (ConfigError, "config_error"),
        (ValidationError, "validation_error"),
        (ConnectionError, "connection_error"),
        (DialectError, "dialect_error"),
        (MappingError, "mapping_error"),
        (EngineError, "engine_error"),
        (DeliveryPolicyError, "delivery_policy_error"),
        (CheckpointError, "checkpoint_error"),
        (PartitionError, "partition_error"),
    ],
)
def test_domain_exception_defaults(exc_cls, expected_code):
    exc = exc_cls("boom")
    assert isinstance(exc, FFEngineError)
    assert str(exc) == "boom"
    assert exc.code == expected_code
    assert exc.details == {}
    assert exc.cause is None


def test_ffengine_error_wrap_keeps_original_exception():
    original = ValueError("invalid payload")
    wrapped = ValidationError.wrap(original, details={"field": "source_type"})

    assert isinstance(wrapped, ValidationError)
    assert isinstance(wrapped, FFEngineError)
    assert str(wrapped) == "invalid payload"
    assert wrapped.details["field"] == "source_type"
    assert wrapped.cause is original


def test_ffengine_error_can_override_code_and_message():
    err = EngineError(
        "pipeline failed",
        code="engine_failure_custom",
        details={"task_group_id": "tg_orders"},
    )

    assert err.code == "engine_failure_custom"
    assert err.message == "pipeline failed"
    assert err.details["task_group_id"] == "tg_orders"


def test_normalize_exception_maps_builtin_types():
    assert isinstance(normalize_exception(ValueError("x")), ValidationError)
    assert isinstance(normalize_exception(TypeError("x")), ValidationError)
    assert isinstance(normalize_exception(KeyError("x")), ConfigError)
    assert isinstance(normalize_exception(RuntimeError("x")), EngineError)


def test_http_status_for_domain_and_builtin_errors():
    assert http_status_for(ValidationError("bad")) == 400
    assert http_status_for(ConnectionError("db down")) == 502
    assert http_status_for(RuntimeError("boom")) == 500


def test_error_payload_is_stable():
    payload = error_payload(MappingError("mapping failed", details={"mode": "mapping_file"}))
    assert payload["error_type"] == "MappingError"
    assert payload["error_code"] == "mapping_error"
    assert payload["message"] == "mapping failed"
    assert payload["details"]["mode"] == "mapping_file"


def test_error_payload_extracts_db_details_from_cause_chain():
    class FakePgError(Exception):
        __module__ = "psycopg.errors"
        sqlstate = "23505"

    wrapped = ConnectionError.wrap(
        FakePgError("duplicate key value violates unique constraint"),
        "db write failed",
        details={"target": '"dwh_stg"."t"'},
    )

    payload = error_payload(wrapped)
    assert payload["details"]["db_exception_type"] == "FakePgError"
    assert payload["details"]["db_sqlstate"] == "23505"
    assert payload["details"]["db_driver"] == "psycopg"
    assert "db_root_cause" in payload["details"]


def test_sanitize_sql_preview_masks_literals_and_truncates():
    raw = "SELECT * FROM t WHERE token = 'abc' AND user = 'xyz'"
    masked = sanitize_sql_preview(raw, max_len=40)
    assert masked is not None
    assert "'abc'" not in masked
    assert "'xyz'" not in masked
    assert "'?'" in masked
    assert masked.endswith("...")
