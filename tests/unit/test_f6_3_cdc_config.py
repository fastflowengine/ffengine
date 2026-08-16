"""F6.3 kafka/CDC config contract tests (T-F6.3-1/-2/-3/-4/-5/-22 config yarisi).

EX-D036: `cdc_start_policy` ACIK secimdir (sessiz default yok); broker
adresi/kimligi yalniz Airflow Connection'da yasar; teslim garantisi
at-least-once consumption + effectively-once target effects.
"""

import pytest

from ffengine.config.schema import VALID_CDC_START_POLICIES
from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ValidationError


def _task(**overrides):
    task = {
        "task_group_id": "cdc_orders",
        "source_type": "kafka",
        "kafka_topic": "pg.public.orders",
        "cdc_start_policy": "earliest",
        "target_schema": "ods",
        "target_table": "orders",
        "target_type": "db",
        "load_method": "cdc_apply",
        "upsert_match_columns": ["order_id"],
    }
    task.update(overrides)
    return task


def _validate(monkeypatch, task):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    ConfigValidator().validate(task)


# ---------------------------------------------------------------- T-F6.3-1

def test_start_policy_is_required_no_silent_default(monkeypatch):
    task = _task()
    task.pop("cdc_start_policy")
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, task)
    message = str(excinfo.value)
    assert "cdc_start_policy" in message
    for valid in sorted(VALID_CDC_START_POLICIES):
        assert valid in message
    # Mesaj watermark anlamini SOYLEMELI: earliest != offset 0.
    assert "watermark" in message.lower()


def test_unknown_start_policy_is_fail_loud(monkeypatch):
    with pytest.raises(ValidationError, match="cdc_start_policy"):
        _validate(monkeypatch, _task(cdc_start_policy="from_beginning"))


@pytest.mark.parametrize("policy", ["earliest", "latest"])
def test_supported_simple_policies_pass_and_normalise(policy, monkeypatch):
    task = _task(cdc_start_policy=f"  {policy.upper()}  ")
    _validate(monkeypatch, task)
    assert task["cdc_start_policy"] == policy


def test_explicit_policy_requires_full_offsets_map(monkeypatch):
    with pytest.raises(ValidationError, match="cdc_start_offsets"):
        _validate(monkeypatch, _task(cdc_start_policy="explicit"))


def test_explicit_policy_normalises_offsets(monkeypatch):
    task = _task(
        cdc_start_policy="explicit",
        cdc_start_offsets={"0": "12", 1: 0},
    )
    _validate(monkeypatch, task)
    assert task["cdc_start_offsets"] == {0: 12, 1: 0}


def test_explicit_offsets_reject_negative_and_non_int(monkeypatch):
    with pytest.raises(ValidationError):
        _validate(
            monkeypatch,
            _task(cdc_start_policy="explicit", cdc_start_offsets={0: -1}),
        )
    with pytest.raises(ValidationError):
        _validate(
            monkeypatch,
            _task(cdc_start_policy="explicit", cdc_start_offsets={0: "abc"}),
        )


def test_offsets_without_explicit_policy_are_stray(monkeypatch):
    with pytest.raises(ValidationError, match="explicit"):
        _validate(
            monkeypatch,
            _task(cdc_start_policy="earliest", cdc_start_offsets={0: 5}),
        )


# ---------------------------------------------------------------- T-F6.3-2

def test_kafka_requires_cdc_apply(monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, _task(load_method="append"))
    # Mesaj dogru alternatife yonlendirmeli ve gerekceyi tasimali.
    assert "cdc_apply" in str(excinfo.value)


def test_cdc_apply_requires_kafka_source(monkeypatch):
    task = _task(
        source_type="table",
        source_schema="public",
        source_table="orders",
    )
    task.pop("kafka_topic")
    task.pop("cdc_start_policy")
    with pytest.raises(ValidationError, match="kafka"):
        _validate(monkeypatch, task)


def test_cdc_fields_are_stray_outside_kafka(monkeypatch):
    task = _task(
        source_type="table",
        source_schema="public",
        source_table="orders",
        load_method="append",
    )
    # kafka_topic/cdc_start_policy hala duruyor -> stray, sessiz yok sayma YOK.
    with pytest.raises(ValidationError, match="kafka"):
        _validate(monkeypatch, task)


# ---------------------------------------------------------------- T-F6.3-3

def test_match_columns_required_for_cdc_apply(monkeypatch):
    with pytest.raises(ValidationError, match="upsert_match_columns"):
        _validate(monkeypatch, _task(upsert_match_columns=None))


# ---------------------------------------------------------------- T-F6.3-4

def test_secret_bearing_field_is_refused_key_name_scan(monkeypatch):
    with pytest.raises(ValidationError, match="Connection"):
        _validate(monkeypatch, _task(sasl_password="hunter2"))


def test_secret_scan_is_source_agnostic_after_hoist(monkeypatch):
    """F6.3 hoist'i: sir taramasi artik kafka/iceberg olmayanlarda da calisir."""
    task = {
        "task_group_id": "plain",
        "source_schema": "public",
        "source_table": "t",
        "source_type": "table",
        "target_schema": "tgt",
        "target_table": "t",
        "load_method": "append",
        "api_token": "leak",
    }
    with pytest.raises(ValidationError, match="Connection"):
        _validate(monkeypatch, task)


# ---------------------------------------------------------------- T-F6.3-5

def test_kafka_to_file_target_is_refused(monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _validate(
            monkeypatch,
            _task(target_type="file", target_file_path="/tmp/out.csv"),
        )
    message = str(excinfo.value)
    assert "db" in message and "iceberg" in message


def test_kafka_topic_is_required_and_normalised(monkeypatch):
    task = _task(kafka_topic="  pg.public.orders  ")
    _validate(monkeypatch, task)
    assert task["kafka_topic"] == "pg.public.orders"
    bad = _task()
    bad.pop("kafka_topic")
    with pytest.raises(ValidationError, match="kafka_topic"):
        _validate(monkeypatch, bad)


def test_max_batch_records_must_be_positive_int(monkeypatch):
    with pytest.raises(ValidationError, match="max_batch_records"):
        _validate(monkeypatch, _task(max_batch_records=0))
    with pytest.raises(ValidationError, match="max_batch_records"):
        _validate(monkeypatch, _task(max_batch_records=True))
    _validate(monkeypatch, _task(max_batch_records=50_000))


# --------------------------------------------------- T-F6.3-22 (config yarisi)

def test_kafka_requires_enterprise_edition(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "community")
    with pytest.raises(ValidationError, match="[Ee]nterprise"):
        ConfigValidator().validate(_task())


def test_valid_kafka_db_task_passes(monkeypatch):
    _validate(monkeypatch, _task())


def test_valid_kafka_iceberg_task_requires_explicit_spark(monkeypatch):
    task = _task(
        target_type="iceberg",
        catalog_type="jdbc",
        _engine_preference="spark",
        _engine_spark={"submit_mode": "local", "conn_id": "iceberg_cat"},
    )
    _validate(monkeypatch, task)
    # Spark'siz iceberg hedefi zaten SPARK_ONLY kapisina takilir.
    task_no_spark = _task(target_type="iceberg", catalog_type="jdbc")
    with pytest.raises(ValidationError, match="spark"):
        _validate(monkeypatch, task_no_spark)
