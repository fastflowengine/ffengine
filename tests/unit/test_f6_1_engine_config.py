"""F6.1 Spark config contract tests (T-F6.1-3/-5/-6/-12)."""

import pytest

from ffengine.config.loader import ConfigLoader
from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ConfigError, ValidationError


def _task(**overrides):
    task = {
        "task_group_id": "spark_load",
        "source_schema": "public",
        "source_table": "orders",
        "source_type": "table",
        "target_schema": "bronze",
        "target_table": "orders",
        "target_type": "iceberg",
        "load_method": "append",
        "_engine_preference": "spark",
        "_engine_spark": {"submit_mode": "k8s", "conn_id": "spark_prod"},
        "writer_workers": None,
        "use_bulk_api": False,
        "partitioning": {"enabled": False},
    }
    task.update(overrides)
    return task


def _validate_with_iceberg(monkeypatch, task):
    """F6.2 adds the enum; isolate F6.1 rules through the public validator."""
    import ffengine.config.validator as validator_module

    monkeypatch.setattr(
        validator_module, "VALID_TARGET_TYPES", frozenset({"db", "file", "iceberg"})
    )
    monkeypatch.setattr(
        validator_module,
        "VALID_SOURCE_TYPES",
        frozenset({"table", "view", "sql", "csv", "json", "script", "iceberg"}),
    )
    ConfigValidator().validate(task)


@pytest.mark.parametrize("mode", ["standalone", "emr", "databricks", "glue"])
def test_submit_mode_whitelist_is_fail_loud(mode, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = _task(_engine_spark={"submit_mode": mode, "conn_id": "spark_prod"})
    with pytest.raises(ValidationError, match="submit_mode"):
        _validate_with_iceberg(monkeypatch, task)


@pytest.mark.parametrize(
    "override",
    [
        {"writer_workers": 2},
        {"use_bulk_api": True, "bulk_api_method": "postgres_copy"},
        {"partitioning": {"enabled": True}},
        {"parallel_degree": 4},
    ],
)
def test_knob_lock_rejects_explicit_pipeline_knobs(override, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="SparkEngine|spark"):
        _validate_with_iceberg(monkeypatch, _task(**override))


def test_knob_lock_accepts_disabled_defaults(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _validate_with_iceberg(monkeypatch, _task())


@pytest.mark.parametrize("preference", ["auto", "standard", "pipeline"])
def test_spark_block_requires_spark_preference(preference, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="engine.spark"):
        _validate_with_iceberg(
            monkeypatch, _task(_engine_preference=preference, target_type="db")
        )


def test_spark_preference_requires_nested_block(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = _task()
    task.pop("_engine_spark")
    # Mesajin KENDISI dogrulanir: yalnizca "engine.spark" aramak, komsu
    # "engine.spark must be a mapping" dalinin da gecmesine izin verirdi ve
    # bu dal silinse bile test yesil kalirdi.
    with pytest.raises(ValidationError, match="requires the nested engine.spark block"):
        _validate_with_iceberg(monkeypatch, task)


def test_spark_block_must_be_a_mapping(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="engine.spark must be a mapping"):
        _validate_with_iceberg(monkeypatch, _task(_engine_spark="k8s"))


def test_knob_lock_accepts_an_explicit_null_parallel_degree(monkeypatch):
    """T-F6.1-6: explicit varsayilan reddedilmez.

    Knob kilidi `parallel_degree`'de anahtar VARLIGINA baksaydi
    `parallel_degree: null` reddedilirdi -- digerleri deger-bazli oldugu icin
    bu asimetri "default != explicit" ayrimini bozardi.
    """
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _validate_with_iceberg(monkeypatch, _task(parallel_degree=None))


def test_cluster_submit_requires_connection(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="conn_id"):
        _validate_with_iceberg(
            monkeypatch, _task(_engine_spark={"submit_mode": "k8s"})
        )


def test_yarn_is_recognised_and_rejected_with_its_reason(monkeypatch):
    """EX-D026: YARN ertelendi -> 'gecersiz mod' degil, GEREKCELI red.

    Kullanici neden calismadigini ogrenmeli: klasik YARN'da konteyner imaji
    yoktur, imaja bake edilen driver script'i NodeManager'da bulunmaz.
    """
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError) as excinfo:
        _validate_with_iceberg(
            monkeypatch,
            _task(_engine_spark={"submit_mode": "yarn", "conn_id": "spark_prod"}),
        )
    message = str(excinfo.value)
    assert "recognised" in message and "not shipped" in message
    assert "container image" in message
    assert "['k8s', 'local']" in message


def test_yarn_is_not_a_shipped_mode(monkeypatch):
    """Enum'un kendisi de sevkiyati yansitmali (Studio bu listeden beslenir)."""
    from ffengine.config.schema import (
        DEFERRED_SPARK_SUBMIT_MODES,
        VALID_SPARK_SUBMIT_MODES,
    )

    assert VALID_SPARK_SUBMIT_MODES == {"k8s", "local"}
    assert "yarn" in DEFERRED_SPARK_SUBMIT_MODES


def test_local_submit_connection_is_optional_at_config_layer(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    _validate_with_iceberg(
        monkeypatch, _task(_engine_spark={"submit_mode": "local"})
    )


@pytest.mark.parametrize("target_type", [None, "db", "file"])
def test_spark_requires_iceberg_target(target_type, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises((ConfigError, ValidationError), match="Iceberg|iceberg"):
        _validate_with_iceberg(monkeypatch, _task(target_type=target_type))


@pytest.mark.parametrize("source_type", ["csv", "json", "script"])
def test_deferred_sources_are_rejected(source_type, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="source|kaynak"):
        _validate_with_iceberg(monkeypatch, _task(source_type=source_type))


def test_loader_carries_nested_spark_block(tmp_path, monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    config = tmp_path / "spark.yaml"
    config.write_text(
        """\
source_db_var: source
target_db_var: target
engine:
  preference: spark
  spark:
    submit_mode: k8s
    conn_id: spark_prod
flow_tasks:
  - task_group_id: spark_load
    source_schema: public
    source_table: orders
    source_type: table
    target_schema: bronze
    target_table: orders
    target_type: iceberg
    load_method: append
""",
        encoding="utf-8",
    )
    # F6.2 has not added the Iceberg enum yet, but the F6.1 loader must carry
    # the nested block before that later validator error.
    with pytest.raises(ValidationError) as exc:
        ConfigLoader().load(str(config), "spark_load")
    assert "engine.spark" not in str(exc.value)


def test_task_cannot_inject_private_spark_block(tmp_path):
    config = tmp_path / "reserved.yaml"
    config.write_text(
        """\
source_db_var: source
target_db_var: target
flow_tasks:
  - task_group_id: spark_load
    source_schema: public
    source_table: orders
    source_type: table
    target_schema: bronze
    target_table: orders
    load_method: append
    _engine_spark: {submit_mode: local}
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="reserved|engine.spark"):
        ConfigLoader().load(str(config), "spark_load")
