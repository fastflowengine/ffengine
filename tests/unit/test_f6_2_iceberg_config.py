"""F6.2 Iceberg config contract tests (T-F6.2-1/-2/-3/-4/-9/-10/-20).

EX-D030: `catalog_type` config'de ZORUNLU ve sirsizdir; baglanti detayi ve
kimlik yalniz Airflow Connection'da yasar. Boylece red DAG-parse'ta dogar ve
burada statik olarak kanitlanabilir.
"""

import pytest

from ffengine.config.schema import (
    REJECTED_ICEBERG_CATALOG_TYPES,
    REJECTED_ICEBERG_LOAD_METHODS,
    VALID_ICEBERG_CATALOG_TYPES,
)
from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ValidationError


def _task(**overrides):
    task = {
        "task_group_id": "iceberg_load",
        "source_schema": "public",
        "source_table": "orders",
        "source_type": "table",
        "target_schema": "bronze",
        "target_table": "orders",
        "target_type": "iceberg",
        "catalog_type": "jdbc",
        "load_method": "append",
        "_engine_preference": "spark",
        "_engine_spark": {"submit_mode": "local", "conn_id": "iceberg_cat"},
    }
    task.update(overrides)
    return task


def _validate(monkeypatch, task):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    ConfigValidator().validate(task)


# ---------------------------------------------------------------- T-F6.2-1

def test_catalog_type_is_required_and_has_no_auto(monkeypatch):
    task = _task()
    task.pop("catalog_type")
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, task)
    message = str(excinfo.value)
    assert "catalog_type" in message
    # Mesaj "auto yok"u SOYLEMELI: kullanici aksi halde alani bos birakip
    # motorun secmesini bekler (INV-7).
    assert "auto" in message.lower()
    for valid in sorted(VALID_ICEBERG_CATALOG_TYPES):
        assert valid in message


def test_unknown_catalog_type_is_fail_loud(monkeypatch):
    with pytest.raises(ValidationError, match="catalog_type"):
        _validate(monkeypatch, _task(catalog_type="nessie"))


@pytest.mark.parametrize("catalog_type", sorted(VALID_ICEBERG_CATALOG_TYPES))
def test_supported_catalog_types_pass(catalog_type, monkeypatch):
    _validate(monkeypatch, _task(catalog_type=catalog_type))


def test_catalog_type_is_normalised(monkeypatch):
    """Motor tarafi tam esleme arar; 'JDBC' dogrulamayi gecip submit'te patlamamali."""
    task = _task(catalog_type="  JDBC  ")
    _validate(monkeypatch, task)
    assert task["catalog_type"] == "jdbc"


# ------------------------------------------------------------ T-F6.2-2/-3

def test_glue_catalog_is_refused_with_its_reason(monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, _task(catalog_type="glue"))
    message = str(excinfo.value).lower()
    # "gecersiz tip" yetmez: air-gapped ihlalinin NEDENI gorunmeli.
    assert "aws" in message
    assert "air-gapped" in message or "internet" in message


def test_hadoop_catalog_is_refused_with_its_reason(monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, _task(catalog_type="hadoop"))
    message = str(excinfo.value).lower()
    assert "rename" in message
    assert "snapshot" in message


def test_every_rejected_catalog_type_carries_a_reason():
    """Gerekce metinleri BOS olamaz: red mesaji sablonu buradan doluyor."""
    assert set(REJECTED_ICEBERG_CATALOG_TYPES) == {"glue", "hadoop"}
    for reason in REJECTED_ICEBERG_CATALOG_TYPES.values():
        assert len(reason.strip()) > 40


# ---------------------------------------------------------------- T-F6.2-4

@pytest.mark.parametrize(
    "field",
    ["catalog_password", "catalog_secret", "aws_access_key_id", "catalog_token"],
)
def test_secret_bearing_config_field_is_refused(field, monkeypatch):
    """Kimlik yalniz Airflow Connection'da; config'e sir yazilamaz (INV-5)."""
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, _task(**{field: "s3cr3t"}))
    message = str(excinfo.value)
    assert field in message
    assert "Connection" in message
    # Sirrin KENDISI mesaja sizmamali.
    assert "s3cr3t" not in message


def test_iceberg_requires_a_connection_even_in_local_mode(monkeypatch):
    """Katalog detayi Connection'dan gelir; local submit de onsuz calisamaz.

    F6.1'de `conn_id` yalniz k8s icin zorunluydu (submit hedefi). Iceberg'de
    Connection AYRICA katalog kanalidir -- eksikse motor katalogu nereden
    kuracagini bilemez ve bu sessiz bir yanlis-katalog riski olurdu.
    """
    with pytest.raises(ValidationError, match="conn_id"):
        _validate(monkeypatch, _task(_engine_spark={"submit_mode": "local"}))


# --------------------------------------------------------------- T-F6.2-10

@pytest.mark.parametrize(
    "load_method, alternative",
    [
        ("create_if_not_exists_or_truncate", "replace"),
        ("drop_if_exists_and_create", "replace"),
        ("script", "script_run"),
    ],
)
def test_rejected_load_methods_name_their_alternative(
    load_method, alternative, monkeypatch
):
    task = _task(load_method=load_method)
    if load_method == "script":
        task["sql_file"] = "x.sql"
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, task)
    message = str(excinfo.value)
    assert load_method in message
    # Kullaniciyi bosluga birakmaz: dogru alternatifi ISIMLE soyler.
    assert alternative in message


def test_every_rejected_load_method_carries_a_reason():
    assert set(REJECTED_ICEBERG_LOAD_METHODS) == {
        "create_if_not_exists_or_truncate",
        "drop_if_exists_and_create",
        "script",
    }
    for reason in REJECTED_ICEBERG_LOAD_METHODS.values():
        assert len(reason.strip()) > 40


@pytest.mark.parametrize("load_method", ["append", "replace", "upsert"])
def test_supported_load_methods_pass(load_method, monkeypatch):
    task = _task(load_method=load_method)
    if load_method == "upsert":
        task["upsert_match_columns"] = ["order_id"]
    _validate(monkeypatch, task)


def test_upsert_still_requires_match_columns(monkeypatch):
    """T-F6.2-9: MERGE INTO anahtarsiz calisamaz (mevcut kural, Iceberg yolunda)."""
    with pytest.raises(ValidationError, match="upsert_match_columns"):
        _validate(monkeypatch, _task(load_method="upsert"))


# ------------------------------------------------------- publish_mode / WAP

def test_publish_mode_defaults_to_direct(monkeypatch):
    task = _task()
    _validate(monkeypatch, task)
    assert task.get("publish_mode", "direct") == "direct"


def test_wap_publish_mode_is_accepted(monkeypatch):
    _validate(monkeypatch, _task(publish_mode="wap"))


def test_unknown_publish_mode_is_fail_loud(monkeypatch):
    with pytest.raises(ValidationError, match="publish_mode"):
        _validate(monkeypatch, _task(publish_mode="staged"))


# ------------------------------------------------------ Iceberg-only fields

@pytest.mark.parametrize(
    "override",
    [{"catalog_type": "jdbc"}, {"publish_mode": "wap"}],
)
def test_iceberg_only_fields_are_refused_on_non_iceberg_tasks(override, monkeypatch):
    """Sessizce yok saymak, kullaniciya uygulanmayan bir ayar sunmaktir (INV-1)."""
    task = _task(target_type="db", source_type="table", **override)
    task.pop("_engine_preference")
    task.pop("_engine_spark")
    with pytest.raises(ValidationError) as excinfo:
        _validate(monkeypatch, task)
    assert next(iter(override)) in str(excinfo.value)


# --------------------------------------------------------------- T-F6.2-20

def test_community_edition_refuses_iceberg(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "community")
    with pytest.raises(ValidationError) as excinfo:
        ConfigValidator().validate(_task())
    assert "Enterprise" in str(excinfo.value)


# ------------------------------------------------------------ iceberg source

def test_iceberg_source_is_accepted(monkeypatch):
    _validate(monkeypatch, _task(source_type="iceberg"))


def test_iceberg_source_without_explicit_spark_is_refused(monkeypatch):
    """T-F6.2-5 (F6.1'de kuruldu, enum eklendikten sonra da gecerli kalmali)."""
    task = _task(source_type="iceberg", target_type="db")
    task.pop("_engine_preference")
    task.pop("_engine_spark")
    task.pop("catalog_type")
    with pytest.raises(ValidationError, match="spark"):
        _validate(monkeypatch, task)


# ------------------------------------------------ parquet kaynagi (EX-D033)

def test_parquet_source_is_accepted_with_a_file_path(monkeypatch):
    task = _task(source_type="parquet", file_path="/data/orders")
    task.pop("source_schema", None)
    task.pop("source_table", None)
    _validate(monkeypatch, task)


def test_parquet_source_requires_file_path(monkeypatch):
    task = _task(source_type="parquet")
    task.pop("source_table", None)
    with pytest.raises(ValidationError, match="file_path"):
        _validate(monkeypatch, task)


def test_parquet_source_requires_explicit_spark(monkeypatch):
    """Standard/Pipeline motorlari parquet okuyamaz; sessizce gecmemeli."""
    task = _task(source_type="parquet", file_path="/data/orders", target_type="db")
    task.pop("_engine_preference")
    task.pop("_engine_spark")
    task.pop("catalog_type")
    with pytest.raises(ValidationError, match="spark"):
        _validate(monkeypatch, task)


# ------------------------------------------------------ Studio yuzeyi (S5)

def _studio_payload(**over):
    payload = {
        "project": "bank",
        "domain": "lake",
        "level": "raw",
        "flow": "orders",
        "source_conn_id": "source_db",
        "target_conn_id": "lakehouse",
        "source_schema": "bronze",
        "source_table": "orders",
        "target_schema": "bronze",
        "target_table": "orders",
        "source_type": "iceberg",
        "target_type": "iceberg",
        "catalog_type": "jdbc",
        "load_method": "append",
        "engine": {
            "preference": "spark",
            "spark": {"submit_mode": "local", "conn_id": "iceberg_cat"},
        },
    }
    payload.update(over)
    return payload


def test_studio_forwards_the_iceberg_fields_to_the_validator():
    """Alanlar tasiyiciya eklenmezse kullanici formu doldursa bile 422 alirdi."""
    from ffengine.ui.studio_service import build_task_dict_for_validation

    task = build_task_dict_for_validation(_studio_payload(publish_mode="wap"))
    assert task["catalog_type"] == "jdbc"
    assert task["publish_mode"] == "wap"
    assert task["target_type"] == "iceberg"


def test_studio_iceberg_payload_validates(monkeypatch):
    """Studio'nun GERCEK giris noktasi: engine blogunu da o bagliyor."""
    from ffengine.ui.studio_service import validate_pipeline_payload

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    validate_pipeline_payload(_studio_payload())


def test_studio_rejects_a_parquet_source_without_a_path():
    from ffengine.ui.studio_service import build_task_dict_for_validation

    with pytest.raises(ValueError, match="file_path"):
        build_task_dict_for_validation(
            _studio_payload(source_type="parquet", catalog_type="jdbc")
        )


def test_studio_parquet_payload_validates(monkeypatch):
    from ffengine.ui.studio_service import build_task_dict_for_validation

    from ffengine.ui.studio_service import validate_pipeline_payload

    payload = _studio_payload(source_type="parquet", file_path="file:///data/orders")
    task = build_task_dict_for_validation(payload)
    assert task["file_path"] == "file:///data/orders"
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    validate_pipeline_payload(payload)


@pytest.mark.parametrize(
    "load_method", ["create_if_not_exists_or_truncate", "drop_if_exists_and_create"]
)
def test_studio_rejected_load_methods_reach_the_reasoned_422(load_method, monkeypatch):
    from ffengine.ui.studio_service import validate_pipeline_payload

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError) as excinfo:
        validate_pipeline_payload(_studio_payload(load_method=load_method))
    assert "replace" in str(excinfo.value)
