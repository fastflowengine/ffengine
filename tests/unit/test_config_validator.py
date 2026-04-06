"""
C05 — ConfigValidator birim testleri.

Kapsam: whitelist doğrulama, zorunlu alan kontrolü, koşullu kurallar.
"""

import pytest

from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ConfigError, ValidationError

# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

_BASE = {
    "task_group_id": "t1",
    "source_schema": "public",
    "source_table": "orders",
    "source_type": "table",
    "target_schema": "dwh",
    "target_table": "orders_stg",
    "load_method": "append",
    "column_mapping_mode": "source",
    "extraction_method": "auto",
}


def _task(**overrides) -> dict:
    t = dict(_BASE)
    t.update(overrides)
    return t


# ---------------------------------------------------------------------------
# Zorunlu alanlar
# ---------------------------------------------------------------------------

class TestRequiredFields:
    @pytest.mark.parametrize("field", [
        "task_group_id",
        "source_schema",
        "target_schema",
        "target_table",
        "source_type",
        "load_method",
    ])
    def test_missing_required_raises_config_error(self, field):
        task = _task()
        del task[field]
        with pytest.raises(ConfigError, match=field):
            ConfigValidator().validate(task)

    def test_none_value_treated_as_missing(self):
        with pytest.raises(ConfigError, match="target_table"):
            ConfigValidator().validate(_task(target_table=None))


# ---------------------------------------------------------------------------
# source_type
# ---------------------------------------------------------------------------

class TestSourceType:
    @pytest.mark.parametrize("st", ["table", "view", "csv", "script"])
    def test_valid_source_types_pass(self, st):
        ConfigValidator().validate(_task(source_type=st))

    def test_sql_with_sql_file_passes(self):
        ConfigValidator().validate(_task(source_type="sql", sql_file="q.sql"))

    def test_sql_with_inline_sql_passes(self):
        ConfigValidator().validate(_task(source_type="sql", inline_sql="SELECT 1"))

    def test_sql_without_source_schema_passes(self):
        ConfigValidator().validate(
            _task(source_type="sql", source_schema=None, source_table=None, inline_sql="SELECT 1")
        )

    def test_sql_without_sql_file_raises(self):
        with pytest.raises(ValidationError, match="sql_file"):
            ConfigValidator().validate(_task(source_type="sql"))

    def test_invalid_source_type_raises(self):
        with pytest.raises(ValidationError, match="source_type"):
            ConfigValidator().validate(_task(source_type="xlsx"))

    def test_empty_string_source_type_raises(self):
        with pytest.raises(ValidationError, match="source_type"):
            ConfigValidator().validate(_task(source_type=""))


# ---------------------------------------------------------------------------
# load_method
# ---------------------------------------------------------------------------

class TestLoadMethod:
    @pytest.mark.parametrize("lm", [
        "create_if_not_exists_or_truncate",
        "append",
        "replace",
        "upsert",
        "delete_from_table",
        "drop_if_exists_and_create",
        "script",
    ])
    def test_valid_load_methods_pass(self, lm):
        ConfigValidator().validate(_task(load_method=lm))

    def test_truncate_plus_insert_is_invalid(self):
        # CONFIG_SCHEMA.md'de tanımlı ama LOAD_METHODS.md'de yok;
        # C05 bu tutarsızlığı ValidationError ile çözer.
        with pytest.raises(ValidationError, match="load_method"):
            ConfigValidator().validate(_task(load_method="truncate+insert"))

    def test_invalid_load_method_raises(self):
        with pytest.raises(ValidationError, match="load_method"):
            ConfigValidator().validate(_task(load_method="bulk_load"))


# ---------------------------------------------------------------------------
# column_mapping_mode
# ---------------------------------------------------------------------------

class TestColumnMappingMode:
    def test_source_mode_passes(self):
        ConfigValidator().validate(_task(column_mapping_mode="source"))

    def test_mapping_file_with_path_passes(self):
        ConfigValidator().validate(
            _task(column_mapping_mode="mapping_file", mapping_file="map.yaml")
        )

    def test_mapping_file_without_path_raises(self):
        with pytest.raises(ValidationError, match="mapping_file"):
            ConfigValidator().validate(_task(column_mapping_mode="mapping_file"))

    def test_mapping_file_empty_path_raises(self):
        with pytest.raises(ValidationError, match="mapping_file"):
            ConfigValidator().validate(
                _task(column_mapping_mode="mapping_file", mapping_file="")
            )

    def test_invalid_mode_raises(self):
        with pytest.raises(ValidationError, match="column_mapping_mode"):
            ConfigValidator().validate(_task(column_mapping_mode="auto"))


# ---------------------------------------------------------------------------
# extraction_method
# ---------------------------------------------------------------------------

class TestExtractionMethod:
    @pytest.mark.parametrize("em", ["auto", "cursor", "copy_binary"])
    def test_valid_extraction_methods_pass(self, em):
        ConfigValidator().validate(_task(extraction_method=em))

    def test_copy_binary_emits_warning(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING):
            ConfigValidator().validate(_task(extraction_method="copy_binary"))
        assert "copy_binary" in caplog.text

    def test_invalid_extraction_method_raises(self):
        with pytest.raises(ValidationError, match="extraction_method"):
            ConfigValidator().validate(_task(extraction_method="bulk"))


# ---------------------------------------------------------------------------
# partitioning doğrulaması (C06)
# ---------------------------------------------------------------------------


def _part_task(**part_overrides) -> dict:
    """Geçerli bir partitioning bloğu içeren task döndürür."""
    t = dict(_BASE)
    t["partitioning"] = {
        "enabled": True,
        "mode": "auto_numeric",
        "parts": 4,
        "distinct_limit": 16,
        "column": "id",
        "ranges": [],
    }
    t["partitioning"].update(part_overrides)
    return t


class TestPartitioningValidation:
    def test_valid_auto_numeric_with_column_passes(self):
        ConfigValidator().validate(_part_task())

    def test_valid_explicit_with_ranges_passes(self):
        ConfigValidator().validate(
            _part_task(mode="explicit", ranges=["id < 100", "id >= 100"], column=None)
        )

    def test_valid_hash_mod_with_column_passes(self):
        ConfigValidator().validate(_part_task(mode="hash_mod"))

    def test_valid_distinct_with_column_passes(self):
        ConfigValidator().validate(_part_task(mode="distinct"))

    def test_full_scan_mode_rejected(self):
        with pytest.raises(ValidationError, match="mode"):
            ConfigValidator().validate(_part_task(mode="full_scan", column=None))

    def test_auto_mode_alias_accepted(self):
        # "auto" → "auto_numeric" normalize edilir, ValidationError fırlatılmaz
        ConfigValidator().validate(_part_task(mode="auto"))

    def test_invalid_mode_raises_validation_error(self):
        with pytest.raises(ValidationError, match="mode"):
            ConfigValidator().validate(_part_task(mode="foobar"))

    def test_enabled_auto_numeric_missing_column_raises(self):
        with pytest.raises(ValidationError, match="column"):
            ConfigValidator().validate(_part_task(column=None))

    def test_enabled_explicit_empty_ranges_raises(self):
        with pytest.raises(ValidationError, match="ranges"):
            ConfigValidator().validate(
                _part_task(mode="explicit", ranges=[], column=None)
            )

    def test_parts_zero_raises(self):
        with pytest.raises(ValidationError, match="parts"):
            ConfigValidator().validate(_part_task(parts=0))

    def test_parts_negative_raises(self):
        with pytest.raises(ValidationError, match="parts"):
            ConfigValidator().validate(_part_task(parts=-1))

    def test_distinct_limit_zero_raises(self):
        with pytest.raises(ValidationError, match="distinct_limit"):
            ConfigValidator().validate(_part_task(mode="distinct", distinct_limit=0))

    def test_explicit_non_string_clause_raises(self):
        with pytest.raises(ValidationError, match="string"):
            ConfigValidator().validate(
                _part_task(mode="explicit", ranges=[{"min": 1, "max": 10}], column=None)
            )

    def test_disabled_skips_column_check(self):
        # enabled=False → kolon kontrolü atlanır
        t = dict(_BASE)
        t["partitioning"] = {"enabled": False, "mode": "auto_numeric", "column": None}
        ConfigValidator().validate(t)


# ---------------------------------------------------------------------------
# passthrough_full konfigürasyon kuralı (C09)
# ---------------------------------------------------------------------------


class TestPassthroughConfig:
    def test_passthrough_full_true_no_source_columns_passes(self):
        ConfigValidator().validate(_task(passthrough_full=True))

    def test_passthrough_full_false_with_source_columns_passes(self):
        ConfigValidator().validate(
            _task(passthrough_full=False, source_columns=["id", "name"])
        )

    def test_passthrough_full_false_without_source_columns_raises(self):
        with pytest.raises(ValidationError, match="source_columns"):
            ConfigValidator().validate(_task(passthrough_full=False, source_columns=None))

    def test_passthrough_full_false_empty_list_raises(self):
        with pytest.raises(ValidationError, match="source_columns"):
            ConfigValidator().validate(_task(passthrough_full=False, source_columns=[]))

    def test_mapping_file_mode_skips_passthrough_check(self):
        # mapping_file modunda passthrough_full=False source_columns olmadan geçerli
        ConfigValidator().validate(
            _task(
                column_mapping_mode="mapping_file",
                mapping_file="map.yaml",
                passthrough_full=False,
                source_columns=None,
            )
        )

