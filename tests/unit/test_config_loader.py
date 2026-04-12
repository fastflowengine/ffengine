"""
C05 — ConfigLoader birim testleri.

Kapsam: YAML yükleme, task arama, default uygulama, hata senaryoları.
"""

import textwrap

import pytest

from ffengine.config.loader import ConfigLoader
from ffengine.errors.exceptions import ConfigError, ValidationError

# ---------------------------------------------------------------------------
# Yardımcı YAML şablonu
# ---------------------------------------------------------------------------

_VALID_YAML = textwrap.dedent("""\
    source_db_var: src_conn
    target_db_var: tgt_conn
    flow_tasks:
      - task_group_id: t1
        source_schema: public
        source_table: orders
        source_type: table
        target_schema: dwh
        target_table: orders_stg
        load_method: append
""")

_MULTI_TASK_YAML = textwrap.dedent("""\
    source_db_var: src_conn
    target_db_var: tgt_conn
    flow_tasks:
      - task_group_id: t1
        source_schema: public
        source_table: orders
        source_type: table
        target_schema: dwh
        target_table: orders_stg
        load_method: append
      - task_group_id: t2
        source_schema: public
        source_table: products
        source_type: table
        target_schema: dwh
        target_table: products_stg
        load_method: replace
""")


# ---------------------------------------------------------------------------
# Geçerli config
# ---------------------------------------------------------------------------

class TestConfigLoaderValid:
    def test_load_returns_dict(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        result = ConfigLoader().load(str(p), "t1")
        assert isinstance(result, dict)

    def test_task_fields_present(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        result = ConfigLoader().load(str(p), "t1")
        assert result["source_schema"] == "public"
        assert result["source_table"] == "orders"
        assert result["target_table"] == "orders_stg"
        assert result["load_method"] == "append"

    def test_default_batch_size_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["batch_size"] == 10_000

    def test_default_reader_workers_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["reader_workers"] == 3

    def test_default_writer_workers_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["writer_workers"] == 5

    def test_default_pipe_queue_max_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["pipe_queue_max"] == 8

    def test_default_extraction_method_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["extraction_method"] == "auto"

    def test_default_passthrough_format_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["passthrough_format"] == "binary"

    def test_default_passthrough_full_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["passthrough_full"] is True

    def test_default_column_mapping_mode_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["column_mapping_mode"] == "source"

    def test_default_where_is_none(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        assert ConfigLoader().load(str(p), "t1")["where"] is None

    def test_task_value_overrides_default(self, tmp_path):
        # 4 boşluk — load_method ile aynı seviye
        yaml_override = _VALID_YAML + "    batch_size: 500\n"
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_override)
        assert ConfigLoader().load(str(p), "t1")["batch_size"] == 500

    def test_partitioning_defaults_applied(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        part = ConfigLoader().load(str(p), "t1")["partitioning"]
        assert part["enabled"] is False
        assert part["mode"] == "auto"
        assert part["parts"] == 4
        assert part["distinct_limit"] == 16
        assert part["column"] is None   # C06 eklendi
        assert part["ranges"] == []     # C06 eklendi

    def test_partitioning_override_merges_with_default(self, tmp_path):
        yaml_part = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
            flow_tasks:
              - task_group_id: t1
                source_schema: public
                source_table: orders
                source_type: table
                target_schema: dwh
                target_table: orders_stg
                load_method: append
                partitioning:
                  enabled: true
                  mode: auto_numeric
                  column: id
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_part)
        part = ConfigLoader().load(str(p), "t1")["partitioning"]
        assert part["enabled"] is True
        assert part["column"] == "id"
        assert part["parts"] == 4   # default korundu
        assert part["distinct_limit"] == 16
        assert part["ranges"] == [] # default korundu

    def test_second_task_loaded_by_id(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_MULTI_TASK_YAML)
        result = ConfigLoader().load(str(p), "t2")
        assert result["source_table"] == "products"
        assert result["load_method"] == "replace"

    def test_does_not_mutate_original_defaults(self, tmp_path):
        from ffengine.config.schema import TASK_DEFAULTS
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        ConfigLoader().load(str(p), "t1")
        assert TASK_DEFAULTS["batch_size"] == 10_000

    def test_mapping_file_relative_path_resolved_against_config_dir(self, tmp_path):
        yaml_m = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
            flow_tasks:
              - task_group_id: t1
                source_schema: public
                source_table: t
                source_type: table
                target_schema: dwh
                target_table: t
                load_method: append
                column_mapping_mode: mapping_file
                mapping_file: mapping/1_t1.yaml
        """)
        cfg = tmp_path / "nested" / "cfg.yaml"
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_text(yaml_m)
        loaded = ConfigLoader().load(str(cfg), "t1")
        assert loaded["mapping_file"] == str((cfg.parent / "mapping" / "1_t1.yaml").resolve())


# ---------------------------------------------------------------------------
# Hata senaryoları
# ---------------------------------------------------------------------------

class TestConfigLoaderErrors:
    def test_file_not_found_raises_config_error(self, tmp_path):
        with pytest.raises(ConfigError, match="bulunamadı"):
            ConfigLoader().load(str(tmp_path / "nofile.yaml"), "t1")

    def test_task_not_found_raises_config_error(self, tmp_path):
        p = tmp_path / "cfg.yaml"
        p.write_text(_VALID_YAML)
        with pytest.raises(ConfigError, match="bulunamadı"):
            ConfigLoader().load(str(p), "nonexistent_id")

    def test_missing_root_target_db_var(self, tmp_path):
        yaml_no_tgt = textwrap.dedent("""\
            source_db_var: src_conn
            flow_tasks: []
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_no_tgt)
        with pytest.raises(ConfigError, match="target_db_var"):
            ConfigLoader().load(str(p), "t1")

    def test_null_source_db_var_raises_config_error(self, tmp_path):
        yaml_null = textwrap.dedent("""\
            source_db_var: null
            target_db_var: tgt_conn
            flow_tasks: []
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_null)
        with pytest.raises(ConfigError, match="source_db_var"):
            ConfigLoader().load(str(p), "t1")

    def test_null_target_db_var_raises_config_error(self, tmp_path):
        yaml_null = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: null
            flow_tasks: []
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_null)
        with pytest.raises(ConfigError, match="target_db_var"):
            ConfigLoader().load(str(p), "t1")

    def test_missing_root_source_db_var(self, tmp_path):
        yaml_no_src = textwrap.dedent("""\
            target_db_var: tgt_conn
            flow_tasks: []
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_no_src)
        with pytest.raises(ConfigError, match="source_db_var"):
            ConfigLoader().load(str(p), "t1")

    def test_missing_root_flow_tasks(self, tmp_path):
        yaml_no_tasks = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_no_tasks)
        with pytest.raises(ConfigError, match="flow_tasks"):
            ConfigLoader().load(str(p), "t1")

    def test_invalid_yaml_raises_config_error(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(": bad\n  invalid:\n :\n  broken:")
        with pytest.raises(ConfigError, match="YAML"):
            ConfigLoader().load(str(p), "t1")

    def test_sql_source_without_sql_file_raises_validation_error(self, tmp_path):
        yaml_sql = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
            flow_tasks:
              - task_group_id: t1
                source_schema: public
                source_type: sql
                target_schema: dwh
                target_table: t
                load_method: append
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_sql)
        with pytest.raises(ValidationError, match="sql_file"):
            ConfigLoader().load(str(p), "t1")

    def test_mapping_file_mode_without_path_raises_validation_error(self, tmp_path):
        yaml_m = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
            flow_tasks:
              - task_group_id: t1
                source_schema: public
                source_table: t
                source_type: table
                target_schema: dwh
                target_table: t
                load_method: append
                column_mapping_mode: mapping_file
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_m)
        with pytest.raises(ValidationError, match="mapping_file"):
            ConfigLoader().load(str(p), "t1")

    def test_invalid_load_method_raises_validation_error(self, tmp_path):
        yaml_bad_lm = textwrap.dedent("""\
            source_db_var: src_conn
            target_db_var: tgt_conn
            flow_tasks:
              - task_group_id: t1
                source_schema: public
                source_table: t
                source_type: table
                target_schema: dwh
                target_table: t
                load_method: truncate+insert
        """)
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml_bad_lm)
        with pytest.raises(ValidationError, match="load_method"):
            ConfigLoader().load(str(p), "t1")
