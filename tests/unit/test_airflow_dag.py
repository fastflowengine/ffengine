"""
C07 — DAG pattern ve DAG generator birim testleri.

Kapsam: XComKeys, build_task_group, generate_dags, register_dags.
"""

import os
import pytest
from unittest.mock import MagicMock, patch, ANY

from ffengine.airflow.dag_patterns import XComKeys, build_task_group
from ffengine.airflow.dag_generator import generate_dags, register_dags


# ---------------------------------------------------------------------------
# XComKeys
# ---------------------------------------------------------------------------


class TestXComKeys:
    def test_task_config_resolved_is_string(self):
        assert isinstance(XComKeys.TASK_CONFIG_RESOLVED, str)

    def test_partition_specs_is_string(self):
        assert isinstance(XComKeys.PARTITION_SPECS, str)

    def test_rows_transferred_is_string(self):
        assert isinstance(XComKeys.ROWS_TRANSFERRED, str)

    def test_all_keys_unique(self):
        keys = [
            XComKeys.TASK_CONFIG_RESOLVED,
            XComKeys.PARTITION_SPECS,
            XComKeys.ROWS_TRANSFERRED,
            XComKeys.DURATION_SECONDS,
            XComKeys.ROWS_PER_SECOND,
        ]
        assert len(keys) == len(set(keys))


# ---------------------------------------------------------------------------
# build_task_group
# ---------------------------------------------------------------------------


class TestBuildTaskGroup:
    @pytest.fixture
    def dag(self):
        from datetime import datetime
        from airflow import DAG

        return DAG(
            dag_id="test_dag",
            start_date=datetime(2023, 1, 1),
            schedule=None,
        )

    def test_returns_task_group(self, dag):
        from airflow.utils.task_group import TaskGroup

        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        assert isinstance(tg, TaskGroup)

    def test_contains_three_tasks(self, dag):
        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        # TaskGroup.children keys = task_ids within group
        assert len(tg.children) == 3

    def test_task_ids(self, dag):
        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        child_ids = set(tg.children.keys())
        assert "ffengine_etl.plan_partitions" in child_ids
        assert "ffengine_etl.prepare_target" in child_ids
        assert "ffengine_etl.run_partitions" in child_ids

    def test_dependency_order_plan_before_prepare(self, dag):
        """plan_partitions → prepare_target bağımlılığı."""
        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        plan = tg.children["ffengine_etl.plan_partitions"]
        prepare = tg.children["ffengine_etl.prepare_target"]
        # downstream_task_ids plan'ın çıktısını kontrol eder
        assert prepare.task_id in plan.downstream_task_ids

    def test_dependency_order_prepare_before_run(self, dag):
        """prepare_target → run_partitions bağımlılığı."""
        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        prepare = tg.children["ffengine_etl.prepare_target"]
        run = tg.children["ffengine_etl.run_partitions"]
        assert run.task_id in prepare.downstream_task_ids

    def test_custom_group_id(self, dag):
        tg = build_task_group(
            dag,
            config_path="/tmp/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src",
            target_conn_id="tgt",
            group_id="custom_group",
        )
        assert tg.group_id == "custom_group"


# ---------------------------------------------------------------------------
# generate_dags
# ---------------------------------------------------------------------------


class TestGenerateDags:
    def test_single_yaml_single_task(self, tmp_path):
        cfg = tmp_path / "orders.yaml"
        cfg.write_text(
            "source_db_var: src_pg\n"
            "target_db_var: tgt_pg\n"
            "etl_tasks:\n"
            "  - task_group_id: load_orders\n"
            "    source_schema: public\n"
            "    source_table: orders\n"
            "    source_type: table\n"
            "    target_schema: dwh\n"
            "    target_table: orders_stg\n"
            "    load_method: append\n",
            encoding="utf-8",
        )
        dags = generate_dags(str(tmp_path))
        assert len(dags) == 1
        assert "ffengine_orders_load_orders" in dags

    def test_multi_task_yaml(self, tmp_path):
        cfg = tmp_path / "sales.yaml"
        cfg.write_text(
            "source_db_var: src\n"
            "target_db_var: tgt\n"
            "etl_tasks:\n"
            "  - task_group_id: t1\n"
            "    source_schema: s\n"
            "    source_table: a\n"
            "    source_type: table\n"
            "    target_schema: d\n"
            "    target_table: a\n"
            "    load_method: append\n"
            "  - task_group_id: t2\n"
            "    source_schema: s\n"
            "    source_table: b\n"
            "    source_type: table\n"
            "    target_schema: d\n"
            "    target_table: b\n"
            "    load_method: append\n",
            encoding="utf-8",
        )
        dags = generate_dags(str(tmp_path))
        assert len(dags) == 2
        assert "ffengine_sales_t1" in dags
        assert "ffengine_sales_t2" in dags

    def test_empty_directory(self, tmp_path):
        dags = generate_dags(str(tmp_path))
        assert dags == {}

    def test_invalid_yaml_skipped(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text(":::invalid yaml:::", encoding="utf-8")
        good = tmp_path / "good.yaml"
        good.write_text(
            "source_db_var: s\n"
            "target_db_var: t\n"
            "etl_tasks:\n"
            "  - task_group_id: ok\n"
            "    source_schema: s\n"
            "    source_table: x\n"
            "    source_type: table\n"
            "    target_schema: d\n"
            "    target_table: x\n"
            "    load_method: append\n",
            encoding="utf-8",
        )
        dags = generate_dags(str(tmp_path))
        assert len(dags) == 1
        assert "ffengine_good_ok" in dags

    def test_nonexistent_dir_returns_empty(self):
        dags = generate_dags("/nonexistent/path")
        assert dags == {}

    def test_custom_prefix_and_tags(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text(
            "etl_tasks:\n"
            "  - task_group_id: x\n",
            encoding="utf-8",
        )
        dags = generate_dags(
            str(tmp_path), dag_prefix="myapp", tags=["prod"],
        )
        assert "myapp_test_x" in dags


# ---------------------------------------------------------------------------
# register_dags
# ---------------------------------------------------------------------------


class TestRegisterDags:
    def test_updates_globals_dict(self, tmp_path):
        cfg = tmp_path / "reg.yaml"
        cfg.write_text(
            "etl_tasks:\n"
            "  - task_group_id: r1\n",
            encoding="utf-8",
        )
        g = {}
        register_dags(str(tmp_path), g)
        assert "ffengine_reg_r1" in g

    def test_dag_id_format(self, tmp_path):
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(
            "etl_tasks:\n"
            "  - task_group_id: load\n",
            encoding="utf-8",
        )
        g = {}
        register_dags(str(tmp_path), g, dag_prefix="etl")
        assert "etl_pipeline_load" in g
