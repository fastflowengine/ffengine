"""
C07 - DAG generator unit tests.

Scope: generate_dags, register_dags, and FFEngineOperator-based DAG generation.
"""

from ffengine.airflow.dag_generator import generate_dags, register_dags
from ffengine.airflow.operator import FFEngineOperator


class TestGenerateDags:
    def test_single_yaml_single_task(self, tmp_path):
        cfg = tmp_path / "orders.yaml"
        cfg.write_text(
            "source_db_var: src_pg\n"
            "target_db_var: tgt_pg\n"
            "flow_tasks:\n"
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
            "flow_tasks:\n"
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
            "flow_tasks:\n"
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
            "flow_tasks:\n"
            "  - task_group_id: x\n",
            encoding="utf-8",
        )
        dags = generate_dags(str(tmp_path), dag_prefix="myapp", tags=["prod"])
        assert "myapp_test_x" in dags

    def test_uses_ffengine_operator(self, tmp_path):
        cfg = tmp_path / "orders.yaml"
        cfg.write_text(
            "source_db_var: src_pg\n"
            "target_db_var: tgt_pg\n"
            "flow_tasks:\n"
            "  - task_group_id: load_orders\n",
            encoding="utf-8",
        )

        dags = generate_dags(str(tmp_path))
        dag = dags["ffengine_orders_load_orders"]

        assert len(dag.tasks) == 1
        assert isinstance(dag.tasks[0], FFEngineOperator)


class TestRegisterDags:
    def test_updates_globals_dict(self, tmp_path):
        cfg = tmp_path / "reg.yaml"
        cfg.write_text(
            "flow_tasks:\n"
            "  - task_group_id: r1\n",
            encoding="utf-8",
        )
        g = {}
        register_dags(str(tmp_path), g)
        assert "ffengine_reg_r1" in g

    def test_dag_id_format(self, tmp_path):
        cfg = tmp_path / "pipeline.yaml"
        cfg.write_text(
            "flow_tasks:\n"
            "  - task_group_id: load\n",
            encoding="utf-8",
        )
        g = {}
        register_dags(str(tmp_path), g, dag_prefix="etl")
        assert "etl_pipeline_load" in g


class TestAirflowPublicExports:
    def test_removed_exports_not_present(self):
        import ffengine.airflow as airflow_mod

        assert hasattr(airflow_mod, "FFEngineOperator")
        assert not hasattr(airflow_mod, "XComKeys")
        assert not hasattr(airflow_mod, "build_task_group")
