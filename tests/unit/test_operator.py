"""
C07 — FFEngineOperator birim testleri.

Kapsam: resolve_dialect, combine_where, aggregate_results,
        FFEngineOperator init/execute, hata senaryoları, XCom.
"""

import pytest
from unittest.mock import MagicMock, patch, call

from ffengine.airflow.operator import (
    resolve_dialect,
    combine_where,
    aggregate_results,
    FFEngineOperator,
)
from ffengine.core.base_engine import FlowResult
from ffengine.errors.exceptions import ConfigError, EngineError


# ---------------------------------------------------------------------------
# Patch hedefleri — execute() lazy import yaptığı için kaynak modül yolu
# ---------------------------------------------------------------------------

_P_ADAPTER = "ffengine.db.airflow_adapter.AirflowConnectionAdapter"
_P_LOADER = "ffengine.config.loader.ConfigLoader"
_P_BINDER = "ffengine.config.binding_resolver.BindingResolver"
_P_DBSESS = "ffengine.db.session.DBSession"
_P_MAPPING = "ffengine.mapping.MappingResolver"
_P_PART = "ffengine.partition.Partitioner"
_P_WRITER = "ffengine.pipeline.target_writer.TargetWriter"
_P_FLOW = "ffengine.core.flow_manager.FlowManager"


# ---------------------------------------------------------------------------
# resolve_dialect
# ---------------------------------------------------------------------------


class TestResolveDialect:
    def test_postgres(self):
        d = resolve_dialect("postgres")
        assert type(d).__name__ == "PostgresDialect"

    def test_postgresql_alias(self):
        d = resolve_dialect("postgresql")
        assert type(d).__name__ == "PostgresDialect"

    def test_mssql(self):
        d = resolve_dialect("mssql")
        assert type(d).__name__ == "MSSQLDialect"

    def test_tds_alias(self):
        d = resolve_dialect("tds")
        assert type(d).__name__ == "MSSQLDialect"

    def test_oracle(self):
        d = resolve_dialect("oracle")
        assert type(d).__name__ == "OracleDialect"

    def test_unknown_raises_config_error(self):
        with pytest.raises(ConfigError, match="Desteklenmeyen"):
            resolve_dialect("mysql")

    def test_case_insensitive(self):
        d = resolve_dialect("POSTGRES")
        assert type(d).__name__ == "PostgresDialect"


# ---------------------------------------------------------------------------
# combine_where
# ---------------------------------------------------------------------------


class TestCombineWhere:
    def test_both_present(self):
        assert combine_where("a > 1", "b < 10") == "(a > 1) AND (b < 10)"

    def test_base_only(self):
        assert combine_where("a > 1", None) == "a > 1"

    def test_partition_only(self):
        assert combine_where(None, "b < 10") == "b < 10"

    def test_neither(self):
        assert combine_where(None, None) is None


# ---------------------------------------------------------------------------
# aggregate_results
# ---------------------------------------------------------------------------


class TestAggregateResults:
    def test_empty_list(self):
        r = aggregate_results([])
        assert r.rows == 0
        assert r.partitions_completed == 0
        assert r.errors == []

    def test_single_result(self):
        r = aggregate_results([FlowResult(100, 2.0, 50.0, 1)])
        assert r.rows == 100
        assert r.duration_seconds == 2.0
        assert r.partitions_completed == 1

    def test_multiple_results(self):
        results = [
            FlowResult(100, 2.0, 50.0, 1),
            FlowResult(200, 3.0, 66.67, 1),
        ]
        r = aggregate_results(results)
        assert r.rows == 300
        assert r.duration_seconds == 3.0  # max
        assert r.partitions_completed == 2
        assert r.throughput == round(300 / 3.0, 2)

    def test_errors_collected(self):
        results = [
            FlowResult(50, 1.0, 50.0, 1, errors=["err1"]),
            FlowResult(0, 0.5, 0.0, 1, errors=["err2", "err3"]),
        ]
        r = aggregate_results(results)
        assert r.errors == ["err1", "err2", "err3"]


# ---------------------------------------------------------------------------
# FFEngineOperator.__init__
# ---------------------------------------------------------------------------


class TestFFEngineOperatorInit:
    def test_required_params(self):
        op = FFEngineOperator(
            config_path="/etc/cfg.yaml",
            task_group_id="t1",
            source_conn_id="src_pg",
            target_conn_id="tgt_pg",
        )
        assert op.config_path == "/etc/cfg.yaml"
        assert op.task_group_id == "t1"
        assert op.source_conn_id == "src_pg"
        assert op.target_conn_id == "tgt_pg"

    def test_defaults(self):
        op = FFEngineOperator(
            config_path="a", task_group_id="b",
            source_conn_id="s", target_conn_id="t",
        )
        assert op.engine == "auto"
        assert op.task_id == "ffengine_etl"

    def test_template_fields(self):
        assert "config_path" in FFEngineOperator.template_fields
        assert "task_group_id" in FFEngineOperator.template_fields
        assert "source_conn_id" in FFEngineOperator.template_fields
        assert "target_conn_id" in FFEngineOperator.template_fields


# ---------------------------------------------------------------------------
# FFEngineOperator.execute() — tam orkestrasyon testleri
# ---------------------------------------------------------------------------


def _make_operator(**overrides):
    defaults = {
        "config_path": "/tmp/cfg.yaml",
        "task_group_id": "task_001",
        "source_conn_id": "src_conn",
        "target_conn_id": "tgt_conn",
    }
    defaults.update(overrides)
    return FFEngineOperator(**defaults)


def _default_mapping_result():
    from ffengine.mapping.resolver import MappingResult
    from ffengine.dialects.base import ColumnInfo

    return MappingResult(
        source_columns=["id", "name"],
        target_columns=["id", "name"],
        target_columns_meta=[
            ColumnInfo("id", "INTEGER"),
            ColumnInfo("name", "VARCHAR"),
        ],
    )


class TestFFEngineOperatorExecute:
    """
    execute() testlerinde tüm dış bağımlılıklar mock'lanır.
    Lazy import nedeniyle kaynak modül yolunda patch yapılır.
    """

    @pytest.fixture(autouse=True)
    def _patch_all(self):
        with (
            patch(_P_ADAPTER) as mock_adapter,
            patch(_P_LOADER) as mock_loader,
            patch(_P_BINDER) as mock_binder,
            patch(_P_DBSESS) as mock_db,
            patch(_P_MAPPING) as mock_mapping,
            patch(_P_PART) as mock_part,
            patch(_P_WRITER) as mock_writer,
            patch(_P_FLOW) as mock_etl,
        ):
            mock_adapter.get_connection_params.return_value = {
                "host": "localhost", "port": 5432,
                "user": "u", "password": "p",
                "database": "db", "conn_type": "postgres",
            }

            self.task_config = {
                "source_schema": "public",
                "source_table": "orders",
                "source_type": "table",
                "load_method": "append",
                "column_mapping_mode": "source",
                "passthrough_full": True,
            }
            mock_loader.return_value.load.return_value = dict(self.task_config)
            mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)

            mock_session = MagicMock()
            mock_session.conn = MagicMock()
            mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_db.return_value.__exit__ = MagicMock(return_value=False)

            mock_mapping.return_value.resolve.return_value = _default_mapping_result()

            mock_part.return_value.plan.return_value = [
                {"part_id": 0, "where": None}
            ]

            mock_writer.return_value.prepare.return_value = None

            mock_etl.return_value.run_flow_task.return_value = FlowResult(
                rows=100, duration_seconds=1.5, throughput=66.67,
                partitions_completed=1, errors=[],
            )

            self.mock_adapter = mock_adapter
            self.mock_loader = mock_loader
            self.mock_binder = mock_binder
            self.mock_db = mock_db
            self.mock_session = mock_session
            self.mock_mapping = mock_mapping
            self.mock_part = mock_part
            self.mock_writer = mock_writer
            self.mock_etl = mock_etl

            yield

    def test_happy_path_single_partition(self):
        op = _make_operator()
        result = op.execute()
        assert result["rows"] == 100
        assert result["partitions_completed"] == 1
        assert result["errors"] == []

    def test_happy_path_multi_partition(self):
        self.mock_part.return_value.plan.return_value = [
            {"part_id": 0, "where": "id < 500"},
            {"part_id": 1, "where": "id >= 500"},
        ]
        self.mock_etl.return_value.run_flow_task.side_effect = [
            FlowResult(50, 1.0, 50.0, 1),
            FlowResult(50, 1.2, 41.67, 1),
        ]
        op = _make_operator()
        result = op.execute()
        assert result["rows"] == 100
        assert result["partitions_completed"] == 2
        assert self.mock_etl.return_value.run_flow_task.call_count == 2

    def test_prepare_called_once(self):
        self.mock_part.return_value.plan.return_value = [
            {"part_id": 0, "where": None},
            {"part_id": 1, "where": None},
        ]
        self.mock_etl.return_value.run_flow_task.return_value = FlowResult(
            50, 1.0, 50.0, 1,
        )
        op = _make_operator()
        op.execute()
        self.mock_writer.return_value.prepare.assert_called_once()

    def test_skip_prepare_used(self):
        op = _make_operator()
        op.execute()
        call_kwargs = self.mock_etl.return_value.run_flow_task.call_args
        assert call_kwargs.kwargs.get("skip_prepare") is True

    def test_where_combination(self):
        """Base WHERE + partition WHERE AND ile birleştirilir."""
        self.mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: {
            **cfg, "_resolved_where": "status = 'ACTIVE'"
        }
        self.mock_part.return_value.plan.return_value = [
            {"part_id": 0, "where": "id < 500"},
        ]
        op = _make_operator()
        op.execute()

        call_kwargs = self.mock_etl.return_value.run_flow_task.call_args
        effective = call_kwargs.kwargs["task_config"]
        assert effective["_resolved_where"] == "(status = 'ACTIVE') AND (id < 500)"

    def test_mapping_integration(self):
        """MappingResolver sonucu task_config'e yazılır."""
        op = _make_operator()
        op.execute()

        call_kwargs = self.mock_etl.return_value.run_flow_task.call_args
        effective = call_kwargs.kwargs["task_config"]
        assert effective["source_columns"] == ["id", "name"]
        assert effective["target_columns"] == ["id", "name"]

    def test_sql_bindings_resolved_after_sessions_open(self):
        self.task_config.update(
            {
                "where": "id > :min_id",
                "bindings": [
                    {
                        "variable_name": "min_id",
                        "binding_source": "default",
                        "default_value": "100",
                    }
                ],
            }
        )
        self.mock_loader.return_value.load.return_value = dict(self.task_config)
        self.mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)
        self.mock_binder.return_value.resolve_sql_bindings.side_effect = lambda cfg, **_: {
            **cfg,
            "_resolved_where": "id > 100",
        }

        op = _make_operator()
        op.execute()

        self.mock_binder.return_value.resolve_sql_bindings.assert_called_once()
        call_kwargs = self.mock_etl.return_value.run_flow_task.call_args
        effective = call_kwargs.kwargs["task_config"]
        assert effective["_resolved_where"] == "id > 100"

    def test_xcom_push(self):
        """XCom push: rows_transferred, duration_seconds, rows_per_second."""
        ti = MagicMock()
        context = {"ti": ti}
        op = _make_operator()
        op.execute(context)

        push_calls = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}
        assert "rows_transferred" in push_calls
        assert "duration_seconds" in push_calls
        assert "rows_per_second" in push_calls
        assert "retry_telemetry" in push_calls
        assert push_calls["rows_transferred"] == 100
        assert isinstance(push_calls["retry_telemetry"], dict)

    def test_config_loader_called_with_correct_args(self):
        op = _make_operator(config_path="/a/b.yaml", task_group_id="tg1")
        op.execute()
        self.mock_loader.return_value.load.assert_called_once_with("/a/b.yaml", "tg1")

    def test_adapter_called_for_both_connections(self):
        op = _make_operator(source_conn_id="src_x", target_conn_id="tgt_y")
        op.execute()
        calls = self.mock_adapter.get_connection_params.call_args_list
        assert any(c.args == ("src_x",) for c in calls)
        assert any(c.args == ("tgt_y",) for c in calls)


# ---------------------------------------------------------------------------
# Hata senaryoları
# ---------------------------------------------------------------------------


class TestFFEngineOperatorErrors:
    def test_bad_source_conn_type_raises_config_error(self):
        with (
            patch(_P_LOADER) as mock_loader,
            patch(_P_ADAPTER) as mock_adapter,
        ):
            mock_loader.return_value.load.return_value = {"source_type": "table"}
            mock_adapter.get_connection_params.side_effect = [
                {"conn_type": "mysql"},
                {"conn_type": "postgres"},
            ]
            op = _make_operator()
            with pytest.raises(ConfigError, match="Desteklenmeyen"):
                op.execute()

    def test_config_error_propagates(self):
        with patch(_P_LOADER) as mock_loader:
            mock_loader.return_value.load.side_effect = ConfigError("dosya bulunamadı")
            op = _make_operator()
            with pytest.raises(ConfigError, match="dosya bulunamadı"):
                op.execute()

    def test_unknown_error_normalized_to_engine_error(self):
        with patch(_P_LOADER) as mock_loader:
            mock_loader.return_value.load.side_effect = RuntimeError("unexpected boom")
            op = _make_operator()
            with pytest.raises(EngineError, match="unexpected boom"):
                op.execute()

    def test_partition_where_none_preserves_base_where(self):
        """Partition where=None ise base_where korunur."""
        with (
            patch(_P_ADAPTER) as mock_adapter,
            patch(_P_LOADER) as mock_loader,
            patch(_P_BINDER) as mock_binder,
            patch(_P_DBSESS) as mock_db,
            patch(_P_MAPPING) as mock_mapping,
            patch(_P_PART) as mock_part,
            patch(_P_WRITER),
            patch(_P_FLOW) as mock_etl,
        ):
            mock_adapter.get_connection_params.return_value = {
                "conn_type": "postgres", "host": "h", "database": "d",
            }
            mock_loader.return_value.load.return_value = {}
            mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: {
                **cfg, "_resolved_where": "year = 2026"
            }
            mock_session = MagicMock()
            mock_session.conn = MagicMock()
            mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_db.return_value.__exit__ = MagicMock(return_value=False)

            mock_mapping.return_value.resolve.return_value = _default_mapping_result()
            mock_part.return_value.plan.return_value = [
                {"part_id": 0, "where": None},
            ]
            mock_etl.return_value.run_flow_task.return_value = FlowResult(
                10, 0.1, 100.0, 1,
            )

            op = _make_operator()
            op.execute()

            call_kwargs = mock_etl.return_value.run_flow_task.call_args
            effective = call_kwargs.kwargs["task_config"]
            assert effective["_resolved_where"] == "year = 2026"
