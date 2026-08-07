"""
C07 — FFEngineOperator birim testleri.

Kapsam: resolve_dialect, combine_where, aggregate_results,
        FFEngineOperator init/execute, hata senaryoları, XCom.
"""

import logging

import pytest
from unittest.mock import MagicMock, patch

from ffengine.airflow.operator import (
    build_runtime_binding_context,
    resolve_dialect,
    combine_where,
    aggregate_results,
    run_partition_for_task,
    FFEngineOperator,
)
from ffengine.core.base_engine import FlowResult
from ffengine.errors.exceptions import ConfigError, ConnectionError, EngineError
import ffengine.airflow.operator as operator_module

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


class _DagRun:
    conf = {"run_date": "2026-07-20", "log_level": "DEBUG"}


class _DeclaredParam:
    def __init__(self, param_type):
        self.schema = {"x-ffengine-type": param_type}


class _DeclaredParams:
    def __init__(self, **types):
        self._params = {name: _DeclaredParam(value) for name, value in types.items()}

    def get_param(self, name):
        return self._params[name]


class _Dag:
    def __init__(self, **types):
        self.params = _DeclaredParams(**types)


def test_runtime_binding_context_merges_direct_xcom_without_values_in_logs():
    ti = MagicMock()
    ti.xcom_pull.side_effect = [
        {"run_date": "2026-07-19"},
        {"batch_limit": 50},
    ]

    context = build_runtime_binding_context(
        {
            "params": {"run_date": "2026-07-18", "log_level": "default"},
            "dag_run": _DagRun(),
            "ti": ti,
        },
        airflow_variables={},
        binding_task_ids=["binding__date", "binding__limit"],
    )

    assert context["binding_values"] == {
        "run_date": "2026-07-19",
        "batch_limit": 50,
    }
    assert context["dag_run_conf"]["run_date"] == "2026-07-20"


def test_runtime_binding_context_normalizes_declared_trigger_values_without_mutation():
    dag_run = MagicMock()
    dag_run.conf = {
        "batch_limit": "3",
        "ratio": "3.5",
        "enabled": "true",
        "label": "3",
    }

    context = build_runtime_binding_context(
        {
            "params": {},
            "dag": _Dag(
                batch_limit="integer",
                ratio="number",
                enabled="boolean",
                label="string",
            ),
            "dag_run": dag_run,
        },
        airflow_variables={},
    )

    assert context["dag_run_conf"] == {
        "batch_limit": 3,
        "ratio": 3.5,
        "enabled": True,
        "label": "3",
    }
    assert dag_run.conf["batch_limit"] == "3"


def test_runtime_binding_context_preserves_native_trigger_value_and_omits_null():
    dag_run = MagicMock()
    dag_run.conf = {"batch_limit": None, "native_limit": 3}
    ti = MagicMock()
    ti.xcom_pull.return_value = {"batch_limit": 2}

    context = build_runtime_binding_context(
        {
            "params": {},
            "dag": _Dag(batch_limit="integer", native_limit="integer"),
            "dag_run": dag_run,
            "ti": ti,
        },
        airflow_variables={},
        binding_task_ids=["binding__limit"],
    )

    assert context["dag_run_conf"] == {"native_limit": 3}
    assert context["binding_values"] == {"batch_limit": 2}


def test_runtime_binding_context_rejects_invalid_declared_trigger_value():
    dag_run = MagicMock()
    dag_run.conf = {"batch_limit": "3x"}

    with pytest.raises(ConfigError, match="batch_limit.*integer"):
        build_runtime_binding_context(
            {
                "params": {},
                "dag": _Dag(batch_limit="integer"),
                "dag_run": dag_run,
            },
            airflow_variables={},
        )


def test_runtime_binding_context_rejects_conflicting_compiled_xcom_values():
    ti = MagicMock()
    ti.xcom_pull.side_effect = [
        {"run_date": "2026-07-19"},
        {"run_date": "2026-07-20"},
    ]

    with pytest.raises(ConfigError, match="Conflicting compiled Binding XCom values"):
        build_runtime_binding_context(
            {"params": {}, "dag_run": _DagRun(), "ti": ti},
            airflow_variables={},
            binding_task_ids=["binding__date_a", "binding__date_b"],
        )


def test_runtime_binding_context_selects_parameter_from_compiled_producer_once():
    ti = MagicMock()
    values = {
        "binding__initial": {"test1": 1, "test2": 10},
        "binding__updated": {"test1": 2},
    }
    ti.xcom_pull.side_effect = lambda task_ids, key: values[task_ids]

    context = build_runtime_binding_context(
        {"params": {}, "dag_run": _DagRun(), "ti": ti},
        airflow_variables={},
        binding_sources={
            "test1": "binding__updated",
            "test2": "binding__initial",
        },
    )

    assert context["binding_values"] == {"test1": 2, "test2": 10}
    assert ti.xcom_pull.call_count == 2
    assert {
        call.kwargs["task_ids"] for call in ti.xcom_pull.call_args_list
    } == {"binding__initial", "binding__updated"}


def test_runtime_binding_context_fails_when_selected_parameter_is_missing():
    ti = MagicMock()
    ti.xcom_pull.return_value = {"test1": 2}

    with pytest.raises(ConfigError, match="test2.*binding__updated"):
        build_runtime_binding_context(
            {"params": {}, "dag_run": _DagRun(), "ti": ti},
            airflow_variables={},
            binding_sources={"test2": "binding__updated"},
        )


def test_airflow_variable_proxy_reads_used_key_once():
    proxy = operator_module._AirflowVarProxy()
    with patch("airflow.models.Variable.get", return_value="2026-07-21") as get:
        assert "etl.business_date" in proxy
        assert proxy["etl.business_date"] == "2026-07-21"
    get.assert_called_once_with("etl.business_date")


def test_airflow_variable_proxy_distinguishes_missing_key_from_service_error():
    proxy = operator_module._AirflowVarProxy()
    with patch("airflow.models.Variable.get", side_effect=KeyError("missing")):
        assert "missing.key" not in proxy
    with patch("airflow.models.Variable.get", side_effect=RuntimeError("offline")):
        with pytest.raises(RuntimeError, match="offline"):
            _ = "etl.business_date" in proxy


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
            config_path="a",
            task_group_id="b",
            source_conn_id="s",
            target_conn_id="t",
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
                "host": "localhost",
                "port": 5432,
                "user": "u",
                "password": "p",
                "database": "db",
                "conn_type": "postgres",
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

            mock_part.return_value.plan.return_value = [{"part_id": 0, "where": None}]

            mock_writer.return_value.prepare.return_value = None

            # F3.3: gerçek run_flow_task artık muhasebe sayaçlarını da
            # doldurur (denklik streamer'da doğrulanmıştır) — mock gerçeği
            # yansıtır.
            mock_etl.return_value.run_flow_task.return_value = FlowResult(
                rows=100,
                duration_seconds=1.5,
                throughput=66.67,
                partitions_completed=1,
                errors=[],
                rows_read=100,
                rows_written=100,
                rows_rejected=0,
                reconciliation_status="passed",
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

    def test_debug_log_level_is_run_scoped_and_does_not_log_values(self):
        dag_run = MagicMock()
        dag_run.conf = {"log_level": "DEBUG", "token": "do-not-log-this"}
        initial_level = operator_module._log.level
        with (
            patch.object(
                operator_module._log,
                "setLevel",
                wraps=operator_module._log.setLevel,
            ) as set_level,
            patch.object(operator_module, "_log_structured") as structured_log,
        ):
            _make_operator().execute(
                {"params": {"log_level": "default"}, "dag_run": dag_run}
            )

        assert set_level.call_args_list[0].args == (logging.DEBUG,)
        assert set_level.call_args_list[-1].args == (initial_level,)
        assert operator_module._log.level == initial_level
        assert "do-not-log-this" not in str(structured_log.call_args_list)

    def test_binding_xcom_cannot_override_builtin_log_level(self):
        runtime_context = {
            "airflow_variables": {},
            "airflow_params": {"log_level": "default"},
            "binding_values": {"log_level": "DEBUG"},
            "dag_run_conf": {},
        }

        with pytest.raises(
            ConfigError,
            match="Built-in DAG parameter 'log_level' cannot be assigned",
        ):
            _make_operator().execute(runtime_context)

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
            50,
            1.0,
            50.0,
            1,
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
            **cfg,
            "_resolved_where": "status = 'ACTIVE'",
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
                "where": "id > {{ min_id }}",
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
        self.mock_binder.return_value.resolve_sql_bindings.side_effect = (
            lambda cfg, **_: {
                **cfg,
                "_resolved_where": "id > 100",
            }
        )

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

        push_calls = {
            c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list
        }
        assert "rows_transferred" in push_calls
        assert "duration_seconds" in push_calls
        assert "rows_per_second" in push_calls
        assert "retry_telemetry" in push_calls
        assert push_calls["rows_transferred"] == 100
        assert isinstance(push_calls["retry_telemetry"], dict)

    def test_reconciliation_fields_in_return_value_xcom_contract(self):
        """T-F3.3-5: muhasebe alanları mevcut return_value XCom'unda taşınır;
        YENİ xcom_push anahtarı açılmaz (mevcut dört anahtar aynen kalır)."""
        ti = MagicMock()
        op = _make_operator()
        summary = op.execute({"ti": ti})

        assert summary["rows_read"] == 100
        assert summary["rows_written"] == 100
        assert summary["rows_rejected"] == 0
        assert summary["reconciliation_status"] == "passed"
        assert summary["rows"] == summary["rows_written"]

        pushed_keys = {c.kwargs["key"] for c in ti.xcom_push.call_args_list}
        assert pushed_keys == {
            "rows_transferred",
            "duration_seconds",
            "rows_per_second",
            "retry_telemetry",
        }

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


class TestRunPartitionTaskLogging:
    def test_run_partition_logs_effective_where(self):
        task_config = {"task_group_id": "tg1", "_resolved_where": "status = 'ACTIVE'"}
        src_params = {"conn_type": "postgres"}
        tgt_params = {"conn_type": "postgres"}
        src_dialect = MagicMock()
        tgt_dialect = MagicMock()
        resolver = MagicMock()
        airflow_ctx = {}

        src_session = MagicMock()
        tgt_session = MagicMock()

        sessions = [src_session, tgt_session]

        def _dbsession_factory(*_args, **_kwargs):
            cm = MagicMock()
            cm.__enter__.return_value = sessions.pop(0)
            cm.__exit__.return_value = False
            return cm

        with (
            patch(
                "ffengine.airflow.operator._resolve_task_runtime",
                return_value=(
                    task_config,
                    src_params,
                    tgt_params,
                    src_dialect,
                    tgt_dialect,
                    resolver,
                    airflow_ctx,
                ),
            ),
            patch(
                "ffengine.airflow.operator._resolve_sql_bindings_if_needed",
                side_effect=lambda **kwargs: kwargs["task_config"],
            ),
            patch(
                "ffengine.airflow.operator._attach_mapping_if_needed",
                side_effect=lambda **kwargs: kwargs["task_config"],
            ),
            patch(_P_DBSESS, side_effect=_dbsession_factory),
            patch(_P_FLOW) as mock_flow,
            patch("ffengine.airflow.operator._log_structured") as mock_log_structured,
        ):
            mock_flow.return_value.run_flow_task.return_value = FlowResult(
                rows=10,
                duration_seconds=1.0,
                throughput=10.0,
                partitions_completed=1,
                errors=[],
            )

            payload = run_partition_for_task(
                config_path="/tmp/cfg.yaml",
                task_group_id="tg1",
                source_conn_id="src",
                target_conn_id="tgt",
                partition_spec={"part_id": 2, "where": "id < 500"},
            )

            expected_where = "(status = 'ACTIVE') AND (id < 500)"
            assert payload["partition_id"] == 2
            log_kwargs = mock_log_structured.call_args.kwargs
            assert log_kwargs["message"] == "run_partition effective where resolved."
            assert log_kwargs["partition_id"] == 2
            assert log_kwargs["base_where"] == "status = 'ACTIVE'"
            assert log_kwargs["partition_where"] == "id < 500"
            assert log_kwargs["effective_where"] == expected_where
            assert log_kwargs["datetime_timezone"] == "UTC"
            assert log_kwargs["datetime_precision"] == "timestamp(6)"
            assert (
                log_kwargs["datetime_boundary_policy"] == "half_open_[lo,hi)_last_lte"
            )

            call_kwargs = mock_flow.return_value.run_flow_task.call_args.kwargs
            assert call_kwargs["task_config"]["_resolved_where"] == expected_where


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
                "conn_type": "postgres",
                "host": "h",
                "database": "d",
            }
            mock_loader.return_value.load.return_value = {}
            mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: {
                **cfg,
                "_resolved_where": "year = 2026",
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
                10,
                0.1,
                100.0,
                1,
            )

            op = _make_operator()
            op.execute()

            call_kwargs = mock_etl.return_value.run_flow_task.call_args
            effective = call_kwargs.kwargs["task_config"]
            assert effective["_resolved_where"] == "year = 2026"

    def test_operator_failed_log_and_xcom_include_db_details(self):
        class FakePgError(Exception):
            __module__ = "psycopg.errors"
            sqlstate = "23505"

        db_exc = FakePgError("duplicate key value violates unique constraint")
        wrapped = ConnectionError.wrap(
            db_exc,
            "Hedefe batch yazimi basarisiz",
            details={"sql_preview": "INSERT INTO t (id) VALUES ('?')"},
        )

        with (
            patch(_P_ADAPTER) as mock_adapter,
            patch(_P_LOADER) as mock_loader,
            patch(_P_BINDER) as mock_binder,
            patch(_P_DBSESS) as mock_db,
            patch(_P_MAPPING) as mock_mapping,
            patch(_P_PART) as mock_part,
            patch(_P_WRITER),
            patch(_P_FLOW) as mock_etl,
            patch("ffengine.airflow.operator._log_structured") as mock_log_structured,
        ):
            mock_adapter.get_connection_params.return_value = {
                "conn_type": "postgres",
                "host": "h",
                "database": "d",
            }
            mock_loader.return_value.load.return_value = {}
            mock_binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)
            mock_session = MagicMock()
            mock_session.conn = MagicMock()
            mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            mock_mapping.return_value.resolve.return_value = _default_mapping_result()
            mock_part.return_value.plan.return_value = [{"part_id": 0, "where": None}]
            mock_etl.return_value.run_flow_task.side_effect = wrapped

            ti = MagicMock()
            op = _make_operator()
            with pytest.raises(ConnectionError):
                op.execute({"ti": ti})

            err_log = mock_log_structured.call_args_list[-1].kwargs
            assert err_log["message"] == "Operator failed."
            assert err_log["db_exception_type"] == "FakePgError"
            assert err_log["db_sqlstate"] == "23505"
            assert err_log["db_driver"] == "psycopg"
            assert "sql_preview" in err_log["error_details"]

            pushed = {
                c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list
            }
            assert "error_summary" in pushed
            assert pushed["error_summary"]["details"]["db_sqlstate"] == "23505"


# ------------------------------------------------------------------
# F3.3 K1 — muhasebe alanlarının XCom/summary ve mapped-partition
# yolundaki taşınması (T-F3.3-5)
# ------------------------------------------------------------------


def _partition_payload(**overrides):
    payload = {
        "rows": 50,
        "duration_seconds": 1.0,
        "throughput": 50.0,
        "partitions_completed": 1,
        "errors": [],
        "partition_id": 0,
        "rows_read": 50,
        "rows_written": 50,
        "rows_rejected": 0,
        "reconciliation_status": "passed",
    }
    payload.update(overrides)
    return payload


def test_aggregate_results_sums_reconciliation_counters():
    from ffengine.airflow.operator import aggregate_results

    parts = [
        FlowResult(
            rows=50,
            duration_seconds=1.0,
            throughput=50.0,
            partitions_completed=1,
            errors=[],
            rows_read=50,
            rows_written=50,
            rows_rejected=0,
            reconciliation_status="passed",
        )
        for _ in range(2)
    ]
    aggregated = aggregate_results(parts)
    assert aggregated.rows_read == 100
    assert aggregated.rows_written == 100
    assert aggregated.rows_rejected == 0
    assert aggregated.reconciliation_status == "passed"


def test_aggregate_results_marks_legacy_when_any_partition_unverified():
    """Bir partition doğrulanmamışsa toplam 'passed' iddia edemez."""
    from ffengine.airflow.operator import aggregate_results

    verified = FlowResult(
        rows=10,
        duration_seconds=1.0,
        throughput=10.0,
        partitions_completed=1,
        errors=[],
        rows_read=10,
        rows_written=10,
        reconciliation_status="passed",
    )
    legacy = FlowResult(
        rows=10, duration_seconds=1.0, throughput=10.0, partitions_completed=1
    )
    assert aggregate_results([verified, legacy]).reconciliation_status == "legacy"


def test_mapped_partition_payload_carries_reconciliation_fields():
    """T-F3.3-5 (mapped yol): bu yolda rows_transferred XCom'u YOK; sayaçlar
    return_value payload'ıyla taşınır ve aggregate aynı alanları üretir."""
    from ffengine.airflow.operator import aggregate_partition_payloads

    aggregated = aggregate_partition_payloads(
        [_partition_payload(partition_id=0), _partition_payload(partition_id=1)]
    )
    assert aggregated["rows_read"] == 100
    assert aggregated["rows_written"] == 100
    assert aggregated["rows_rejected"] == 0
    assert aggregated["reconciliation_status"] == "passed"


def test_legacy_partition_payload_without_counters_still_aggregates():
    """Backward-compat: sayaç taşımayan eski payload rows'tan türetilir."""
    from ffengine.airflow.operator import aggregate_partition_payloads

    legacy = {
        "rows": 30,
        "duration_seconds": 1.0,
        "throughput": 30.0,
        "partitions_completed": 1,
        "errors": [],
    }
    aggregated = aggregate_partition_payloads([legacy])
    assert aggregated["rows"] == 30
    assert aggregated["rows_read"] == 30
    assert aggregated["rows_written"] == 30
    assert aggregated["reconciliation_status"] == "legacy"
