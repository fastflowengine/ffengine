"""
F1.4/F1.5 — FFEngineOperator.execute() file-endpoint branch tests.

Verifies that a file source / file target bypasses DB dialect+session
resolution, forces a single partition (M=1), and routes the right handle +
skip_prepare into FlowManager. All external deps are mocked (FlowManager is a
mock, so no real IO happens here — the read/write path is covered by
test_file_pipeline.py). Also covers file_path {{ }} templating.
"""

from unittest.mock import MagicMock, patch

import pytest

from ffengine.airflow.operator import (
    FFEngineOperator,
    _render_file_paths,
)
from ffengine.core.base_engine import FlowResult
from ffengine.errors.exceptions import ConfigError
from ffengine.pipeline.file_transport import FileSourceContext, FileTargetContext

_P_ADAPTER = "ffengine.db.airflow_adapter.AirflowConnectionAdapter"
_P_LOADER = "ffengine.config.loader.ConfigLoader"
_P_BINDER = "ffengine.config.binding_resolver.BindingResolver"
_P_DBSESS = "ffengine.db.session.DBSession"
_P_MAPPING = "ffengine.mapping.MappingResolver"
_P_PART = "ffengine.partition.Partitioner"
_P_WRITER = "ffengine.pipeline.target_writer.TargetWriter"
_P_FLOW = "ffengine.core.flow_manager.FlowManager"


def _mapping_result():
    from ffengine.dialects.base import ColumnInfo
    from ffengine.mapping.resolver import MappingResult

    return MappingResult(
        source_columns=["id", "name"],
        target_columns=["id", "name"],
        target_columns_meta=[ColumnInfo("id", "INTEGER"), ColumnInfo("name", "VARCHAR")],
    )


def _operator():
    return FFEngineOperator(
        config_path="/tmp/cfg.yaml",
        task_group_id="t1",
        source_conn_id="src_conn",
        target_conn_id="tgt_conn",
    )


def _run(config, conn_types):
    """Execute the operator with mocked deps; return the collaborators."""
    with (
        patch(_P_ADAPTER) as adapter,
        patch(_P_LOADER) as loader,
        patch(_P_BINDER) as binder,
        patch(_P_DBSESS) as dbsess,
        patch(_P_MAPPING) as mapping,
        patch(_P_PART) as part,
        patch(_P_WRITER) as writer,
        patch(_P_FLOW) as flow,
    ):
        adapter.get_connection_params.side_effect = [
            {"conn_type": conn_types[0], "host": "h", "database": "d"},
            {"conn_type": conn_types[1], "host": "h", "database": "d"},
        ]
        loader.return_value.load.return_value = dict(config)
        binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)
        session = MagicMock()
        session.conn = MagicMock()
        dbsess.return_value.__enter__ = MagicMock(return_value=session)
        dbsess.return_value.__exit__ = MagicMock(return_value=False)
        mapping.return_value.resolve.return_value = _mapping_result()
        part.return_value.plan.return_value = [{"part_id": 0, "where": None}]
        flow.return_value.run_flow_task.return_value = FlowResult(2, 0.1, 20.0, 1)

        _operator().execute()
        return {
            "run_kwargs": flow.return_value.run_flow_task.call_args.kwargs,
            "part": part,
            "writer": writer,
            "dbsess": dbsess,
        }


def test_file_source_to_db_target():
    cfg = {
        "task_group_id": "t1",
        "source_type": "csv",
        "file_path": "/incoming/orders.csv",
        "column_mapping_mode": "mapping_file",
        "mapping_file": "/m.yaml",
        "load_method": "append",
        "target_schema": "dwh",
        "target_table": "orders",
    }
    out = _run(cfg, conn_types=["fs", "postgres"])
    kw = out["run_kwargs"]
    assert isinstance(kw["src_session"], FileSourceContext)
    assert kw["src_session"].file_path == "/incoming/orders.csv"
    assert kw["src_dialect"] is None          # source dialect bypassed
    assert kw["tgt_dialect"] is not None       # DB target keeps its dialect
    assert kw["skip_prepare"] is True          # DB target prepared in operator
    out["part"].return_value.plan.assert_not_called()   # file → single partition
    out["writer"].assert_called_once()          # DB TargetWriter prepared once
    assert out["dbsess"].call_count == 1        # only the target DB session


def test_db_source_to_file_target():
    cfg = {
        "task_group_id": "t1",
        "source_type": "table",
        "source_schema": "public",
        "source_table": "orders",
        "target_type": "file",
        "target_file_path": "/out/orders.csv",
        "load_method": "append",
    }
    out = _run(cfg, conn_types=["postgres", "sftp"])
    kw = out["run_kwargs"]
    assert isinstance(kw["tgt_session"], FileTargetContext)
    assert kw["tgt_session"].file_path == "/out/orders.csv"
    assert kw["tgt_dialect"] is None            # target dialect bypassed
    assert kw["src_dialect"] is not None         # DB source keeps its dialect
    assert kw["skip_prepare"] is False           # file writer prepares in FlowManager
    out["part"].return_value.plan.assert_not_called()
    out["writer"].assert_not_called()            # no DB TargetWriter for a file target
    assert out["dbsess"].call_count == 1         # only the source DB session


# ---------------------------------------------------------------------------
# file_path templating
# ---------------------------------------------------------------------------


def test_render_file_paths_substitutes_binding_values():
    ctx = {
        "airflow_params": {},
        "dag_run_conf": {},
        "binding_values": {"run_date": "2026-07-27"},
    }
    cfg = {"file_path": "/in/orders_{{ run_date }}.csv", "target_file_path": "/o.csv"}
    out = _render_file_paths(cfg, ctx)
    assert out["file_path"] == "/in/orders_2026-07-27.csv"
    assert out["target_file_path"] == "/o.csv"


def test_render_file_paths_fails_loud_on_unknown_token():
    ctx = {"airflow_params": {}, "dag_run_conf": {}, "binding_values": {}}
    with pytest.raises(ConfigError, match="cozulemeyen"):
        _render_file_paths({"file_path": "/in/{{ missing }}.csv"}, ctx)
