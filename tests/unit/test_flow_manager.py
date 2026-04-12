import pytest
from unittest.mock import MagicMock, patch, call
from ffengine.core.flow_manager import FlowManager, PythonEngine
from ffengine.core.base_engine import FlowResult
from ffengine.errors import EngineError


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def dialect():
    d = MagicMock()
    d.quote_identifier.side_effect = lambda n: f'"{n}"'
    d.generate_bulk_insert_query.return_value = "INSERT INTO ..."
    d.generate_ddl.return_value = "CREATE TABLE ..."
    return d


@pytest.fixture
def src_session(dialect):
    s = MagicMock()
    s.conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchmany.side_effect = [[(1, "Alice"), (2, "Bob")], []]
    s.cursor.return_value = cursor
    return s


@pytest.fixture
def tgt_session(dialect):
    s = MagicMock()
    s.conn = MagicMock()
    cursor = MagicMock()
    cursor.executemany.return_value = None
    s.cursor.return_value = cursor
    return s


@pytest.fixture
def task_config():
    return {
        "load_method": "append",
        "source_schema": "src",
        "source_table": "employees",
        "source_columns": ["id", "name"],
        "target_schema": "tgt",
        "target_table": "employees",
        "target_columns": ["id", "name"],
        "target_columns_meta": [],
        "batch_size": 1000,
    }


# ------------------------------------------------------------------
# PythonEngine.is_available()
# ------------------------------------------------------------------


def test_python_engine_is_available():
    engine = PythonEngine()
    assert engine.is_available() is True


# ------------------------------------------------------------------
# PythonEngine.run() — C05 sonrası ConfigLoader'a bağlı
# ------------------------------------------------------------------


def test_python_engine_run_raises_config_error_for_missing_file():
    # C05 aktif: var olmayan dosya → ConfigError (NotImplementedError değil)
    from ffengine.errors.exceptions import ConfigError

    engine = PythonEngine()
    with pytest.raises(ConfigError, match="bulunamadı"):
        engine.run("path/to/config.yaml", "task_001")


def test_python_engine_run_raises_config_error_for_missing_sessions(tmp_path):
    # Geçerli YAML ama session/dialect enjeksiyonu yok → ConfigError
    import textwrap
    from ffengine.errors.exceptions import ConfigError

    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(textwrap.dedent("""\
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
    """))
    engine = PythonEngine()
    with pytest.raises(ConfigError, match="session"):
        engine.run(str(cfg), "t1")


# ------------------------------------------------------------------
# FlowManager.run_flow_task() — başarılı akış
# ------------------------------------------------------------------


def test_run_flow_task_returns_etl_result(src_session, tgt_session, dialect, task_config):
    manager = FlowManager()
    result = manager.run_flow_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=dialect,
        tgt_dialect=dialect,
        task_config=task_config,
    )
    assert isinstance(result, FlowResult)
    assert result.rows == 2
    assert result.partitions_completed == 1
    assert result.errors == []


def test_run_flow_task_duration_positive(src_session, tgt_session, dialect, task_config):
    manager = FlowManager()
    result = manager.run_flow_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=dialect,
        tgt_dialect=dialect,
        task_config=task_config,
    )
    assert result.duration_seconds >= 0


def test_run_flow_task_throughput_non_negative(src_session, tgt_session, dialect, task_config):
    manager = FlowManager()
    result = manager.run_flow_task(
        src_session=src_session,
        tgt_session=tgt_session,
        src_dialect=dialect,
        tgt_dialect=dialect,
        task_config=task_config,
    )
    assert result.throughput >= 0


# ------------------------------------------------------------------
# Partition spec WHERE enjeksiyonu
# ------------------------------------------------------------------


def test_run_flow_task_partition_spec_injects_where(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.SourceReader") as MockReader:
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = iter([])
        MockReader.return_value = mock_reader_instance

        manager = FlowManager()
        manager.run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=dialect,
            tgt_dialect=dialect,
            task_config=task_config,
            partition_spec={"part_id": 2, "where": "region = 'EU'"},
        )

        init_config = MockReader.call_args[0][1]
        assert init_config.get("_resolved_where") == "region = 'EU'"


# ------------------------------------------------------------------
# Hata → rollback tetiklenmesi
# ------------------------------------------------------------------


def test_run_flow_task_rollback_on_write_error(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.TargetWriter") as MockWriter:
        mock_writer = MagicMock()
        mock_writer.write_batch.side_effect = RuntimeError("insert failed")
        MockWriter.return_value = mock_writer

        manager = FlowManager()
        with pytest.raises(EngineError, match="insert failed"):
            manager.run_flow_task(
                src_session=src_session,
                tgt_session=tgt_session,
                src_dialect=dialect,
                tgt_dialect=dialect,
                task_config=task_config,
            )

        mock_writer.rollback_batch.assert_called()


def test_run_flow_task_rollback_called_once_on_error(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.TargetWriter") as MockWriter:
        mock_writer = MagicMock()
        mock_writer.write_batch.side_effect = Exception("fail")
        MockWriter.return_value = mock_writer

        manager = FlowManager()
        with pytest.raises(EngineError):
            manager.run_flow_task(
                src_session=src_session,
                tgt_session=tgt_session,
                src_dialect=dialect,
                tgt_dialect=dialect,
                task_config=task_config,
            )

        assert mock_writer.rollback_batch.call_count == 1


# ------------------------------------------------------------------
# BaseEngine.detect()
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# skip_prepare parametresi (C07)
# ------------------------------------------------------------------


def test_skip_prepare_true_skips_writer_prepare(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.TargetWriter") as MockWriter:
        mock_writer = MagicMock()
        mock_writer.write_batch.return_value = 0
        MockWriter.return_value = mock_writer

        manager = FlowManager()
        manager.run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=dialect,
            tgt_dialect=dialect,
            task_config=task_config,
            skip_prepare=True,
        )

        mock_writer.prepare.assert_not_called()


def test_skip_prepare_false_calls_writer_prepare(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.TargetWriter") as MockWriter:
        mock_writer = MagicMock()
        mock_writer.write_batch.return_value = 0
        MockWriter.return_value = mock_writer

        manager = FlowManager()
        manager.run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=dialect,
            tgt_dialect=dialect,
            task_config=task_config,
            skip_prepare=False,
        )

        mock_writer.prepare.assert_called_once()


def test_skip_prepare_default_is_false(src_session, tgt_session, dialect, task_config):
    with patch("ffengine.core.flow_manager.TargetWriter") as MockWriter:
        mock_writer = MagicMock()
        mock_writer.write_batch.return_value = 0
        MockWriter.return_value = mock_writer

        manager = FlowManager()
        manager.run_flow_task(
            src_session=src_session,
            tgt_session=tgt_session,
            src_dialect=dialect,
            tgt_dialect=dialect,
            task_config=task_config,
        )

        mock_writer.prepare.assert_called_once()


# ------------------------------------------------------------------
# BaseEngine.detect()
# ------------------------------------------------------------------


def test_detect_community_returns_python_engine():
    from ffengine.core.base_engine import BaseEngine
    engine = BaseEngine.detect("community")
    assert isinstance(engine, PythonEngine)


def test_detect_auto_fallback_to_python_engine():
    from ffengine.core.base_engine import BaseEngine
    engine = BaseEngine.detect("auto")
    assert isinstance(engine, PythonEngine)
