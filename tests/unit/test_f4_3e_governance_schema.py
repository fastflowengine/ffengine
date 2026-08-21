"""
F4.3E — Kanonik governance event modeli (`ffgov` v1) birim testleri.

Kapsam (T-ID eşlemesi state/evidence/F4.3E/EVIDENCE.md içinde):
  şema modülü (başlık/dataset/counter_source/config_revision), operator
  yüzeyleri (summary / mapped-partition / aggregate / error path),
  require_reconciliation preflight'ı, frozen kontrat ve Community-only.

F3.3 testleri DEĞİŞTİRİLMEZ (T-F4.3E-3) — bu dosya yalnız additive davranışı
mühürler.
"""

import inspect
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ffengine.airflow.operator as operator_module
from ffengine.airflow import governance_schema as gov
from ffengine.airflow.operator import (
    FFEngineOperator,
    aggregate_partition_payloads,
    run_partition_for_task,
)
from ffengine.core.base_engine import FlowResult
from ffengine.errors.exceptions import (
    ConfigError,
    ReconciliationError,
    ReconciliationUnavailableError,
    ValidationError,
)

_P_ADAPTER = "ffengine.db.airflow_adapter.AirflowConnectionAdapter"
_P_LOADER = "ffengine.config.loader.ConfigLoader"
_P_BINDER = "ffengine.config.binding_resolver.BindingResolver"
_P_DBSESS = "ffengine.db.session.DBSession"
_P_MAPPING = "ffengine.mapping.MappingResolver"
_P_PART = "ffengine.partition.Partitioner"
_P_WRITER = "ffengine.pipeline.target_writer.TargetWriter"
_P_FLOW = "ffengine.core.flow_manager.FlowManager"

_SECRET = "s3cr3t-pass-do-not-emit"


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


def _fake_ti(**overrides):
    class _Ti:
        dag_id = "flow_dag"
        run_id = "manual__2026-08-17"
        task_id = "task_001"
        map_index = -1

        def __init__(self):
            self.pushed = {}

        def xcom_push(self, *, key, value):
            self.pushed[key] = value

    ti = _Ti()
    for name, value in overrides.items():
        setattr(ti, name, value)
    return ti


# ---------------------------------------------------------------------------
# Şema modülü — başlık / sabitler
# ---------------------------------------------------------------------------


class TestEventHeader:
    def test_header_values(self):  # T-F4.3E-1 (başlık)
        header = gov.event_header(event_scope="task", outcome="succeeded")
        assert header == {
            "schema_version": "ffgov-1",
            "event_scope": "task",
            "outcome": "succeeded",
        }

    def test_invalid_scope_fails_loud(self):
        with pytest.raises(ValueError, match="event_scope"):
            gov.event_header(event_scope="dag", outcome="succeeded")

    def test_invalid_outcome_fails_loud(self):
        with pytest.raises(ValueError, match="outcome"):
            gov.event_header(event_scope="task", outcome="done")

    def test_history_dir_literal_matches_studio_service(self):
        from ffengine.ui import studio_service

        assert gov.STUDIO_HISTORY_DIR_NAME == studio_service.STUDIO_HISTORY_DIR_NAME

    def test_revision_dir_regex_matches_studio_service(self):  # review-fix NIT-1
        from ffengine.ui import studio_service

        assert (
            gov._REVISION_DIR_RE.pattern == studio_service._REVISION_DIR_RE.pattern
        )


# ---------------------------------------------------------------------------
# Şema modülü — dataset kimliği
# ---------------------------------------------------------------------------


class TestDatasetIdentity:
    def test_db_endpoints(self):  # T-F4.3E-4
        identity = gov.dataset_identity(
            {
                "source_type": "table",
                "source_schema": "SALES",
                "source_table": "ORDERS",
                "target_type": "db",
                "target_schema": "DW",
                "target_table": "ORDERS_FACT",
            },
            task_group_id="tg1",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        assert identity["source_dataset"] == "SALES.ORDERS"
        assert identity["source_dataset_resolved"] == "SALES.ORDERS"
        assert identity["target_dataset"] == "DW.ORDERS_FACT"
        assert identity["target_dataset_resolved"] == "DW.ORDERS_FACT"
        assert identity["source_conn_id"] == "src"
        assert identity["target_conn_id"] == "tgt"
        assert identity["source_type"] == "table"
        assert identity["target_type"] == "db"

    def test_templated_file_source(self):  # T-F4.3E-5
        identity = gov.dataset_identity(
            {
                "source_type": "file",
                "source_file_format": "csv",
                "file_path": "/data/in/2026-08-17.csv",
                "target_type": "file",
                "target_file_path": "/data/out/2026-08-17.csv",
            },
            task_group_id="tg1",
            source_conn_id="src",
            target_conn_id="tgt",
            raw_file_path="/data/in/{{ ds }}.csv",
            raw_target_file_path="/data/out/{{ ds }}.csv",
        )
        assert identity["source_dataset"] == "/data/in/{{ ds }}.csv"
        assert identity["source_dataset_resolved"] == "/data/in/2026-08-17.csv"
        assert identity["target_dataset"] == "/data/out/{{ ds }}.csv"
        assert identity["target_dataset_resolved"] == "/data/out/2026-08-17.csv"

    def test_sql_source_is_opaque(self):  # T-F4.3E-6 (INV-1: tablo tahmini yok)
        identity = gov.dataset_identity(
            {
                "source_type": "sql",
                "inline_sql": "SELECT o.id FROM sales.orders o JOIN dw.dim d ON 1=1",
                "target_type": "db",
                "target_schema": "DW",
                "target_table": "T",
            },
            task_group_id="tg42",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        assert identity["source_dataset"] == "sql:tg42"
        assert identity["source_dataset_resolved"] == "sql:tg42"
        # Tablo adı SQL'den TAHMİN EDİLMEZ.
        assert "orders" not in json.dumps(identity)

    def test_no_credential_fields(self):  # T-F4.3E-7 (INV-5)
        identity = gov.dataset_identity(
            {"source_type": "table", "source_schema": "s", "source_table": "t"},
            task_group_id="tg",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        for banned in ("password", "user", "host", "port"):
            assert banned not in identity

    def test_missing_table_is_null_not_guess(self):
        identity = gov.dataset_identity(
            {"source_type": "table", "source_schema": "s"},
            task_group_id="tg",
            source_conn_id="src",
            target_conn_id="tgt",
        )
        assert identity["source_dataset"] is None


# ---------------------------------------------------------------------------
# Şema modülü — counter_source & preflight
# ---------------------------------------------------------------------------


class TestCounterSource:
    def test_expected_authority_map(self):
        # Review-fix MAJOR-1: taban sınıf İDDİASIZ "engine"dir; target_native
        # yükseltmesi yalnız resolve_counter_source'ta sinyal-bazlıdır.
        assert gov.expected_counter_source("standard") == "engine"
        assert gov.expected_counter_source("pipeline") == "engine"
        assert gov.expected_counter_source("spark") == "engine"
        assert gov.expected_counter_source("cdc") == "engine"
        assert gov.expected_counter_source("mystery") == "unavailable"
        assert gov.expected_counter_source(None) == "unavailable"

    def test_engine_null_counter_is_contract_violation(self):  # T-F4.3E-9
        with pytest.raises(ReconciliationError) as err:
            gov.resolve_counter_source("standard", rows_read=None, rows_written=10)
        message = str(err.value)
        assert "counter_source" in message
        assert _SECRET not in message

    def test_known_engine_null_counters_raise_not_unavailable(self):
        # Spark sonuç kanalı sayaç doldurmayı garanti eder (spark_submit
        # marker zorunluluğu); eksikse kanal bozuktur — sessiz unavailable YOK.
        with pytest.raises(ReconciliationError):
            gov.resolve_counter_source(
                "spark", rows_read=None, rows_written=None, snapshot_id=None
            )

    def test_spark_added_records_path_is_target_native(self):
        # Yalnız gerçek hedef-attestasyon: snapshot + added-records eşitlik
        # yolunun status'u ("passed" — spark_iceberg.RECONCILIATION_PASSED).
        value = gov.resolve_counter_source(
            "spark",
            rows_read=5,
            rows_written=5,
            snapshot_id="123",
            reconciliation_status="passed",
        )
        assert value == "target_native"

    def test_spark_merge_is_engine_not_target_native(self):  # MAJOR-1
        # merge: rows_written = rows_read EKOsu (spark_job.py) — snapshot
        # olsa da hedef-attested sayı DEĞİL; yukarı yuvarlama yok.
        value = gov.resolve_counter_source(
            "spark",
            rows_read=5,
            rows_written=5,
            snapshot_id="123",
            reconciliation_status="not_applicable_merge",
        )
        assert value == "engine"

    def test_cdc_is_engine_not_target_native(self):  # MAJOR-1
        # CDC: records_applied koordinatörün KENDİ sayacı (cdc/core.py
        # "fiziksel değişen satır sayısı DEĞİLDİR").
        value = gov.resolve_counter_source(
            "cdc",
            rows_read=7,
            rows_written=7,
            snapshot_id=None,
            reconciliation_status="cdc_ordered_applied",
        )
        assert value == "engine"

    def test_spark_probe_and_unknown_status_stay_engine(self):
        assert (
            gov.resolve_counter_source(
                "spark",
                rows_read=1,
                rows_written=1,
                snapshot_id=None,
                reconciliation_status="probe",
            )
            == "engine"
        )
        # Bilinmeyen status + snapshot: belirsizlik yukarı yuvarlanmaz.
        assert (
            gov.resolve_counter_source(
                "spark",
                rows_read=1,
                rows_written=1,
                snapshot_id="9",
                reconciliation_status="something_new",
            )
            == "engine"
        )

    def test_preflight_unavailable_fails_loud(self):  # T-F4.3E-10 (şema tarafı)
        with pytest.raises(ReconciliationUnavailableError, match="require_reconciliation"):
            gov.preflight_reconciliation("mystery", require_reconciliation=True)

    def test_preflight_optout_allows(self):  # T-F4.3E-11 (şema tarafı)
        gov.preflight_reconciliation("mystery", require_reconciliation=False)

    def test_preflight_engine_authority_passes(self):
        gov.preflight_reconciliation("standard", require_reconciliation=True)


# ---------------------------------------------------------------------------
# Şema modülü — config_revision (EX-D039.6)
# ---------------------------------------------------------------------------


class TestConfigRevision:
    def _write_manifest(self, root: Path, rev: str, bundle_hash: str) -> None:
        rev_dir = root / rev
        rev_dir.mkdir(parents=True)
        (rev_dir / "manifest.json").write_text(
            json.dumps({"revision_id": rev, "hashes": {"bundle": bundle_hash}}),
            encoding="utf-8",
        )

    def test_latest_revision_bundle_hash(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        history = tmp_path / gov.STUDIO_HISTORY_DIR_NAME / "flow_dag"
        self._write_manifest(history, "rev_000001", "old-hash")
        self._write_manifest(history, "rev_000002", "new-hash")
        assert gov.config_revision_for(str(config_path), "flow_dag") == "new-hash"

    def test_no_revision_is_none(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        assert gov.config_revision_for(str(config_path), "flow_dag") is None

    def test_missing_dag_id_is_none(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        assert gov.config_revision_for(str(config_path), None) is None

    def test_corrupt_manifest_is_none_not_crash(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        rev_dir = tmp_path / gov.STUDIO_HISTORY_DIR_NAME / "flow_dag" / "rev_000001"
        rev_dir.mkdir(parents=True)
        (rev_dir / "manifest.json").write_text("{not json", encoding="utf-8")
        assert gov.config_revision_for(str(config_path), "flow_dag") is None

    @pytest.mark.parametrize("payload", ["null", "[]", '"just-a-string"'])
    def test_valid_json_non_dict_manifest_is_none_not_crash(
        self, tmp_path, payload, caplog
    ):  # review-fix MAJOR-2
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        rev_dir = tmp_path / gov.STUDIO_HISTORY_DIR_NAME / "flow_dag" / "rev_000001"
        rev_dir.mkdir(parents=True)
        (rev_dir / "manifest.json").write_text(payload, encoding="utf-8")
        with caplog.at_level(logging.WARNING, logger=gov.__name__):
            assert gov.config_revision_for(str(config_path), "flow_dag") is None
        # Anomali sessiz DEĞİL: görünür uyarı loglanır (manifest-yok sessiz
        # None spec-uygun; anomali uyarılır — GOV-INV-4: task asla düşmez).
        assert "config_revision unavailable" in caplog.text

    def test_non_dict_hashes_is_none_not_crash(self, tmp_path, caplog):  # MAJOR-2
        config_path = tmp_path / "config.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        rev_dir = tmp_path / gov.STUDIO_HISTORY_DIR_NAME / "flow_dag" / "rev_000001"
        rev_dir.mkdir(parents=True)
        (rev_dir / "manifest.json").write_text(
            json.dumps({"hashes": "not-a-dict"}), encoding="utf-8"
        )
        with caplog.at_level(logging.WARNING, logger=gov.__name__):
            assert gov.config_revision_for(str(config_path), "flow_dag") is None
        assert "hashes" in caplog.text


# ---------------------------------------------------------------------------
# require_reconciliation config sözleşmesi
# ---------------------------------------------------------------------------


class TestRequireReconciliationConfig:
    def _load(self, tmp_path, root_extra: str):
        from ffengine.config.loader import ConfigLoader

        config = tmp_path / "cfg.yaml"
        config.write_text(
            "source_db_var: s\n"
            "target_db_var: t\n"
            + root_extra
            + "flow_tasks:\n"
            "  - task_group_id: tg1\n"
            "    source_schema: public\n"
            "    target_schema: public\n"
            "    target_table: t1\n"
            "    source_table: s1\n"
            "    source_type: table\n"
            "    load_method: append\n",
            encoding="utf-8",
        )
        return ConfigLoader().load(str(config), "tg1")

    def test_default_true_when_absent(self, tmp_path):
        task = self._load(tmp_path, "")
        assert operator_module._require_reconciliation(task) is True

    def test_explicit_false_propagates(self, tmp_path):  # T-F4.3E-11 (config)
        task = self._load(tmp_path, "require_reconciliation: false\n")
        assert operator_module._require_reconciliation(task) is False

    def test_non_bool_rejected(self, tmp_path):
        with pytest.raises(ValidationError, match="require_reconciliation"):
            self._load(tmp_path, "require_reconciliation: yes please\n")

    def test_task_level_field_rejected_fail_loud(self, tmp_path):  # MINOR-1
        from ffengine.config.loader import ConfigLoader

        config = tmp_path / "cfg.yaml"
        config.write_text(
            "source_db_var: s\n"
            "target_db_var: t\n"
            "flow_tasks:\n"
            "  - task_group_id: tg1\n"
            "    source_schema: public\n"
            "    target_schema: public\n"
            "    target_table: t1\n"
            "    source_table: s1\n"
            "    source_type: table\n"
            "    load_method: append\n"
            "    require_reconciliation: false\n",
            encoding="utf-8",
        )
        with pytest.raises(ValidationError, match="root-level"):
            ConfigLoader().load(str(config), "tg1")


# ---------------------------------------------------------------------------
# Operator yüzeyi — summary / error / preflight
# ---------------------------------------------------------------------------


class TestOperatorGovernanceEvent:
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
                "host": "db-host.internal",
                "port": 5432,
                "user": "svc_user",
                "password": _SECRET,
                "database": "db",
                "conn_type": "postgres",
            }
            self.task_config = {
                "source_schema": "public",
                "source_table": "orders",
                "source_type": "table",
                "target_schema": "dw",
                "target_table": "orders_fact",
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
            self.mock_db = mock_db
            self.mock_part = mock_part
            self.mock_etl = mock_etl
            yield

    def test_success_event_blocks_complete(self):  # T-F4.3E-1
        ti = _fake_ti()
        summary = _make_operator().execute({"ti": ti})
        assert summary["schema_version"] == "ffgov-1"
        assert summary["event_scope"] == "task"
        assert summary["outcome"] == "succeeded"
        assert summary["dag_id"] == "flow_dag"
        assert summary["run_id"] == "manual__2026-08-17"
        assert summary["task_id"] == "task_001"
        assert summary["map_index"] is None
        assert summary["source_conn_id"] == "src_conn"
        assert summary["target_conn_id"] == "tgt_conn"
        assert summary["source_dataset"] == "public.orders"
        assert summary["target_dataset"] == "dw.orders_fact"
        assert summary["source_type"] == "table"
        assert summary["target_type"] == "db"
        assert summary["engine_type"] == "standard"
        assert summary["counter_source"] == "engine"
        assert "config_revision" in summary

    def test_existing_names_preserved(self):  # T-F4.3E-2 (INV-2)
        summary = _make_operator().execute({"ti": _fake_ti()})
        assert summary["rows"] == 100
        assert summary["rows_read"] == 100
        assert summary["rows_written"] == 100
        assert summary["rows_rejected"] == 0
        assert summary["reconciliation_status"] == "passed"
        assert summary["duration_seconds"] == 1.5
        assert summary["throughput"] == 66.67
        assert summary["rows"] == summary["rows_written"]

    def test_no_credential_or_conn_detail_in_summary(self):  # T-F4.3E-7
        summary = _make_operator().execute({"ti": _fake_ti()})
        dumped = json.dumps(summary, default=str)
        assert _SECRET not in dumped
        assert "db-host.internal" not in dumped
        assert "svc_user" not in dumped
        for banned in ("password", "user", "host", "port"):
            assert banned not in summary

    def test_no_data_values_in_summary(self):  # T-F4.3E-8
        sentinel = "row-value-must-not-leak"
        dag_run = MagicMock()
        dag_run.conf = {"customer_name": sentinel}
        summary = _make_operator().execute({"ti": _fake_ti(), "dag_run": dag_run})
        assert sentinel not in json.dumps(summary, default=str)

    def test_xcom_key_set_unchanged(self):  # T-F4.3E-16 (INV-6)
        ti = _fake_ti()
        _make_operator().execute({"ti": ti})
        assert set(ti.pushed) == {
            "rows_transferred",
            "duration_seconds",
            "rows_per_second",
            "retry_telemetry",
        }

    def test_unavailable_preflight_fails_before_any_connection(self):  # T-F4.3E-10
        external = MagicMock()
        with patch.object(
            operator_module,
            "_engine_preflight",
            return_value=(external, "mystery"),
        ):
            with pytest.raises(ReconciliationUnavailableError):
                _make_operator().execute({"ti": _fake_ti()})
        self.mock_adapter.get_connection_params.assert_not_called()
        self.mock_db.assert_not_called()
        external.run.assert_not_called()

    def test_optout_runs_with_honest_nulls(self):  # T-F4.3E-11
        self.task_config["_require_reconciliation"] = False
        self.mock_loader.return_value.load.return_value = dict(self.task_config)
        external = MagicMock()
        external.run_with_context = None
        result = SimpleNamespace(
            rows=5,
            duration_seconds=1.0,
            throughput=5.0,
            partitions_completed=1,
            errors=[],
            rows_read=None,
            rows_written=None,
            rows_rejected=None,
            reconciliation_status=None,
            engine=None,
            application_id=None,
            snapshot_id=None,
        )
        external.run.return_value = result
        with patch.object(
            operator_module,
            "_engine_preflight",
            return_value=(external, "mystery"),
        ):
            summary = _make_operator().execute({"ti": _fake_ti()})
        assert summary["counter_source"] == "unavailable"
        assert summary["rows_read"] is None
        assert summary["rows_written"] is None
        assert summary["rows_rejected"] is None
        assert summary["reconciliation_status"] == "not_applicable"

    def test_failure_event_has_null_reconciliation_key(self):  # T-F4.3E-12/13
        self.mock_etl.return_value.run_flow_task.side_effect = ConfigError(
            "bad config"
        )
        ti = _fake_ti()
        with pytest.raises(ConfigError):
            _make_operator().execute({"ti": ti})
        payload = ti.pushed["error_summary"]
        assert payload["outcome"] == "failed"
        assert payload["schema_version"] == "ffgov-1"
        assert "reconciliation_status" in payload
        assert payload["reconciliation_status"] is None
        assert payload["error_code"]  # T-F4.3E-13
        assert _SECRET not in json.dumps(payload, default=str)
        # Mevcut error_summary anahtarları korunur (T-F4.3E-14 tüketicileri).
        assert payload["error_type"] == "ConfigError"
        assert "message" in payload

    def test_preflight_error_event_status_is_null_not_failed(self):  # MINOR-2
        # Preflight reddi: aktarım hiç BAŞLAMADI → "failed" muhasebe iması
        # yanlış olurdu; dürüst değer null.
        external = MagicMock()
        ti = _fake_ti()
        with patch.object(
            operator_module,
            "_engine_preflight",
            return_value=(external, "mystery"),
        ):
            with pytest.raises(ReconciliationUnavailableError):
                _make_operator().execute({"ti": ti})
        payload = ti.pushed["error_summary"]
        assert payload["outcome"] == "failed"
        assert "reconciliation_status" in payload
        assert payload["reconciliation_status"] is None
        assert payload["error_code"] == "reconciliation_unavailable"

    def test_reconciliation_error_event_status_is_failed(self):  # MINOR-2 kapsam
        # KOŞMUŞ muhasebenin tutmaması ise gerçek "failed"dır.
        self.mock_etl.return_value.run_flow_task.side_effect = ReconciliationError(
            "rows_read != rows_written + rows_rejected"
        )
        ti = _fake_ti()
        with pytest.raises(ReconciliationError):
            _make_operator().execute({"ti": ti})
        payload = ti.pushed["error_summary"]
        assert payload["reconciliation_status"] == "failed"
        assert payload["error_code"] == "reconciliation_error"

    def test_external_file_source_resolved_is_null(self):  # MINOR-3
        # Harici motor dispatch'i render'dan ÖNCE: dataset=şablon,
        # `*_resolved`=null ("resolved" etiketi yalan söylemez).
        self.task_config.update(
            {
                "source_type": "parquet",
                "file_path": "/data/in/{{ ds }}.parquet",
                "_require_reconciliation": False,
            }
        )
        self.mock_loader.return_value.load.return_value = dict(self.task_config)
        result = SimpleNamespace(
            rows=3,
            duration_seconds=1.0,
            throughput=3.0,
            partitions_completed=1,
            errors=[],
            rows_read=3,
            rows_written=3,
            rows_rejected=0,
            reconciliation_status="passed",
            engine=None,
            application_id="app-1",
            snapshot_id="42",
        )
        external = MagicMock()
        # `{{ ds }}` runtime token'ı taşıyan task → W2 sözleşmesi gereği
        # run_with_context kanalı kullanılır.
        external.run_with_context = MagicMock(return_value=result)
        external.run.return_value = result
        with patch.object(
            operator_module,
            "_engine_preflight",
            return_value=(external, "spark"),
        ):
            summary = _make_operator().execute({"ti": _fake_ti()})
        assert summary["source_dataset"] == "/data/in/{{ ds }}.parquet"
        assert summary["source_dataset_resolved"] is None
        # added-records eşitlik sinyali (snapshot + "passed") → target_native.
        assert summary["counter_source"] == "target_native"

    def test_notification_consumers_still_work(self):  # T-F4.3E-14 (INV-2)
        from ffengine.airflow.notifications import (
            _pull_error_summary,
            _pull_rows,
        )

        ti = _fake_ti()
        _make_operator().execute({"ti": ti})

        consumer_ti = MagicMock()
        consumer_ti.task_id = "task_001"
        consumer_ti.xcom_pull.side_effect = lambda task_ids, key: ti.pushed.get(key)
        assert _pull_rows([consumer_ti]) == 100
        assert _pull_error_summary([consumer_ti]) is None

    def test_config_revision_read_once_per_task(self):  # T-F4.3E-18 (INV-3)
        self.mock_part.return_value.plan.return_value = [
            {"part_id": 0, "where": "id < 500"},
            {"part_id": 1, "where": "id >= 500"},
            {"part_id": 2, "where": "id >= 900"},
        ]
        with patch.object(
            operator_module._gov,
            "config_revision_for",
            wraps=operator_module._gov.config_revision_for,
        ) as revision_call:
            _make_operator().execute({"ti": _fake_ti()})
        assert revision_call.call_count == 1
        assert self.mock_etl.return_value.run_flow_task.call_count == 3

    def test_config_revision_from_active_manifest(self, tmp_path):
        config_path = tmp_path / "cfg.yaml"
        config_path.write_text("flow_tasks: []", encoding="utf-8")
        rev_dir = tmp_path / gov.STUDIO_HISTORY_DIR_NAME / "flow_dag" / "rev_000007"
        rev_dir.mkdir(parents=True)
        (rev_dir / "manifest.json").write_text(
            json.dumps({"hashes": {"bundle": "bundle-hash-7"}}),
            encoding="utf-8",
        )
        summary = _make_operator(config_path=str(config_path)).execute(
            {"ti": _fake_ti()}
        )
        assert summary["config_revision"] == "bundle-hash-7"

    def test_forward_compatible_consumer(self):  # T-F4.3E-17
        summary = _make_operator().execute({"ti": _fake_ti()})
        extended = {**summary, "future_optional_field": "x"}
        # v1 tüketicisi bildiği anahtarları okur; ek anahtar kırmaz.
        assert extended["schema_version"] == "ffgov-1"
        assert extended["rows"] == 100


# ---------------------------------------------------------------------------
# Mapped-partition & aggregate yüzeyleri
# ---------------------------------------------------------------------------


class TestMappedPartitionEvent:
    def test_partition_scope_and_map_index(self):  # T-F4.3E-15
        task_config = {
            "source_type": "table",
            "source_schema": "public",
            "source_table": "orders",
            "target_schema": "dw",
            "target_table": "orders_fact",
        }
        runtime = (
            dict(task_config),
            {"conn_type": "postgres"},
            {"conn_type": "postgres"},
            MagicMock(),
            MagicMock(),
            MagicMock(),
            {"binding_values": {}},
            "standard",
        )
        session = MagicMock()
        session.conn = MagicMock()
        db_ctx = MagicMock()
        db_ctx.__enter__ = MagicMock(return_value=session)
        db_ctx.__exit__ = MagicMock(return_value=False)
        flow_result = FlowResult(
            rows=10,
            duration_seconds=0.5,
            throughput=20.0,
            partitions_completed=1,
            errors=[],
            rows_read=10,
            rows_written=10,
            rows_rejected=0,
            reconciliation_status="passed",
        )
        with (
            patch.object(
                operator_module, "_resolve_task_runtime", return_value=runtime
            ),
            patch.object(
                operator_module,
                "_resolve_sql_bindings_if_needed",
                side_effect=lambda **kw: dict(kw["task_config"]),
            ),
            patch.object(
                operator_module,
                "_attach_mapping_if_needed",
                side_effect=lambda **kw: dict(kw["task_config"]),
            ),
            patch(_P_DBSESS, return_value=db_ctx),
            patch(_P_FLOW) as mock_flow,
        ):
            mock_flow.return_value.run_flow_task.return_value = flow_result
            payload = run_partition_for_task(
                config_path="/tmp/cfg.yaml",
                task_group_id="tg1",
                source_conn_id="src",
                target_conn_id="tgt",
                partition_spec={"part_id": 2, "where": "id > 10"},
                airflow_context={
                    "ti": SimpleNamespace(
                        dag_id="flow_dag",
                        run_id="r1",
                        task_id="run_partition",
                        map_index=2,
                    )
                },
            )
        assert payload["event_scope"] == "partition"
        assert payload["map_index"] == 2
        assert payload["schema_version"] == "ffgov-1"
        assert payload["outcome"] == "succeeded"
        assert payload["partition_id"] == 2
        assert payload["counter_source"] == "engine"
        assert payload["source_dataset"] == "public.orders"
        # F3.3 alanları aynı adla durur (INV-2).
        assert payload["rows_read"] == 10
        assert payload["reconciliation_status"] == "passed"

    def test_aggregate_carries_task_scope_header(self):
        payloads = [
            {
                "rows": 5,
                "duration_seconds": 1.0,
                "throughput": 5.0,
                "partitions_completed": 1,
                "errors": [],
                "rows_read": 5,
                "rows_written": 5,
                "rows_rejected": 0,
                "reconciliation_status": "passed",
                "engine_type": "standard",
                "counter_source": "engine",
                "config_revision": "hash-1",
                "source_dataset": "public.orders",
            },
            {
                "rows": 7,
                "duration_seconds": 2.0,
                "throughput": 3.5,
                "partitions_completed": 1,
                "errors": [],
                "rows_read": 7,
                "rows_written": 7,
                "rows_rejected": 0,
                "reconciliation_status": "passed",
                "engine_type": "standard",
                "counter_source": "engine",
                "config_revision": "hash-1",
                "source_dataset": "public.orders",
            },
        ]
        summary = aggregate_partition_payloads(payloads)
        assert summary["event_scope"] == "task"
        assert summary["schema_version"] == "ffgov-1"
        assert summary["outcome"] == "succeeded"
        assert summary["counter_source"] == "engine"
        assert summary["config_revision"] == "hash-1"
        assert summary["source_dataset"] == "public.orders"
        assert summary["rows"] == 12
        assert summary["rows_read"] == 12
        assert summary["reconciliation_status"] == "passed"

    def test_aggregate_legacy_payloads_keep_working(self):  # INV-2 köprüsü
        payloads = [
            {
                "rows": 5,
                "duration_seconds": 1.0,
                "throughput": 5.0,
                "partitions_completed": 1,
                "errors": [],
            }
        ]
        summary = aggregate_partition_payloads(payloads)
        assert summary["rows"] == 5
        assert summary["reconciliation_status"] == "legacy"
        assert summary["counter_source"] is None
        assert summary["config_revision"] is None


# ---------------------------------------------------------------------------
# Frozen kontrat & Community-only
# ---------------------------------------------------------------------------


class TestFrozenContracts:
    def test_streamer_stream_signature_unchanged(self):  # T-F4.3E-19 (INV-2)
        from ffengine.pipeline.streamer import Streamer

        params = list(inspect.signature(Streamer.stream).parameters)
        assert params == ["self", "source_iter", "writer", "transformer", "task_config"]

    def test_write_batch_signature_unchanged(self):  # T-F4.3E-19
        from ffengine.pipeline.target_writer import TargetWriter

        params = list(inspect.signature(TargetWriter.write_batch).parameters)
        assert params == ["self", "rows", "task_config"]

    def test_community_only_module(self):  # T-F4.3E-20 (INV-8)
        source = Path(gov.__file__).read_text(encoding="utf-8")
        assert "ffengine_enterprise" not in source
        operator_source = Path(operator_module.__file__).read_text(encoding="utf-8")
        assert "import ffengine_enterprise" not in operator_source

    def test_log_structured_emits_core_null_as_value(self):  # null bir değerdir
        records = []

        class _Handler(logging.Handler):
            def emit(self, record):
                # logging, tek dict argümanını doğrudan record.args yapar.
                args = record.args
                records.append(args if isinstance(args, dict) else args[0])

        handler = _Handler()
        operator_module._log.addHandler(handler)
        operator_module._log.setLevel(logging.INFO)
        try:
            operator_module._log_structured(
                level=logging.INFO,
                stage="airflow",
                message="m",
                task_group_id="tg",
                source_db="postgres",
                target_db="postgres",
                reconciliation_status=None,
                counter_source=None,
                free_form_optional=None,
            )
        finally:
            operator_module._log.removeHandler(handler)
        payload = records[-1]
        assert "reconciliation_status" in payload
        assert payload["reconciliation_status"] is None
        assert "counter_source" in payload
        # Şema dışı opsiyonel alanların None-düşürme davranışı DEĞİŞMEDİ.
        assert "free_form_optional" not in payload
