"""
F6.0 — Motor seçim seam'i.

Kapsam: engine preflight'ın I/O'dan ÖNCE çalışması (mapped + single yol),
harici motor dispatch'i, operator argüman precedence'ı ve `engine_type`
raporlaması.

Kritik nokta (plan §1): mapped zincir `plan_partitions -> prepare_target ->
run_partition` olduğundan, motor doğrulaması `run_partition` içinde yapılırsa
`prepare_target` hedefte TRUNCATE/CREATE çalıştırdıktan SONRA fail-loud olur.
Bu yüzden preflight `_resolve_task_runtime()` içinde, config yüklendikten
hemen sonra ve `AirflowConnectionAdapter`'a dokunulmadan önce olmalıdır.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import patch

from ffengine.core import engine_registry
from ffengine.core.base_engine import BaseEngine, FlowResult
from ffengine.errors.exceptions import EngineError

import ffengine.airflow.operator as operator_module

# Lazy import nedeniyle kaynak modül yolunda patch (test_operator.py deseni)
_P_ADAPTER = "ffengine.db.airflow_adapter.AirflowConnectionAdapter"
_P_LOADER = "ffengine.config.loader.ConfigLoader"
_P_BINDER = "ffengine.config.binding_resolver.BindingResolver"
_P_DBSESS = "ffengine.db.session.DBSession"
_P_WRITER = "ffengine.pipeline.target_writer.TargetWriter"
_P_RUNTIME_GUARD = "ffengine.core.runtime_guard.run_runtime_guards"

_MAPPED_HELPERS = (
    "plan_partitions_for_task",
    "prepare_target_for_task",
    "run_partition_for_task",
)

_FAILURE_MODES = ("missing_provider", "unavailable_provider", "non_standard_engine")


class _FakeExternalEngine(BaseEngine):
    """Kayıtlı, kullanılabilir ama StandardEngine olmayan motor."""

    available = True
    run_calls: list = []

    def is_available(self) -> bool:
        return type(self).available

    def run(self, config_path: str, task_group_id: str) -> FlowResult:
        type(self).run_calls.append((config_path, task_group_id))
        return FlowResult(
            rows=7,
            duration_seconds=0.5,
            throughput=14.0,
            partitions_completed=1,
        )


class _UnavailableEngine(_FakeExternalEngine):
    available = False
    run_calls: list = []


@pytest.fixture(autouse=True)
def _isolated_engine_registry():
    engine_registry.clear_engine_providers()
    _FakeExternalEngine.run_calls = []
    _UnavailableEngine.run_calls = []
    yield
    engine_registry.clear_engine_providers()


def _arrange_failure(mode: str) -> str:
    """Hata modunu kurar ve kullanılacak `engine.preference` değerini döndürür."""
    if mode == "missing_provider":
        return "spark"  # hiçbir provider kayıtlı değil
    if mode == "unavailable_provider":
        engine_registry.register_engine_provider("spark", _UnavailableEngine)
        return "spark"
    if mode == "non_standard_engine":
        # Kayıtlı ve kullanılabilir; mapped yol yine de reddetmeli (B8).
        engine_registry.register_engine_provider("pipeline", _FakeExternalEngine)
        return "pipeline"
    raise AssertionError(f"bilinmeyen mod: {mode}")


def _task_config(preference: str | None) -> dict:
    cfg = {
        "source_schema": "public",
        "source_table": "orders",
        "source_type": "table",
        "load_method": "append",
        "column_mapping_mode": "source",
        "passthrough_full": True,
    }
    if preference is not None:
        cfg["_engine_preference"] = preference
    return cfg


def _call_mapped_helper(helper_name: str):
    fn = getattr(operator_module, helper_name)
    kwargs = {
        "config_path": "/tmp/cfg.yaml",
        "task_group_id": "task_001",
        "source_conn_id": "src_conn",
        "target_conn_id": "tgt_conn",
    }
    if helper_name == "run_partition_for_task":
        kwargs["partition_spec"] = {"part_id": 0, "where": None}
    return fn(**kwargs)


@pytest.mark.parametrize("helper", _MAPPED_HELPERS)
@pytest.mark.parametrize("mode", _FAILURE_MODES)
def test_f6_0_mapped_path_fails_loud_before_any_io(helper, mode, monkeypatch):
    """T-F6.0-2/-3 + B8: mapped zincirde motor hatası hiçbir I/O'dan önce.

    `AirflowConnectionAdapter.get_connection_params`, `DBSession` ve
    `TargetWriter.prepare` çağrılmamalı — özellikle `prepare_target_for_task`
    hedefte TRUNCATE/CREATE çalıştırmadan durmalı.
    """
    # Edition gate'in (422) runtime EngineError'u gölgelemediğini belgele.
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    preference = _arrange_failure(mode)

    with (
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
        patch(_P_DBSESS) as dbsession,
        patch(_P_WRITER) as writer,
        patch(_P_BINDER) as binder,
    ):
        loader.return_value.load.return_value = _task_config(preference)
        binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)

        with pytest.raises(EngineError):
            _call_mapped_helper(helper)

        adapter.get_connection_params.assert_not_called()
        dbsession.assert_not_called()
        writer.return_value.prepare.assert_not_called()
        # Harici motor mapped yolda ASLA çalıştırılmaz (partition başına
        # tüm task-group'u tekrar koşturmak veri çiftler).
        assert _FakeExternalEngine.run_calls == []


@pytest.mark.parametrize("helper", _MAPPED_HELPERS)
def test_f6_0_mapped_path_runs_guard_before_preflight_and_io(helper):
    """Mapped task'larda lisans/runtime guard preflight'tan once calisir."""
    events = []

    def _guard(_context):
        events.append("guard")

    def _preflight(*_args, **_kwargs):
        events.append("preflight")
        raise EngineError("stop-before-io")

    with (
        patch(_P_RUNTIME_GUARD, side_effect=_guard) as guard,
        patch.object(operator_module, "_engine_preflight", side_effect=_preflight),
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
    ):
        loader.return_value.load.return_value = _task_config(None)
        with pytest.raises(EngineError, match="stop-before-io"):
            _call_mapped_helper(helper)

    assert events == ["guard", "preflight"]
    guard.assert_called_once()
    adapter.get_connection_params.assert_not_called()


def test_f6_0_runtime_guard_context_contains_airflow_metadata():
    """Mapped guard context preserves the audit identifiers Enterprise needs."""
    started_at = object()
    context = {
        "dag_run": SimpleNamespace(
            dag_id="daily_load", run_id="manual__1", start_date=started_at
        ),
        "ti": SimpleNamespace(task_id="load_orders.run_partition"),
    }

    assert operator_module._runtime_guard_context(context) == {
        "dag_id": "daily_load",
        "task_id": "load_orders.run_partition",
        "run_id": "manual__1",
        "dag_run_start_date": started_at,
    }


@pytest.mark.parametrize("helper", _MAPPED_HELPERS)
def test_f6_0_mapped_path_standard_engine_unchanged(helper, monkeypatch):
    """T-F6.0-4: preference yok -> `auto` -> StandardEngine; akış değişmez.

    Preflight eklendikten sonra da mapped yol mevcut davranışını sürdürmeli:
    connection adapter çağrılır (yani preflight geçilir), EngineError yok.
    """
    monkeypatch.setenv("FFENGINE_EDITION", "community")

    with (
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
        patch(_P_DBSESS),
        patch(_P_WRITER),
        patch(_P_BINDER) as binder,
    ):
        loader.return_value.load.return_value = _task_config(None)
        binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)
        adapter.get_connection_params.return_value = {
            "host": "localhost",
            "port": 5432,
            "user": "u",
            "password": "p",
            "database": "db",
            "conn_type": "postgres",
        }

        # Preflight'ı geçtiğini kanıtlamak yeterli; sonrasında akış
        # mock'lanmamış bir bileşende kesilebilir (bu testin konusu değil).
        try:
            _call_mapped_helper(helper)
        except EngineError as exc:  # pragma: no cover - regresyon koruması
            pytest.fail(f"StandardEngine yolu EngineError ile kesildi: {exc}")
        except Exception:
            pass

        assert adapter.get_connection_params.called, (
            "preflight StandardEngine'i geçirmeli ve akış connection "
            "adapter'a ulaşmalı"
        )


# ---------------------------------------------------------------------------
# Harici motor dispatch'i (tek-task yolu) — karar matrisi satır 3/7
# ---------------------------------------------------------------------------


def _make_operator(**overrides):
    from ffengine.airflow.operator import FFEngineOperator

    defaults = {
        "config_path": "/tmp/cfg.yaml",
        "task_group_id": "task_001",
        "source_conn_id": "src_conn",
        "target_conn_id": "tgt_conn",
    }
    defaults.update(overrides)
    return FFEngineOperator(**defaults)


def test_f6_0_external_engine_dispatches_via_w2_run(monkeypatch):
    """Harici motor W2 `run(config_path, task_group_id)` ile calisir.

    Standard hazirligi (connection adapter / DBSession) calistirilmaz.
    """
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    engine_registry.register_engine_provider("spark", _FakeExternalEngine)

    with (
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
        patch(_P_DBSESS) as dbsession,
    ):
        loader.return_value.load.return_value = _task_config("spark")
        summary = _make_operator().execute()

    assert _FakeExternalEngine.run_calls == [("/tmp/cfg.yaml", "task_001")]
    adapter.get_connection_params.assert_not_called()
    dbsession.assert_not_called()
    assert summary["engine_type"] == "spark"
    assert summary["rows"] == 7


def test_f6_0_external_engine_rejects_mismatched_reported_identity(monkeypatch):
    """Resolved provider identity is authoritative; engines cannot relabel runs."""
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    engine_registry.register_engine_provider("spark", _FakeExternalEngine)
    mismatched = FlowResult(
        rows=1,
        duration_seconds=0.1,
        throughput=10.0,
        partitions_completed=1,
        engine="pipeline",
    )

    with (
        patch(_P_LOADER) as loader,
        patch.object(_FakeExternalEngine, "run", return_value=mismatched),
        patch(_P_ADAPTER) as adapter,
    ):
        loader.return_value.load.return_value = _task_config("spark")
        with pytest.raises(EngineError, match="engine_type"):
            _make_operator().execute()

    adapter.get_connection_params.assert_not_called()


def test_f6_0_engine_type_reports_resolved_engine_not_preference(monkeypatch):
    """T-F6.0-5: `auto` -> StandardEngine cozuldugunde rapor 'standard'."""
    monkeypatch.setenv("FFENGINE_EDITION", "community")

    with (
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
        patch(_P_DBSESS) as dbsession,
        patch("ffengine.config.binding_resolver.BindingResolver") as binder,
        patch("ffengine.mapping.MappingResolver") as mapping,
        patch("ffengine.partition.Partitioner") as partitioner,
        patch("ffengine.pipeline.target_writer.TargetWriter"),
        patch("ffengine.core.flow_manager.FlowManager") as flow,
    ):
        loader.return_value.load.return_value = _task_config(None)
        binder.return_value.resolve.side_effect = lambda cfg, ctx: dict(cfg)
        adapter.get_connection_params.return_value = {
            "host": "h", "port": 1, "user": "u", "password": "p",
            "database": "d", "conn_type": "postgres",
        }
        session = dbsession.return_value.__enter__.return_value
        session.conn = object()
        from ffengine.mapping.resolver import MappingResult
        from ffengine.dialects.base import ColumnInfo

        mapping.return_value.resolve.return_value = MappingResult(
            source_columns=["id"],
            target_columns=["id"],
            target_columns_meta=[ColumnInfo("id", "INTEGER")],
        )
        partitioner.return_value.plan.return_value = [{"part_id": 0, "where": None}]
        flow.return_value.run_flow_task.return_value = FlowResult(
            rows=3, duration_seconds=1.0, throughput=3.0, partitions_completed=1
        )

        summary = _make_operator().execute()

    assert summary["engine_type"] == "standard"


def test_f6_0_aggregate_rejects_mixed_engine_types():
    """T-F6.0-5: farkli motor kimlikleri sessizce birlestirilmez."""
    from ffengine.airflow.operator import aggregate_partition_payloads

    payloads = [
        {"rows": 1, "engine_type": "standard"},
        {"rows": 2, "engine_type": "pipeline"},
    ]
    with pytest.raises(EngineError, match="engine_type"):
        aggregate_partition_payloads(payloads)


def test_f6_0_aggregate_preserves_single_engine_type():
    from ffengine.airflow.operator import aggregate_partition_payloads

    out = aggregate_partition_payloads(
        [{"rows": 1, "engine_type": "standard"}, {"rows": 2, "engine_type": "standard"}]
    )
    assert out["engine_type"] == "standard"
    assert out["rows"] == 3


def test_f6_0_aggregate_results_preserves_flow_result_engine():
    from ffengine.airflow.operator import aggregate_results

    result = aggregate_results(
        [
            FlowResult(1, 0.2, 5.0, 1, engine="standard"),
            FlowResult(2, 0.3, 6.0, 1, engine="standard"),
        ]
    )
    assert result.engine == "standard"


def test_f6_0_aggregate_results_rejects_mixed_engines():
    from ffengine.airflow.operator import aggregate_results

    with pytest.raises(EngineError, match="engine"):
        aggregate_results(
            [
                FlowResult(1, 0.2, 5.0, 1, engine="standard"),
                FlowResult(2, 0.3, 6.0, 1, engine="pipeline"),
            ]
        )


def test_f6_0_legacy_partition_payload_without_engine_type_is_none():
    """Geriye uyum: eski payload'lar engine_type tasimaz -> uydurma deger yok."""
    from ffengine.airflow.operator import aggregate_partition_payloads

    out = aggregate_partition_payloads([{"rows": 5}])
    assert out["engine_type"] is None


# ---------------------------------------------------------------------------
# Operator argumani precedence (plan §6) — 6 satirlik tablo
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "yaml_pref,operator_arg,expected",
    [
        (None, None, "auto"),          # 1: ikisi de yok
        (None, "pipeline", "pipeline"),  # 2: legacy programmatic DAG
        (None, "auto", "auto"),        # 3: ayirt edilemez, sonuc ayni
        ("spark", "auto", "spark"),    # 4/5: YAML kazanir (arg belirsiz)
    ],
)
def test_f6_0_engine_precedence_matrix(yaml_pref, operator_arg, expected):
    from ffengine.airflow.operator import _resolve_engine_preference

    assert _resolve_engine_preference(_task_config(yaml_pref), operator_arg) == expected


def test_f6_0_conflicting_operator_argument_fails_loud():
    """Satir 6: explicit arg YAML ile celisiyorsa sessizce yok sayilmaz."""
    from ffengine.airflow.operator import _resolve_engine_preference
    from ffengine.errors.exceptions import ConfigError

    with pytest.raises(ConfigError, match="celisiyor"):
        _resolve_engine_preference(_task_config("spark"), "pipeline")


# ---------------------------------------------------------------------------
# W2 kontrat korunumu
# ---------------------------------------------------------------------------


def test_f6_0_flow_result_engine_is_optional_and_last():
    """T-F6.0-6: `engine` additive/defaulted; legacy pozisyonel cagrilar calisir."""
    legacy = FlowResult(10, 1.0, 10.0, 1)
    assert legacy.engine is None
    assert legacy.rows_read == 10  # F3.3 turetme davranisi korunur

    stamped = FlowResult(10, 1.0, 10.0, 1, engine="spark")
    assert stamped.engine == "spark"


def test_f6_0_base_engine_run_signature_unchanged():
    import inspect

    params = list(inspect.signature(BaseEngine.run).parameters)
    assert params == ["self", "config_path", "task_group_id"]


# ---------------------------------------------------------------------------
# Config sozlesmesi — public ConfigValidator.validate() uzerinden
# ---------------------------------------------------------------------------


def _validated_task(**overrides) -> dict:
    """`_check_required`'i gecen minimal task; engine kurallari ondan sonra."""
    task = {
        "task_group_id": "t1",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "dw",
        "target_table": "orders",
        "source_type": "table",
        "load_method": "append",
    }
    task.update(overrides)
    return task


def _validate(task: dict) -> None:
    from ffengine.config.validator import ConfigValidator

    ConfigValidator().validate(task)


@pytest.mark.parametrize("preference", ["auto", "standard", "pipeline"])
@pytest.mark.parametrize("endpoint", ["source_type", "target_type"])
def test_f6_0_validate_rejects_iceberg_without_explicit_spark(
    preference, endpoint, monkeypatch
):
    """T-F6.0-7: `iceberg` + acik olmayan tercih -> gerekceli 422.

    Public `ConfigValidator.validate()` uzerinden kanitlanir (private helper
    cagrisi tek basina acceptance sayilmaz).
    """
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = _validated_task(**{endpoint: "iceberg", "_engine_preference": preference})
    with pytest.raises(ValidationError, match="[Ii]ceberg"):
        _validate(task)


def test_f6_0_validate_rejects_iceberg_when_engine_block_absent(monkeypatch):
    """Engine blogu hic yokken de 422 — varsayilan `auto` Spark secmez."""
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="[Ii]ceberg"):
        _validate(_validated_task(target_type="iceberg"))


def test_f6_0_validate_allows_iceberg_with_explicit_spark(monkeypatch):
    """Acik `spark` engine kuralini gecer.

    (F6.0'da sonrasinda `_check_target_type` `iceberg`'i reddeder — Iceberg
    destegi F6.2'dir. Burada yalnizca ENGINE kuralinin gecildigi kanitlanir.)
    """
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = _validated_task(target_type="iceberg", _engine_preference="spark")
    with pytest.raises(ValidationError) as exc:
        _validate(task)
    assert "iceberg" not in str(exc.value).lower() or "target_type" in str(exc.value)


@pytest.mark.parametrize("preference", ["pipeline", "spark"])
def test_f6_0_validate_enterprise_gate_on_community(preference, monkeypatch):
    """INV-8: Community edition'da `pipeline`/`spark` -> 422."""
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "community")
    with pytest.raises(ValidationError, match="Enterprise"):
        _validate(_validated_task(_engine_preference=preference))


@pytest.mark.parametrize("alias", ["community", "enterprise"])
def test_f6_0_validate_rejects_yaml_legacy_alias(alias, monkeypatch):
    """Legacy alias YAML'da kabul edilmez; actionable migration mesaji verir."""
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="legacy alias"):
        _validate(_validated_task(_engine_preference=alias))


def test_f6_0_validate_accepts_canonical_values(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    for preference in ("auto", "standard", "pipeline", "spark"):
        _validate(_validated_task(_engine_preference=preference))


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param(None, id="null"),
        pytest.param("", id="empty-string"),
        pytest.param(False, id="false"),
        pytest.param(0, id="zero"),
        pytest.param([], id="empty-list"),
        pytest.param({}, id="empty-mapping"),
    ],
)
def test_f6_0_validate_rejects_explicit_falsy_preference(invalid, monkeypatch):
    """Yalniz eksik alan auto olur; acik gecersiz deger fail-loud'dur."""
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    task = _validated_task(_engine_preference=invalid)
    with pytest.raises(ValidationError, match="non-empty string"):
        _validate(task)


# ---------------------------------------------------------------------------
# §5 — tek normalizasyon kaynagi, katmanina gore farkli exception
# ---------------------------------------------------------------------------


def test_f6_0_unknown_preference_raises_engine_error_in_python_api():
    with pytest.raises(EngineError, match="Unknown engine preference"):
        BaseEngine.detect("turbo")


def test_f6_0_unknown_preference_raises_validation_error_in_config(monkeypatch):
    from ffengine.errors.exceptions import ValidationError

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    with pytest.raises(ValidationError, match="Unknown engine preference"):
        _validate(_validated_task(_engine_preference="turbo"))


def test_f6_0_normalize_helper_is_single_source():
    from ffengine.core.base_engine import (
        CANONICAL_ENGINE_PREFERENCES,
        normalize_engine_preference,
    )

    assert CANONICAL_ENGINE_PREFERENCES == ("auto", "standard", "pipeline", "spark")
    # Python API: alias cozulur
    with pytest.warns(DeprecationWarning):
        assert (
            normalize_engine_preference("enterprise", allow_legacy_aliases=True)
            == "auto"
        )
    # Config: alias reddedilir
    with pytest.raises(ValueError, match="legacy alias"):
        normalize_engine_preference("enterprise", allow_legacy_aliases=False)


# ---------------------------------------------------------------------------
# T-F6.0-1 / T-F6.0-4 — loader: kok `engine:` blogu -> runtime task dict
# ---------------------------------------------------------------------------

_YAML_TEMPLATE = """\
source_db_var: src_var
target_db_var: tgt_var
{engine_block}flow_tasks:
  - task_group_id: t1
    source_schema: public
    source_table: orders
    source_type: table
    target_schema: dw
    target_table: orders
    load_method: append
"""


def _write_config(tmp_path, engine_block: str = ""):
    path = tmp_path / "cfg.yaml"
    path.write_text(_YAML_TEMPLATE.format(engine_block=engine_block), encoding="utf-8")
    return str(path)


def test_f6_0_root_engine_preference_reaches_runtime_task(tmp_path, monkeypatch):
    from ffengine.config.loader import ConfigLoader
    from ffengine.config.schema import ENGINE_PREFERENCE_KEY

    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    path = _write_config(tmp_path, "engine:\n  preference: spark\n")
    task = ConfigLoader().load(path, "t1")
    assert task[ENGINE_PREFERENCE_KEY] == "spark"


def test_f6_0_missing_engine_block_defaults_to_auto(tmp_path):
    """T-F6.0-4: alan yoksa anahtar da yok -> tercih `auto` olarak cozulur."""
    from ffengine.airflow.operator import _resolve_engine_preference
    from ffengine.config.loader import ConfigLoader
    from ffengine.config.schema import ENGINE_PREFERENCE_KEY

    task = ConfigLoader().load(_write_config(tmp_path), "t1")
    assert ENGINE_PREFERENCE_KEY not in task
    assert _resolve_engine_preference(task) == "auto"


def test_f6_0_engine_block_shape_is_fail_loud(tmp_path):
    from ffengine.config.loader import ConfigLoader
    from ffengine.errors.exceptions import ConfigError

    scalar = _write_config(tmp_path, "engine: standard\n")
    with pytest.raises(ConfigError, match="mapping olmalidir|mapping olmalıdır"):
        ConfigLoader().load(scalar, "t1")


def test_f6_0_engine_block_unknown_field_is_fail_loud(tmp_path):
    from ffengine.config.loader import ConfigLoader
    from ffengine.errors.exceptions import ConfigError

    path = _write_config(tmp_path, "engine:\n  preference: auto\n  submit_mode: k8s\n")
    with pytest.raises(ConfigError, match="bilinmeyen alan"):
        ConfigLoader().load(path, "t1")


def test_f6_0_task_cannot_inject_private_engine_preference(tmp_path):
    """Root engine block is the only YAML authority for motor selection."""
    from ffengine.config.loader import ConfigLoader
    from ffengine.errors.exceptions import ConfigError

    path = tmp_path / "reserved-engine-key.yaml"
    path.write_text(
        _YAML_TEMPLATE.format(engine_block="").replace(
            "    load_method: append\n",
            "    load_method: append\n    _engine_preference: spark\n",
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="engine.preference|reserved|ayrilmis"):
        ConfigLoader().load(str(path), "t1")


@pytest.mark.parametrize("mode", ["missing_provider", "unavailable_provider"])
def test_f6_0_single_operator_path_fails_before_any_io(mode, monkeypatch):
    """T-F6.0-2/-3, tek-task yolu: `execute()` de I/O'dan once durur.

    Preflight `run_runtime_guards`'tan SONRA, connection adapter'dan ONCE.
    """
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    preference = _arrange_failure(mode)

    with (
        patch(_P_LOADER) as loader,
        patch(_P_ADAPTER) as adapter,
        patch(_P_DBSESS) as dbsession,
        patch(_P_WRITER) as writer,
    ):
        loader.return_value.load.return_value = _task_config(preference)
        with pytest.raises(EngineError):
            _make_operator().execute()

        adapter.get_connection_params.assert_not_called()
        dbsession.assert_not_called()
        writer.return_value.prepare.assert_not_called()
