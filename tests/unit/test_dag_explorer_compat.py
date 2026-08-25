"""F7.5 (EX-D039.9) — DAG Explorer compat katmani testleri.

T-F7.5-13: ffgovernance kurulu OLMAYAN Community kurulumunda DAG Explorer
calismaya devam eder (fallback; hicbir Studio akisi kirilmaz).
T-F7.5-14: ffgovernance kuruluysa eski `/flow-studio/dag-explorer` route'u
HTTP seviyesinde FF Governance Explorer'a redirect eder (bookmark kirilmaz).

Bu dosya + `src/ffengine/ui/dag_explorer_compat.py` compat katmaninin
TAMAMIDIR; Flow Studio cekirdegi diff'i bos kalir (T-F7.5-11 kaniti
`git diff main..agent/f7-5-compat --name-only` ile evidence'ta).
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import sys
import types
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import ffengine.ui.api_app as api_app_module
import ffengine.ui.plugin as plugin_module
from ffengine.ui.api_app import flow_studio_app


@pytest.fixture()
def client():
    return TestClient(flow_studio_app)


def _fake_ffgovernance_module() -> types.ModuleType:
    fake = types.ModuleType("ffgovernance")
    # find_spec, sys.modules'taki modulun __spec__'ini kullanir; None birakmak
    # ValueError uretir (o yol ayrica test ediliyor) — gercek kurulumu temsil
    # eden sahte modul gecerli bir spec tasir.
    fake.__spec__ = importlib.machinery.ModuleSpec("ffgovernance", loader=None)
    return fake


class _FakeLicenseState:
    def __init__(self, active: bool):
        self.governance_active = active


def _fake_licensing_module(active: bool) -> types.ModuleType:
    licensing = types.ModuleType("ffgovernance.licensing")
    licensing.__spec__ = importlib.machinery.ModuleSpec(
        "ffgovernance.licensing", loader=None
    )
    licensing.evaluate_license = lambda **kw: _FakeLicenseState(active)
    return licensing


def _install_fake_ffgovernance(monkeypatch, *, licensed: bool):
    """sys.modules'a gecerli spec'li sahte ffgovernance + licensing kurar."""
    pkg = _fake_ffgovernance_module()
    licensing = _fake_licensing_module(licensed)
    pkg.licensing = licensing
    monkeypatch.setitem(sys.modules, "ffgovernance", pkg)
    monkeypatch.setitem(sys.modules, "ffgovernance.licensing", licensing)


# --- T-F7.5-13: gercek Community ortaminda fallback ---------------------


def test_environment_truth_ffgovernance_absent():
    # Kanonik Community suite ffgovernance'siz kosar; bu test ortam
    # varsayimini GORUNUR kilar (bozulursa asagidaki fallback kanitlari
    # 'yanlislikla yesil' olamaz).
    assert importlib.util.find_spec("ffgovernance") is None


def test_explorer_html_works_without_ffgovernance(client):
    # T-F7.5-13: redirect YOK, mevcut Explorer HTML'i aynen servis edilir.
    r = client.get("/dag-explorer")
    assert r.status_code == 200
    assert "/api/dag-explorer" in r.text
    assert r.headers.get("cache-control") == "no-store"


def test_explorer_json_api_works_without_ffgovernance(client):
    # T-F7.5-13: JSON uclari da regresyonsuz (mock'lu discover — DB'siz).
    mocked = {"root": "/opt/airflow/dags", "items": [], "count": 0}
    with patch.object(
        api_app_module, "discover_dag_explorer_items", return_value=mocked
    ):
        r = client.get("/api/dag-explorer")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_detection_failure_falls_back_to_builtin_explorer(client, monkeypatch):
    # Sessiz-guvenli tespit: find_spec patlarsa (bozuk/yarim kurulum)
    # "kurulu degil" sayilir ve yerlesik Explorer calisir.
    from ffengine.ui import dag_explorer_compat

    def _boom(name):
        raise ValueError("module.__spec__ is None")

    monkeypatch.setattr(
        dag_explorer_compat.importlib.util, "find_spec", _boom
    )
    r = client.get("/dag-explorer")
    assert r.status_code == 200
    assert "/api/dag-explorer" in r.text


# --- T-F7.5-14: kuruluysa HTTP redirect ---------------------------------


def test_redirects_to_ffgovernance_when_installed(client, monkeypatch):
    # HTTP seviyesinde dogrulama (mock route tablosu DEGIL): gercek route,
    # gercek TestClient; ffgovernance sys.modules'a gecerli spec'li sahte
    # modul olarak enjekte edilir (find_spec bunu 'kurulu' gorur) ve
    # lisansi AKTIF'tir (R-f75 r-1: redirect yalniz kurulu+lisansli).
    _install_fake_ffgovernance(monkeypatch, licensed=True)
    r = client.get("/dag-explorer", follow_redirects=False)
    assert r.status_code == 307
    assert r.headers["location"] == "/ff-governance/?tab=Explorer"


def test_installed_but_unlicensed_serves_builtin_explorer(client, monkeypatch):
    # R-f75 r-1: paket kurulu AMA lisans yok/refused -> redirect YOK;
    # yerlesik Explorer aynen calisir (403'e kurban edilmez).
    _install_fake_ffgovernance(monkeypatch, licensed=False)
    r = client.get("/dag-explorer", follow_redirects=False)
    assert r.status_code == 200
    assert "/api/dag-explorer" in r.text
    assert r.headers.get("cache-control") == "no-store"


def test_license_probe_error_serves_builtin_explorer(client, monkeypatch):
    # Lisans sondasi patlarsa (bozuk kurulum, beklenmeyen API) fail-safe:
    # "aktif degil" sayilir, yerlesik Explorer calisir.
    pkg = _fake_ffgovernance_module()
    licensing = _fake_licensing_module(True)

    def _boom(**kw):
        raise RuntimeError("licensing exploded")

    licensing.evaluate_license = _boom
    pkg.licensing = licensing
    monkeypatch.setitem(sys.modules, "ffgovernance", pkg)
    monkeypatch.setitem(sys.modules, "ffgovernance.licensing", licensing)
    r = client.get("/dag-explorer", follow_redirects=False)
    assert r.status_code == 200
    assert "/api/dag-explorer" in r.text


def test_json_apis_do_not_redirect_even_when_installed(client, monkeypatch):
    # Compat sinirlari: yalniz HTML ucu redirect eder; JSON uclari
    # (baska araclarin da kullanabilecegi veri uclari) DEGISMEZ.
    _install_fake_ffgovernance(monkeypatch, licensed=True)
    mocked = {"root": "/opt/airflow/dags", "items": [], "count": 0}
    with patch.object(
        api_app_module, "discover_dag_explorer_items", return_value=mocked
    ):
        r = client.get("/api/dag-explorer", follow_redirects=False)
    assert r.status_code == 200
    assert r.json()["ok"] is True


# --- Menu gorunurlugu: redirect kosuluyla AYNI kosul --------------------


def _has_dag_explorer(views) -> bool:
    return any(v.get("url_route") == "dag_explorer" for v in views)


def _reload_plugin_views(monkeypatch=None, **fake_kwargs):
    """Sahte ffgovernance kuruluyken plugin.py'yi yeniden import eder.

    `external_views` SINIF GOVDESINDE hesaplanir ve modul test aninda
    zaten import edilmistir; kosulu degistirip reload etmek TEK yoldur.
    Sahte modul reload'DAN ONCE sys.modules'a konur (sonda sinif govdesi
    calisirken kosar).
    """
    if fake_kwargs:
        _install_fake_ffgovernance(monkeypatch, **fake_kwargs)
    reloaded = importlib.reload(plugin_module)
    return reloaded.FlowStudioPlugin.external_views


@pytest.fixture()
def plugin_views(monkeypatch):
    """Reload eden yardimci; teardown'da modulu TEMIZ haline dondurur.

    Son reload sahteler kaldirildiktan SONRA yapilir; boylece sonraki
    testler gercek (ffgovernance'siz) external_views'i gorur.
    """
    yield _reload_plugin_views
    monkeypatch.undo()
    importlib.reload(plugin_module)


def test_menu_shows_dag_explorer_without_ffgovernance(plugin_views):
    # Kanonik Community ortami (ffgovernance YOK): menu bugunku gibi.
    assert _has_dag_explorer(plugin_views())


def test_menu_entry_is_unchanged_when_shown(plugin_views):
    # Girdi ve SIRASI bugunku ile birebir ayni (regresyon kilidi).
    views = plugin_views()
    assert views[2] == plugin_module._DAG_EXPLORER_VIEW
    assert [v["url_route"] for v in views] == [
        "flow_studio",
        "flow_studio_update",
        "dag_explorer",
        "mail_templates",
    ]


def test_menu_hides_dag_explorer_when_licensed(plugin_views, monkeypatch):
    # FFGov kurulu VE lisansli -> route zaten 307; menude olu girdi kalmaz.
    views = plugin_views(monkeypatch, licensed=True)
    assert not _has_dag_explorer(views)
    # Diger girdiler DOKUNULMAZ.
    assert [v["url_route"] for v in views] == [
        "flow_studio",
        "flow_studio_update",
        "mail_templates",
    ]


def test_menu_shows_dag_explorer_when_unlicensed(plugin_views, monkeypatch):
    # R-f75 r-1'in menu ayagi: lisanssizken redirect YOK -> girdi GORUNUR
    # (yerlesik Explorer erisilemez hale getirilmez).
    assert _has_dag_explorer(plugin_views(monkeypatch, licensed=False))


def test_menu_shows_dag_explorer_when_probe_raises(plugin_views, monkeypatch):
    # Fail-safe YONU: sonda patlarsa girdi GIZLENMEZ, gorunur kalir ve
    # plugin yuklenmesi cokmez.
    from ffengine.ui import dag_explorer_compat

    def _boom():
        raise RuntimeError("probe exploded")

    monkeypatch.setattr(
        dag_explorer_compat, "ffgovernance_owns_explorer", _boom
    )
    assert _has_dag_explorer(plugin_views())


def test_owns_explorer_mirrors_redirect_condition(monkeypatch):
    # Turetilmis yuklem ile route kosulu AYNI seyi soyler (drift olamaz).
    from ffengine.ui import dag_explorer_compat

    assert dag_explorer_compat.ffgovernance_owns_explorer() is False
    _install_fake_ffgovernance(monkeypatch, licensed=True)
    assert dag_explorer_compat.ffgovernance_owns_explorer() is True
    assert dag_explorer_compat.ffgovernance_explorer_url() is not None


def test_compat_layer_has_no_pyproject_dependency():
    # Community'ye ffgovernance bagimliligi EKLENMEZ (EX-D039.1/9):
    # pyproject icinde ffgovernance gecmez.
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    text = (repo_root / "pyproject.toml").read_text(encoding="utf-8")
    assert "ffgovernance" not in text
