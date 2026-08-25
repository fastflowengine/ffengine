"""F7.5 (EX-D039.9) — DAG Explorer compatibility layer.

DAG Explorer'in sahipligi FF Governance'a tasindi (EX-D039.9). Bu modul,
FFEngine tarafinda kalan TEK yeni parcadir: ffgovernance kurulu VE
lisansi AKTIF ise eski ``/flow-studio/dag-explorer`` HTML route'u FF
Governance Explorer'ina redirect edilir; AKSI HER DURUMDA (paket yok,
import/tespit hatasi, lisans yok/gecersiz/refused) yerlesik DAG Explorer
aynen calisir (T-F7.5-13 + R-f75 r-1 — calisan yerlesik ozellik hicbir
kosulda 403'lu bir governance sayfasina kurban edilmez).

Sinirlar:
- Community'ye ffgovernance BAGIMLILIGI eklenmez (EX-D039.1): pyproject
  0 satir; tespit runtime'dadir ve statik ``import ffgovernance`` satiri
  YOKTUR (EX-D039.1 grep=0 kurali korunur) — lisans sondasi
  ``importlib.import_module`` ile, try/except korumali, tamamen
  opsiyonel bir runtime probe'udur; HER hata "aktif degil" sayilir.
- Yalniz HTML ucu redirect eder; ``/api/dag-explorer`` ve
  ``/api/dag-search`` JSON uclari degismez.
- Flow Studio cekirdegine dokunulmaz; api_app.py'daki tek dokunus
  ``dag_explorer_index`` route govdesidir (kanit: branch diff'i).
"""

from __future__ import annotations

import importlib
import importlib.util

# FF Governance tek uygulamasi /ff-governance'a mount edilir (FFGovernance
# plugin'i, EX-D039.5); Explorer orada bir tab'dir.
FFGOVERNANCE_EXPLORER_URL = "/ff-governance/?tab=Explorer"


def _ffgovernance_license_active() -> bool:
    """ffgovernance lisansi aktif mi? (fail-safe: her hata -> False).

    R-f75 r-1: paket kurulu ama lisans yok/refused ise redirect,
    kullaniciyi 403 GOVERNANCE_LICENSE_REFUSED'a goturur ve FFEngine'in
    KENDI calisan Explorer'ini erisilemez kilardi. ffgovernance'in mevcut
    public degerlendirmesi (``licensing.evaluate_license().
    governance_active`` — valid|grace) hafif kanca olarak kullanilir;
    import/degerlendirme hatalari dahil HER anomali "aktif degil" olarak
    yorumlanir ve yerlesik Explorer servis edilir.
    """
    try:
        licensing = importlib.import_module("ffgovernance.licensing")
        return bool(licensing.evaluate_license().governance_active)
    except Exception:
        return False


def ffgovernance_explorer_url() -> str | None:
    """FF Governance kurulu VE lisansli ise Explorer URL'i, degilse None.

    Fail-safe tespit: find_spec'in olasi TUM hatalari (ValueError:
    ``module.__spec__ is None`` dahil) ve lisans sondasinin her hatasi
    "aktif degil" olarak yorumlanir — yerlesik DAG Explorer'in calismasi
    hicbir kosulda tespite/lisansa kurban edilmez (T-F7.5-13, R-f75 r-1).
    """
    try:
        spec = importlib.util.find_spec("ffgovernance")
    except Exception:
        return None
    if spec is None:
        return None
    if not _ffgovernance_license_active():
        return None
    return FFGOVERNANCE_EXPLORER_URL


def ffgovernance_owns_explorer() -> bool:
    """FF Governance kurulu VE lisansli mi? (navigasyon gorunurlugu icin).

    Kosulun TEK kaynagi ``ffgovernance_explorer_url``'dur; menu de ayni
    kosulu kullanir ki "menude gorunur ama tiklayinca redirect" gibi bir
    tutarsizlik olusmasin (girdi yalnizca redirect'e goturen olu bir
    baglanti haline gelmez).

    Fail-safe: her hata -> False -> menu girdisi GORUNUR. Route'taki kural
    "hata -> yerlesigi servis et", menudeki kural "hata -> girdiyi goster";
    ikisi de ayni sonuca cikar — yerlesik DAG Explorer erisilebilir kalir
    (R-f75 r-1).
    """
    try:
        return ffgovernance_explorer_url() is not None
    except Exception:
        return False
