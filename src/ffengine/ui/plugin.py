"""Airflow 3-native Flow Studio plugin registration."""

from airflow.plugins_manager import AirflowPlugin

from ffengine.ui.api_app import flow_studio_app

_TRANSPARENT_ICON_DATA_URI = (
    "data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs="
)

_DAG_EXPLORER_VIEW = {
    "name": "DAG Explorer",
    "href": "/flow-studio/dag-explorer",
    "destination": "nav",
    "url_route": "dag_explorer",
    "category": "browse",
    "icon": _TRANSPARENT_ICON_DATA_URI,
    "icon_dark_mode": _TRANSPARENT_ICON_DATA_URI,
}


def _build_external_views() -> list[dict]:
    """Navigasyon girdileri; DAG Explorer sahipligine gore filtrelenir.

    F7.5 (EX-D039.9): ffgovernance kurulu VE lisansli ise Explorer'in
    sahibi FF Governance'tir ve route zaten 307 ile oraya yonlendirir;
    menude ayri bir "DAG Explorer" girdisi tutmak kullaniciyi yalnizca
    redirect'e goturen olu bir baglanti olur, bu yuzden girdi gizlenir.
    AKSI HER DURUMDA (paket yok, lisans yok/refused, tespit hatasi) girdi
    AYNEN gorunur — fail-safe yonu: calisan yerlesik Explorer hicbir
    kosulda tespite kurban edilmez (R-f75 r-1).

    Airflow ``_get_ui_plugins()`` @cache'lidir: bu liste surec basina BIR
    KEZ, plugin import aninda hesaplanir (per-request degerlendirme
    imkansizdir) — lisans durumu degisirse navigasyonun guncellenmesi icin
    webserver restart gerekir; ROUTE ise her istekte canli lisans durumunu
    yansitir. Sonda maliyeti bir ``find_spec`` + lisans okumasidir ve
    webserver/scheduler acilisinda calisir; DAG parse yolunda DEGIL
    (bu modulu DAG tarafindan import eden kod yoktur).
    """
    views = [
        {
            "name": "Flow Studio",
            "href": "/flow-studio/",
            "destination": "nav",
            "url_route": "flow_studio",
            "category": "flow_studio",
        },
        {
            "name": "Flow Studio Update",
            "href": "/flow-studio/?dag_id={DAG_ID}",
            "destination": "dag",
            "url_route": "flow_studio_update",
            "category": "flow_studio",
        },
    ]
    # Import fonksiyon icinde ve korumali: hicbir sonda hatasi plugin
    # yuklenmesini cokertemez (api_app.dag_explorer_index ile ayni kalip).
    try:
        from ffengine.ui.dag_explorer_compat import (
            ffgovernance_owns_explorer,
        )

        hide_dag_explorer = ffgovernance_owns_explorer()
    except Exception:
        hide_dag_explorer = False
    if not hide_dag_explorer:
        views.append(_DAG_EXPLORER_VIEW)
    views.append(
        {
            "name": "Mail Templates",
            "href": "/flow-studio/mail-templates",
            "destination": "nav",
            "url_route": "mail_templates",
            "category": "admin",
            "icon": _TRANSPARENT_ICON_DATA_URI,
            "icon_dark_mode": _TRANSPARENT_ICON_DATA_URI,
        }
    )
    return views


class FlowStudioPlugin(AirflowPlugin):
    name = "flow_studio_plugin"
    fastapi_apps = [
        {
            "name": "flow_studio_fastapi",
            "app": flow_studio_app,
            "url_prefix": "/flow-studio",
        }
    ]
    # Categories matching "browse" | "docs" | "admin" | "user"
    # are rendered inside the corresponding Airflow menu section.
    # Any unique custom value creates a new top-level menu section.
    external_views = _build_external_views()
