"""Airflow 3-native Flow Studio plugin registration."""

from airflow.plugins_manager import AirflowPlugin

from ffengine.ui.api_app import flow_studio_app


class FlowStudioPlugin(AirflowPlugin):
    name = "flow_studio_plugin"
    fastapi_apps = [
        {
            "name": "flow_studio_fastapi",
            "app": flow_studio_app,
            "url_prefix": "/flow-studio",
        }
    ]
    # category: "browse" | "docs" | "admin" | "user" ile eşleşenler ilgili menünün İÇİNE konur.
    # Bunların dışında benzersiz bir değer verilirse Airflow yeni bir üst menü öğesi oluşturur (Yönetici altında değil).
    external_views = [
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
