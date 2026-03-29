"""Airflow 3-native ETL Studio plugin registration."""

from airflow.plugins_manager import AirflowPlugin

from ffengine.ui.api_app import etl_studio_app


class ETLStudioPlugin(AirflowPlugin):
    name = "etl_studio_plugin"
    fastapi_apps = [
        {
            "name": "etl_studio_fastapi",
            "app": etl_studio_app,
            "url_prefix": "/etl-studio",
        }
    ]
    # category: "browse" | "docs" | "admin" | "user" ile eşleşenler ilgili menünün İÇİNE konur.
    # Bunların dışında benzersiz bir değer verilirse Airflow yeni bir üst menü öğesi oluşturur (Yönetici altında değil).
    external_views = [
        {
            "name": "ETL Studio",
            "href": "/etl-studio/",
            "destination": "nav",
            "url_route": "etl_studio",
            "category": "etl_studio",
        },
        {
            "name": "ETL Studio Update",
            "href": "/etl-studio/?dag_id={DAG_ID}",
            "destination": "dag",
            "url_route": "etl_studio_update",
            "category": "etl_studio",
        },
    ]
