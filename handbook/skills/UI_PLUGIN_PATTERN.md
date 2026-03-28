# ETL Studio UI Plugin Pattern

## Amac
Airflow 3 sol menuye ETL Studio'yu ayri ust seviye nav ogesi olarak eklemek ve FastAPI tabanli ETL/YAML/DAG yonetim ekranini sunmak.

## Kisit
Bu pattern ETL Studio'nin source->target transfer odakli sade v2 sozlesmesini anlatir.

---

## Airflow 3 Plugin Kayit Yapisi
```python
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
    external_views = [
        {
            "name": "ETL Studio",
            "href": "/etl-studio/",
            "destination": "nav",
            "url_route": "etl_studio",
            "category": "etl_studio",
        }
    ]
```

## Ekranlar ve Davranislar
| Ekran | Yol | Davranis |
|---|---|---|
| Ana ekran | `/etl-studio/` | ETL Configuration Studio |
| Sema kesfi | `/etl-studio/api/schemas` | Conn'a ait schema listesi |
| Tablo kesfi | `/etl-studio/api/tables?schema=X&q=Y` | 50 limit + offset, `q` opsiyonel |
| DAG olustur | `/etl-studio/api/create-dag` | YAML + DAG dosyasi uretir |
| DAG guncelle | `/etl-studio/api/update-dag` | Config ve DAG'i gunceller |
| Timeline | `/etl-studio/api/timeline?limit=&dag_id=&state=` | DagRun listesi + filtre |

## Guncel Payload Pattern
Form payload'i yalnizca aktif alanlari gonderir:
- `project`
- `source_conn_id`, `target_conn_id`
- `source_schema`, `source_table`, `source_type(table|view)`
- `target_schema`, `target_table`, `load_method`
- `column_mapping_mode`, `mapping_file?`
- `where?`, `batch_size`
- `partitioning_*`
- `task_group_id?`

`DagUpsertPayload` strict'tir (`extra=forbid`).

## Kaldirilan Alanlar
Asagidaki alanlar UI/API v2'den kaldirilmistir:
- `sql_text`, `tags`, `dag_prefix`
- `reader_workers`, `writer_workers`, `pipe_queue_max`
- `extraction_method`, `passthrough_full`

## Timeline UX
UI'da timeline icin su filtreler vardir:
- `dag_id`
- `state`
- `limit` (1..200)

## Mutasyon API Guvenligi
- `ETL_STUDIO_API_KEY` tanimliysa `POST /api/create-dag` ve `POST /api/update-dag` icin `X-ETL-Studio-API-Key` zorunludur.

## Agent Kontrol Listesi
1. Plugin `AirflowPlugin`'den turemis mi?
2. `fastapi_apps` icinde `name/app/url_prefix` var mi?
3. `external_views` ile nav girisi ekli mi?
4. Legacy FAB alanlari kullanilmiyor mu?
5. UI payload'i guncel sade sozlesmeyle uyumlu mu?
6. Kaldirilan alanlar API'de 422 donuyor mu?
7. Timeline filtreleri UI'dan API'ye dogru tasiniyor mu?
