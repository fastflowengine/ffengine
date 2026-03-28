# Airflow 3 Plugin Standard (FFEngine)

Bu dokuman ETL Studio eklentisinin Airflow 3 uzerindeki guncel standartlarini tanimlar.

## 1) Zorunlu plugin modeli
ETL Studio plugin'i su modelle gelistirilir:
- `fastapi_apps`
- `external_views`

Legacy FAB (`appbuilder_views`, `flask_blueprints`) yeni gelistirmelerde kullanilmaz.

## 2) Sol menu yerlesimi
- `external_views.destination = nav`
- `external_views.category` degeri `browse/docs/admin/user` disinda olursa ayri ust menu olur.

FFEngine standardi: ETL Studio, Admin altinda degil ayri nav ogesi olarak konumlanir.

## 3) UI standardi (guncel)
Ana sayfa su bolumleri icerir:
- Global Settings (Project & DB)
- Task paneli (Source/Target)
- Sekmeler: `Filter & Bindings`, `Partitioning`, `Performance`, `Mapping`
- Aksiyonlar: `Create DAG + YAML`, `Update DAG + YAML`, kesif (`schemas/tables/columns`), timeline
- Timeline filtreleri: `dag_id`, `state`, `limit`

## 4) CSS uyumu
- Airflow static CSS aday yollarini dinamik yuklemeyi dener.
- Bulunamazsa fallback CSS ile calismaya devam eder.

## 5) DAG + YAML uretim standardi (guncel)
- `POST /etl-studio/api/create-dag`:
  - `config.yaml` olusturur
  - generated DAG `.py` olusturur
- `POST /etl-studio/api/update-dag`:
  - ayni flow hedefinde `config.yaml` ve DAG'i gunceller
  - ETL Studio marker kontrolu uygular

Not: ETL Studio v2 source->target transfer modunda SQL metnini ayri `.sql` dosyasina yazma davranisi kaldirilmistir.

## 6) API sozlesme standardi
`DagUpsertPayload` sade ve strict'tir (`extra=forbid`):
- `source_type` yalnizca `table|view`
- payload disi alanlar 422 doner
- kaldirilan alanlar: `sql_text`, `tags`, `dag_prefix`, `reader_workers`, `writer_workers`, `pipe_queue_max`, `extraction_method`, `passthrough_full`

## 7) Guvenlik
- `ETL_STUDIO_API_KEY` tanimliysa mutasyon endpointleri (`create-dag`, `update-dag`) icin `X-ETL-Studio-API-Key` zorunludur.
- Bu katman Airflow RBAC yerine gecmez.

## 8) Test standardi
ETL Studio testleri en az su kapsamda olmalidir:
- health/index/schemas/tables/columns/timeline endpointleri
- create/update basari + hata yollari
- `config.yaml` icerik dogrulamasi (guncel payload alanlari)
- generated DAG marker dogrulamasi
- API key zorunlulugu
- kaldirilan alanlar icin 422 sozlesme testi

## 9) Operasyon
- Plugin veya UI degisikliklerinden sonra en az `airflow-webserver` restart edilmelidir.
- Gerekirse `airflow-scheduler` ve `airflow-dag-processor` da restart edilmelidir.

## 10) Referanslar
- `src/ffengine/ui/plugin.py`
- `src/ffengine/ui/api_app.py`
- `src/ffengine/ui/studio_service.py`
- `src/ffengine/ui/templates/etl_studio/index.html`
- `handbook/context/C08_ETL_STUDIO.md`
