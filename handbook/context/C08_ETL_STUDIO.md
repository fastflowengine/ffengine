# CONTEXT: C08 - ETL Studio UI

## Amac
Airflow 3.x ile uyumlu ETL Studio arayuzunun source->target transfer odakli, sade ve tutarli bir sozlesme ile calismasi.

## Airflow 3 Uyum Notu
- Plugin kaydi `fastapi_apps + external_views` ile yapilir.
- Legacy FAB (`flask_blueprints`, `appbuilder_views`) yeni gelistirmelerde kullanilmaz.
- ETL Studio API route'lari `/etl-studio/api/*` altinda FastAPI ile sunulur.

## Guncel V2 Kararlari (Mart 2026)
- Geriye uyumluluk zorunlulugu yoktur.
- ETL Studio payload yalnizca transferde aktif kullanilan alanlari icerir.
- No-op veya yaniltici alanlar UI ve API'den kaldirilmistir.

## Guncel Payload Ozeti
`DagUpsertPayload` icin temel alanlar:
- `project`
- `source_conn_id`, `target_conn_id`
- `source_schema`, `source_table`, `source_type` (`table|view`)
- `target_schema`, `target_table`
- `load_method`
- `column_mapping_mode`, `mapping_file` (opsiyonel, mode=mapping_file)
- `where` (opsiyonel)
- `batch_size`
- `partitioning_enabled`, `partitioning_mode`, `partitioning_column`, `partitioning_parts`, `partitioning_ranges`
- `task_group_id` (opsiyonel, bos ise otomatik)

`extra = forbid` oldugu icin payload disi alanlar 422 doner.

## Kaldirilan UI/API Alanlari
- `domain`, `level`, `direction`, `dag_prefix`
- `sql_text`, `tags`
- `reader_workers`, `writer_workers`, `pipe_queue_max`
- `extraction_method`, `passthrough_full`

Not: Servis katmani icerde hala flow path ve auto tag turetir; ancak bunlar artik UI/API sozlesmesinin parcasi degildir.

## Endpointler
- UI: `/etl-studio/`
- Health: `/etl-studio/health`
- Schemas: `/etl-studio/api/schemas?conn_id=`
- Tables: `/etl-studio/api/tables?conn_id=&schema=&q=&limit=50&offset=0`
- Columns: `/etl-studio/api/columns?conn_id=&schema=&table=`
- Create DAG: `POST /etl-studio/api/create-dag`
- Update DAG: `POST /etl-studio/api/update-dag`
- Timeline: `/etl-studio/api/timeline?limit=&dag_id=&state=`

## UI Durumu
- Tek task payload ureten form devam eder.
- Timeline icin `dag_id`, `state`, `limit` filtre alanlari UI'da vardir.
- Form SQL editor veya tag editor icermez.

## Guvenlik
- `ETL_STUDIO_API_KEY` tanimliysa create/update endpointleri `X-ETL-Studio-API-Key` ister.
- Bu kontrol Airflow RBAC yerine gecmez; ek katmandir.

## Test Kapsami
- `tests/unit/test_etl_studio_api.py` guncel payload sozlesmesini test eder.
- Kaldirilan alanlarin 422 donmesi dogrulanir.

## Not
Bu dokuman ETL Studio'nin guncel source->target sade transfer modunu tanimlar.
