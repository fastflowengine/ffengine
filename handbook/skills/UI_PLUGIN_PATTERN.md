# ETL Studio UI Plugin Pattern

## Amaç
Apache Airflow 3 sol menüsüne ETL Studio'yu **ayrı bir üst seviye nav öğesi** olarak eklemek
ve FastAPI tabanlı ETL/YAML/DAG yönetim ekranını sunmak.

## Kısıt
UI katmanı her iki versiyonda ortaktır. Enterprise özellikler UI'da görünebilir,
ancak runtime desteği scope'a göre kontrol edilmelidir.

---

## Airflow 3 Plugin Kayıt Yapısı
```python
# src/ffengine/ui/plugin.py
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
            # "browse"|"docs"|"admin"|"user" dışı: ayrı üst menü; "admin" ile ETL Studio Yönetici altına düşer.
            "category": "etl_studio",
        }
    ]
```

## Ekranlar ve Davranışlar

| Ekran | Yol | Davranış |
|---|---|---|
| Ana ekran | `/etl-studio/` | ETL Configuration Studio (Global Settings + Task #1 + sekmeler) |
| Şema keşfi | `/etl-studio/api/schemas` | Seçili connection'daki şema listesi |
| Tablo keşfi | `/etl-studio/api/tables?schema=X&q=Y` | 50 tablo limiti; `q` ile typeahead filtre |
| DAG oluştur | `/etl-studio/api/create-dag` | YAML + DAG dosyası üretir; SQL ayrı `.sql` dosyasına yazılır |
| DAG güncelle | `/etl-studio/api/update-dag` | Mevcut config güncellenir; DAG dosyası yeniden üretilir |
| Timeline | `/etl-studio/api/timeline?limit=&dag_id=&state=` | Son DagRun kayıtları; `dag_id` ve `state` ile filtre (T10) |

## Airflow 3 UI uyumu
- Sayfa, Airflow 3 static CSS aday yollarını (`/static/dist/main.css` vb.) **dinamik** olarak
  yüklemeyi dener; bulunamazsa local fallback CSS ile çalışır.
- Form düzeni eski ETL studio ekranına benzer şekilde bölümlenir:
  - Global Settings (Project & DB)
  - Task paneli (Source/Target)
  - Sekmeler: Filter & Bindings, Partitioning, Performance, Mapping
- Mevcut implementasyon tek-task payload üretir; backend `config.yaml` içine tek bir `etl_tasks[0]`
  yazar ve DAG dosyasını `register_dags(...)` üzerinden üretir.

## Şema Keşfi Kuralları
- Maksimum 50 tablo döner; fazlası sayfalanır
- Arama filtresi harf girildiğinde tetiklenir (typeahead — minimum 2 karakter)
- Kolon metadata: ad, tip, nullable, precision, scale
- Büyük tablolarda (>1M satır) satır sayısı tahmini gösterilir, kesin değil

## DAG Oluşturma Kuralları
- `task_group_id` → `{src_schema}_{src_table}_to_{tgt_schema}_{tgt_table}_v1` formatında otomatik üretilir
- SQL sorgusu varsa `sql/` dizinine ayrı dosya olarak yazılır; `config.yaml`'da `sql_file:` ile referans verilir
- Tag'ler dizin yolundan otomatik türetilir: `{domain}/{level}/{direction}`; `.etl_studio.json` içinde `auto_tags` / `user_tags` ayrımı (T09)
- Üretilen dosyalar `FFENGINE_STUDIO_PROJECTS_ROOT` altında `projects/{proje}/{domain}/level{N}/{yön}/` dizinine yazılır; DAG `.py` dosyası `FFENGINE_STUDIO_DAG_ROOT` altında (T08 path kontrolü)
- Create/Update öncesi pipeline gövdesi `ConfigValidator` ile doğrulanır (T06)

## Tag Yönetimi
```python
# Dizin yapısından otomatik tag türetme
path = "projects/webhook/whk/level1/src_to_stg"
tags = ["whk", "level1", "src_to_stg"]

# UI'den ek tag eklenebilir; otomatik tag'ler silinmez
```

## Enterprise UI Notu
Enterprise özellikleri (queue depth gösterimi, throughput grafiği, multi-lane status)
UI'da render edilebilir ancak sayfa render öncesinde şu kontrol yapılmalıdır:
```python
from ffengine.core.engine_interface import BaseEngine
is_enterprise = BaseEngine.detect("auto").is_available() and \
                type(BaseEngine.detect("auto")).__name__ == "CEngine"
```

## Mutasyon API güvenliği (T12)
- Ortamda `ETL_STUDIO_API_KEY` tanımlıysa `POST /api/create-dag` ve `POST /api/update-dag` istekleri `X-ETL-Studio-API-Key` başlığı ile aynı değeri göndermelidir; aksi halde 401.

## Agent Kontrol Listesi
1. Plugin `AirflowPlugin` sınıfından mı türetiliyor?
2. `fastapi_apps` içinde `name/app/url_prefix` alanları var mı?
3. `external_views` ile sol menüye ETL Studio bağlantısı eklendi mi?
4. Legacy FAB alanları (`flask_blueprints`, `appbuilder_views`) kullanılmıyor mu?
5. Şema keşfi 50 tablo limitini uyguluyor mu?
6. DAG oluşturma SQL'i ayrı dosyaya mı yazıyor?
7. Tag'ler dizin yolundan türetiliyor mu?
8. Enterprise-only görünümler `is_enterprise` kontrolüne bağlı mı?
9. Production’da mutasyon endpoint’leri için `ETL_STUDIO_API_KEY` + başlık politikası net mi?
10. `DagUpsertPayload` alanları `VALID_*` şema sabitleriyle uyumlu mu (T05/T06)?