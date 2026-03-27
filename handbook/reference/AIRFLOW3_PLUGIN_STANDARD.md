# Airflow 3 Plugin Standard (FFEngine)

Bu dokuman, FFEngine icin ETL Studio eklentisinin **standart mimari** kurallarini tanimlar.

## 1) Zorunlu plugin modeli

ETL Studio eklentisi Airflow 3'te su model ile gelistirilir:

- `fastapi_apps`
- `external_views`

Legacy FAB (`appbuilder_views`, `flask_blueprints`) yeni gelistirmelerde kullanilmaz.

## 2) Sol menu yerlesim standardi

- `external_views.destination` degeri `nav` olmalidir.
- `external_views.category` degeri:
  - `browse`, `docs`, `admin`, `user` olursa ilgili menu altina duser.
  - Bu dort deger disinda olursa ayri bir ust menu ogesi olusturur.

FFEngine standardi: ETL Studio, **Yonetici altina degil** ayri nav ogesi olarak konumlanir.

## 3) UI standardi (ETL Configuration Studio)

ETL Studio ana sayfasi asagidaki bolumleri icermelidir:

- Global Settings (Project & DB)
- Task paneli (Source/Target)
- Sekmeler: `Filter & Bindings`, `Partitioning`, `Performance`, `Mapping`
- Aksiyonlar: `Create DAG + YAML`, `Update DAG + YAML`, kesif (`schemas/tables/columns`)

## 4) CSS uyum standardi

- Sayfa, Airflow static CSS aday yollarini dinamik olarak yuklemeyi denemelidir.
- Static CSS bulunamazsa fallback CSS ile calismaya devam etmelidir.

Bu kural, farkli Airflow paketleme ciktilarinda UI kirilmalarini azaltir.

## 5) DAG + YAML uretim standardi

- `POST /etl-studio/api/create-dag`:
  - `config.yaml` olusturur
  - generated DAG `.py` olusturur
  - `source_type=sql` ise SQL'i ayri `.sql` dosyasina yazar
- `POST /etl-studio/api/update-dag`:
  - Ayni flow hedefinde `config.yaml` ve DAG'i gunceller
  - ETL Studio marker kontrolu uygular

## 6) Guvenlik standardi

- `ETL_STUDIO_API_KEY` tanimliysa mutasyon endpointleri (`create-dag`, `update-dag`)
  `X-ETL-Studio-API-Key` basligini zorunlu kilar.
- Bu katman Airflow RBAC'in yerine gecmez; ek koruma katmanidir.

## 7) Bagimlilik standardi

- Airflow 3.1.x ve FastAPI surumu uyumlu aralikta sabitlenmelidir.
- Dev/test ortaminda da ayni uyum korunmalidir.

## 8) Test standardi

ETL Studio testleri en az su kapsamda olmalidir:

- health/index/schemas/tables/columns/timeline endpointleri
- create/update basari + hata yollari
- `config.yaml` icerik dogrulamasi (ileri alanlar dahil)
- SQL dosyasi uretimi (`source_type=sql`)
- generated DAG marker dogrulamasi
- API key zorunlulugu senaryolari

## 9) Operasyon standardi

- Plugin veya UI degisikliklerinden sonra en az `airflow-webserver` restart edilmelidir.
- Gerekirse `airflow-scheduler` ve `airflow-dag-processor` da restart edilmelidir.

## 10) Referanslar

- `src/ffengine/ui/plugin.py`
- `src/ffengine/ui/api_app.py`
- `src/ffengine/ui/studio_service.py`
- `src/ffengine/ui/templates/etl_studio/index.html`
- `handbook/context/C08_ETL_STUDIO.md`
