# CONTEXT: C08 — ETL Studio UI

## Amaç
Airflow 3.1.6 ile uyumlu ETL Studio low-code yönetim arayüzünü geliştir.

## Airflow 3 Uyum Notu
- Legacy FAB (`flask_blueprints`, `appbuilder_views`) yolu **deprecated**.
- C08'de plugin kaydı `fastapi_apps` + `external_views` üzerinden yapılmalıdır.
- ETL Studio API route'ları `"/etl-studio/api/*"` altında FastAPI ile sunulmalıdır.

## Faz 1 (MVP) — T01–T04, T07, T11
- Plugin: `fastapi_apps` + `external_views` (`src/ffengine/ui/plugin.py`).
- Keşif: şema / tablo (typeahead, `limit`≤50, `q` en az 2 karakter) / kolon.
- Create/Update DAG: YAML + üretilen DAG dosyası; SQL ayrı dosyada.
- MVP HTML: `/` altında servis (`templates/etl_studio/index.html`).

## Faz 2 (Üretimleşme) — T05–T06–T08–T09–T10–T12
| Task | Kapsam | Uygulama özeti |
|------|--------|----------------|
| **T05** | Pipeline formu genişletilmiş alanlar | `DagUpsertPayload`: batch/worker, extraction, partitioning, `passthrough_full`, `task_group_id` vb. |
| **T06** | YAML kurallarıyla hizalı doğrulama | `validate_pipeline_payload` → `build_task_dict_for_validation` + `ConfigValidator` (`studio_service.py`). |
| **T08** | Path güvenliği | Üretilen DAG yolu `FFENGINE_STUDIO_DAG_ROOT` altında; `_ensure_path_under_root`. |
| **T09** | Tag birleştirme | `.etl_studio.json`: `auto_tags` / `user_tags`; `tags` gövdesi yoksa mevcut tag’ler korunur (update). |
| **T10** | Timeline + filtre | `GET /api/timeline?limit=&dag_id=&state=`; `fetch_timeline_runs` (Airflow `DagRun`). UI’da filtre alanları. |
| **T12** | Mutasyon için API anahtarı | `ETL_STUDIO_API_KEY` tanımlıysa `POST create-dag` / `update-dag` için `X-ETL-Studio-API-Key` zorunlu (`api_app._optional_api_key_dep`). |

## Ortam değişkenleri
- `FFENGINE_STUDIO_PROJECTS_ROOT` — proje/flow dizini (varsayılan: `/opt/airflow/dags/projects`).
- `FFENGINE_STUDIO_DAG_ROOT` — üretilen DAG `.py` kökü (varsayılan: `/opt/airflow/dags/generated`).
- `ETL_STUDIO_API_KEY` — (opsiyonel) ayarlanırsa mutasyon endpoint’leri bu anahtarı ister.

## C08_T13 — Testler
- `tests/unit/test_etl_studio_api.py`: FastAPI `TestClient` ile health, index, mock’lu keşif, create/update DAG, timeline, Pydantic validasyonu, API anahtarı davranışı.

## UI Revizyonu (Mart 2026)
- Sayfa adı ve yerleşim: **ETL Configuration Studio**.
- Eski örnek ekranla uyumlu düzen:
  - Global Settings (Project & DB)
  - Task #1 paneli
  - Sekmeler: `Filter & Bindings`, `Partitioning`, `Performance`, `Mapping`
- Kullanıcı aksiyonları:
  - `Get Schemas`, `Get Tables`, `Get Columns`
  - `Create DAG + YAML`, `Update DAG + YAML`, `Load Timeline`
- Stil uyumu:
  - Airflow 3 static CSS dosyaları varsa dinamik yüklenir,
  - bulunamazsa fallback CSS ile çalışır.
- Backend davranışı:
  - create/update akışı `config.yaml` + DAG `.py` üretir/günceller,
  - `source_type=sql` ise SQL içeriği ayrı `.sql` dosyasına yazılır.

## Beklenen Ekranlar
- Şema/tablo keşfi
- Pipeline formu
- SQL editörü / dosya seçimi
- Tag ve timeline görünümü

## Route'lar
- UI: `/etl-studio/` (Airflow mount: `/etl-studio` prefix)
- Health: `/etl-studio/health` — yanıtta `dag_marker` (ETL Studio imzası).
- Schemas: `/etl-studio/api/schemas?conn_id=`
- Tables: `/etl-studio/api/tables?conn_id=&schema=&q=&limit=50&offset=0`
- Columns: `/etl-studio/api/columns?conn_id=&schema=&table=`
- Create DAG: `POST /etl-studio/api/create-dag`
- Update DAG: `POST /etl-studio/api/update-dag`
- Timeline: `/etl-studio/api/timeline?limit=50&dag_id=&state=`

---

## C08_T14 — Kullanım

### Erişim
- Airflow Web UI sol menüde **Yönetici (Admin) ile aynı seviyede ayrı bir öğe** olarak **ETL Studio** (`external_views` içinde `category` değeri `browse`/`docs`/`admin`/`user` dışında tutulur; böylece Yönetici alt menüsüne düşmez). Doğrudan: `{AIRFLOW_BASE_URL}/etl-studio/`.
- Plugin’in yüklendiğini doğrulamak: `GET {BASE}/etl-studio/health` — `ok: true` ve `dag_marker` içinde `generated_by: etl_studio` beklenir.

### Docker / ortam
- `docker/docker-compose.yml` içinde `plugins/` ve `src/` mount’ları; `AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins`.
- `FFENGINE_STUDIO_PROJECTS_ROOT` ve `FFENGINE_STUDIO_DAG_ROOT` container’da Airflow’un gördüğü `dags` ağacına işaret etmeli (varsayılanlar: `.../dags/projects`, `.../dags/generated`).

### UI akışı (özet)
1. **Connection ID** ile şema listesini al; **Schema** ve isteğe bağlı **Arama** (en az 2 karakter) ile tabloları listele.
2. Task panelinde source/target alanlarını ve sekmelerde filter/partitioning/performance/mapping alanlarını doldur.
3. **Create DAG + YAML** veya **Update DAG + YAML** ile üretim/güncelleme yap (aynı `project/domain/level/direction` hedefi korunmalı).
4. **Load Timeline** ile son DAG run durumlarını izle (`dag_id` / `state` filtreleri API tarafında desteklenir).

### API ile mutasyon (anahtar açıksa)
`ETL_STUDIO_API_KEY` tanımlıysa `POST` isteklerine başlık ekleyin:
`X-ETL-Studio-API-Key: <aynı değer>`.

---

## C08_T14 — Sorun giderme

| Belirti | Olası neden | Ne yapılmalı |
|--------|----------------|--------------|
| Menüde ETL Studio yok | Plugin yüklenmedi | `plugins/etl_studio_plugin.py` ve `plugins` klasöründe `FFENGINE_STUDIO_*` / `PYTHONPATH` ile `ffengine`’in görünür olduğunu kontrol edin; scheduler/api-server loglarında plugin hatası. |
| 404 veya `/etl-studio` açılmıyor | Yanlış base URL veya mount | Airflow API server’ın plugin FastAPI uygulamasını `/etl-studio` altına bağladığını doğrulayın; `external_views` ile uyumlu sürüm. |
| Keşif 500 | Bağlantı / dialect | Airflow Connection’ın doğru `conn_type` ve erişilebilir endpoint olduğunu doğrulayın; DB firewall. |
| Tablo listesi 400, “typeahead” | `q` çok kısa | `q` için en az 2 karakter veya `q` göndermeyin. |
| Create/update 400 | `ConfigValidator` / şema | Yanıt `detail` mesajına göre `load_method`, `source_type`, partitioning vb. alanları düzeltin. |
| Update 400, ETL Studio imzası | DAG dosyası elle değiştirildi | Üretilen `.py` içinde `# generated_by: etl_studio` satırı korunmalı; yoksa güncelleme reddedilir. |
| Create/update 401 | API anahtarı | `ETL_STUDIO_API_KEY` tanımlıysa `X-ETL-Studio-API-Key` ekleyin. |
| Timeline boş | Henüz DAG çalışmamış veya filtre | `dag_id`/`state` filtrelerini gevşetin; `limit` artırın. |

---

## C08_T14 — Bilinen sınırlamalar

- **UI kapsamı:** MVP tek sayfa; gelişmiş SQL editörü, sürümleme ve çoklu pipeline aynı ekranda yönetimi bu epic kapsamı dışında değerlendirilir.
- **Task modeli:** Mevcut form tek task payload üretir; çoklu task yönetimi (`Add New Task` benzeri dinamik liste) sonraki iterasyona bırakılmıştır.
- **Yetkilendirme:** Airflow’un kimlik doğrulaması geçerlidir; `ETL_STUDIO_API_KEY` yalnızca mutasyon endpoint’leri için ek bir paylaşılan sır katmanıdır, Airflow RBAC yerine geçmez.
- **Keşif:** Tablo listesi sunucu tarafında filtrelenir; çok büyük şemalarda ilk 50 tablo + offset ile sayfalanır (`limit` üst sınırı 50).
- **Platform:** Airflow resmi olarak Windows’ta desteklenmez; üretim ve geliştirme Linux/ Docker önerilir.
- **UI routing:** Airflow 3 SPA ve auth manager sürümüne göre menü URL’leri veya oturum davranışı farklılık gösterebilir; sorun olursa doğrudan `/etl-studio/health` ile servis kontrolü yapın.
