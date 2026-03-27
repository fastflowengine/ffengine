# C08 Handoff — ETL Studio (Airflow 3 Native)

## Status
🟩 **DONE** — Tüm görevler (C08_T01–C08_T14) tamamlandı — 2026-03-27

## What Changed

- ETL Studio plugin kaydı Airflow 3 native modele taşındı: `fastapi_apps` + `external_views` (`src/ffengine/ui/plugin.py`).
- Legacy FAB birincil yol olarak kullanılmıyor; API FastAPI uygulamasında.
- **Faz 2:** `DagUpsertPayload` ile genişletilmiş pipeline alanları (T05); create/update öncesi `ConfigValidator` (T06); `FFENGINE_STUDIO_*` kökleri altında path kontrolü (T08); `.etl_studio.json` ile `auto_tags` / `user_tags` (T09); timeline `dag_id` / `state` filtreleri (T10); `ETL_STUDIO_API_KEY` + `X-ETL-Studio-API-Key` ile mutasyon koruması (T12).
- **C08_T13:** `tests/unit/test_etl_studio_api.py` — FastAPI `TestClient` ile kapsamlı endpoint testleri.
- **C08_T14:** `handbook/context/C08_ETL_STUDIO.md` — kullanım, sorun giderme tablosu, bilinen sınırlamalar.
- **UI polish (Airflow 3 uyum):** `index.html` ETL Configuration Studio düzenine taşındı (Global Settings + Task #1 + sekmeler), Airflow static CSS adaylarını dinamik yükleyip fallback stil ile çalışacak şekilde güncellendi.

### Endpoint özeti
- `/etl-studio/health` — `dag_marker` alanı
- `/etl-studio/api/schemas`, `/api/tables`, `/api/columns`
- `POST /api/create-dag`, `POST /api/update-dag`
- `GET /api/timeline?limit=&dag_id=&state=`

## Files

- `src/ffengine/ui/plugin.py`
- `src/ffengine/ui/api_app.py`
- `src/ffengine/ui/studio_service.py`
- `src/ffengine/ui/templates/etl_studio/index.html`
- `plugins/etl_studio_plugin.py`
- `tests/unit/test_etl_studio_api.py`
- `docker/docker-compose.yml`
- `pyproject.toml` (dev: `httpx`, `fastapi`)
- `README.md`
- `handbook/context/C08_ETL_STUDIO.md`
- `handbook/skills/UI_PLUGIN_PATTERN.md`
- `checkpoints/C08_checkpoint.yaml`

## Notes

- `docker-compose.yml` plugin klasörü: `/opt/airflow/plugins`; `FFENGINE_STUDIO_PROJECTS_ROOT` / `FFENGINE_STUDIO_DAG_ROOT` ile studio çıktıları yönlendirilir.
- Yerel test: `pip install -e ".[dev]"` sonra `PYTHONPATH=src pytest tests/unit/test_etl_studio_api.py`.
- Güncel test seti: `14 passed` (UI index + API + DAG/YAML üretim doğrulamaları).

## Next Steps

- Wave 5: **C10** Error Handling veya roadmap’teki sonraki epic.
- Production’da `ETL_STUDIO_API_KEY` rotasyonu ve Airflow auth ile birlikte davranışın doğrulanması.
