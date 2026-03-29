# Debugpy UAT Playbook (Docker)

Bu dokuman ETL Studio + FFEngine runtime icin asamali breakpoint UAT akisini tanimlar.
Amac: UI -> service -> scheduler katmanlarinda sorunu deterministik izole etmek.

## 1) Local Debug Override Hazirligi

1. Ornek dosyayi local calisma dosyasina kopyalayin:

```powershell
Copy-Item docker\docker-compose.local.debug.yml.example docker\docker-compose.local.debug.yml
```

2. Local hook guard'lari kurun:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\dev\install_local_debug_guards.ps1 -Force
```

3. Debug imajini olusturun ve stack'i debug override ile kaldirin:

```powershell
docker compose -f docker/docker-compose.yml -f docker/docker-compose.local.debug.yml --env-file .env build
docker compose -f docker/docker-compose.yml -f docker/docker-compose.local.debug.yml --env-file .env up -d --remove-orphans
```

## 2) Attach Portlari

- Webserver debugpy: `localhost:5679`
- Scheduler debugpy: `localhost:5678`

`DEBUGPY_WAIT_FOR_CLIENT=1` yaparsaniz proses debugger baglanmadan devam etmez.

## 3) Asamali UAT Akisi

1. UI preload asamasi (webserver):
   - Breakpoint: `api_dag_config`, `resolve_dag_config_for_update`
   - Beklenen:
     - `?dag_id=<new>` -> `supported_for_update=true`
     - `?dag_id=<legacy>` -> `supported_for_update=false`

2. Service reverse-resolve (webserver):
   - Breakpoint: DAG path bulma, `CONFIG_PATH` parse, YAML normalize.
   - Beklenen: `project/domain/level/flow/group_no` dogru.

3. Frontend guard (browser + webserver):
   - JS breakpoint: preload handler + guard render.
   - Beklenen: legacy DAG icin update butonu kilitli.

4. Operator runtime (scheduler):
   - Breakpoint: `FFEngineOperator.execute` plan/prepare/run gecisleri.
   - Beklenen: mapping/DDL hatasi faz bazli net izole.

## 4) DAG Smoke Komutlari

```powershell
docker exec core-airflow-webserver airflow dags test ffengine_dummy_heartbeat 2026-03-29
docker exec core-airflow-webserver airflow dags test ffengine_connection_test 2026-03-29
docker exec core-airflow-webserver airflow dags test ffengine_config_group_12_public_ff_test_data_to_dbo_ff_test_data_psql_v12 2026-03-29
```

## 5) Kapanis

```powershell
docker compose -f docker/docker-compose.yml -f docker/docker-compose.local.debug.yml --env-file .env down
Remove-Item docker\docker-compose.local.debug.yml -Force
git status --short
```

`git status` ciktisinda debug kaynakli staged/modified satir kalmamali.
