# CONTEXT: C01 — Proje İskeleti & Altyapı

## Amaç
FFEngine Python projesinin temel iskeletini, paket yönetimini, docker ortamını ve CI düzenini kur.

## Çıktılar
- `pyproject.toml`
- `src/ffengine/__init__.py`
- `docker/Dockerfile`
- `docker/docker-compose.yml` (Core Airflow sunucuları)
- `docker/docker-compose.test.yml` (Dev/Test DB: Postgres-test, MSSQL, Oracle)
- `dags/`, `logs/` (Docker eşleştirilen operasyon klasörleri)
- `dags/dummy_test_dag.py` (Doğrulama ve canlı bağlantı testi)
- `.env` (Yerel DB şifreleri, GitHub'da yok)
- `.gitignore` ve `.dockerignore` (İmaj/Repo Gizlilik ve Optimizasyon Kuralları)
- `.github/workflows/ci.yml`
- `tests/conftest.py`

**Geliştirici Ortamı Çalışma Portları ve Konteyner İsimleri:**
- Airflow Webserver (`core-airflow-webserver`): `8085`
- Airflow DB (`core-postgres`): `5436`
- Airflow Scheduler (`core-airflow-scheduler`): Arka Plan (Background)
- Airflow DAG Processor (`core-airflow-dag-processor`): Arka Plan (Airflow 3 Yeniliği)
- Test Postgres (`test-postgres`): `5435`
- Test MSSQL (`test-mssql`): `1433`
- Test Oracle (`test-oracle`): `1521`

## Kabul Kriterleri
- `pip install -e .` çalışır
- `import ffengine` hata vermez
- Temel test ve lint pipeline ayağa kalkar
