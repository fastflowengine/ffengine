# Community Quickstart

Bu kılavuz FFEngine Community'yi yerel ortamda çalıştırmak ve integration gate'lerini
doğrulamak için referans kaynağıdır.

## 1) Gereksinimler

- Python `3.12.x`
- Docker Desktop
- Editable kurulum:

```bash
py -3.12 -m pip install -e ".[dev]"
```

## 2) Test Veritabanlarını Başlat

```bash
docker-compose -p ffengine-test -f docker/docker-compose.test.yml --env-file .env up -d --remove-orphans
```

Container'lar ayağa kalktıktan sonra `.env` değişkenlerini kontrol et:

| Değişken | Varsayılan (docker-compose.test.yml) |
|---|---|
| `POSTGRES_TEST_USER` | `ffengine_test` |
| `POSTGRES_TEST_PASS` | `ffengine_pg_pass` |
| `POSTGRES_TEST_DB` | `ffengine_test_db` |
| `MSSQL_SA_PASS` | `Mssql_password123!` |
| `ORACLE_PASS` | `Oracle_password123!` |

**Port eşlemeleri:** PostgreSQL → `5435`, MSSQL → `1433`, Oracle → `1521`

## 3) Kalıcı Test Tablosu — `ff_test_data`

Integration testleri **kalıcı** bir kaynak tablosu kullanır.
Container başladıktan sonra her DB'de bir kez oluşturulur:

| Kolon | PostgreSQL | MSSQL | Oracle |
|---|---|---|---|
| `id` | `INT` | `INT` | `NUMBER` |
| `name` | `VARCHAR(100)` | `VARCHAR(100)` | `VARCHAR2(100)` |
| `created_date` | `DATE` | `DATE` | `DATE` |

**100 satır** içerir. Satır örneği: `(1, 'Record_001', <dün>)`.

Tabloyu yeniden oluşturmak için:

```bash
# PostgreSQL
docker exec test-postgres psql -U ffengine_test -d ffengine_test_db -c "
DROP TABLE IF EXISTS ff_test_data;
CREATE TABLE ff_test_data (id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL);
INSERT INTO ff_test_data SELECT gs, 'Record_'||LPAD(gs::TEXT,3,'0'), CURRENT_DATE-gs FROM generate_series(1,100) gs;"

# MSSQL
docker exec test-mssql bash -c "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Mssql_password123!' -C -d ffengine_test -Q \"
IF OBJECT_ID('dbo.ff_test_data','U') IS NOT NULL DROP TABLE dbo.ff_test_data;
CREATE TABLE dbo.ff_test_data (id INT NOT NULL, name VARCHAR(100) NOT NULL, created_date DATE NOT NULL);
WITH n AS (SELECT 1 n UNION ALL SELECT n+1 FROM n WHERE n<100)
INSERT INTO dbo.ff_test_data SELECT n,'Record_'+RIGHT('000'+CAST(n AS VARCHAR),3),CAST(DATEADD(day,-n,GETDATE()) AS DATE) FROM n OPTION(MAXRECURSION 100);\""

# Oracle
docker exec test-oracle bash -c "sqlplus -s ffengine/Oracle_password123!@//localhost/FREEPDB1 <<'EOF'
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ff_test_data'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE ff_test_data (id NUMBER NOT NULL, name VARCHAR2(100) NOT NULL, created_date DATE NOT NULL);
BEGIN FOR i IN 1..100 LOOP INSERT INTO ff_test_data VALUES(i,'Record_'||LPAD(i,3,'0'),SYSDATE-i); END LOOP; COMMIT; END;
/
EXIT;
EOF"
```

## 4) Test Aktivasyon Bayrakları

```bash
# PowerShell
$env:PYTHONPATH = "src"

# Sadece PostgreSQL testleri
$env:FFENGINE_ENABLE_PG_TESTS = "1"

# Tüm cross-DB testleri (9 kombinasyon + mapping chain)
$env:FFENGINE_ENABLE_CROSS_DB_TESTS = "1"
```

## 5) Test Komutları

```bash
# Unit testleri (mock, container gerekmez)
py -3.12 -m pytest tests/unit/ -q

# PostgreSQL → PostgreSQL (4 test)
py -3.12 -m pytest tests/integration/test_pg_to_pg.py -v

# 9 cross-DB kombinasyonu
py -3.12 -m pytest tests/integration/test_cross_db_etl.py -v

# Mapping zinciri (MappingGenerator → DAG → Operator → verify)
py -3.12 -m pytest tests/integration/test_mapping_chain.py -v

# Zorunlu gate komutları (Wave 6)
py -3.12 -m pytest tests/integration/test_cross_db_etl.py::test_pg_to_pg tests/integration/test_cross_db_etl.py::test_pg_to_mssql tests/integration/test_cross_db_etl.py::test_pg_to_oracle tests/integration/test_mapping_chain.py -q
```

## 6) Release Kontrol Listesi (Community)

- [x] `ff_test_data` tablosu 3 DB'de mevcut (100 satır, 3 kolon)
- [x] `FFENGINE_ENABLE_PG_TESTS=1` ile `test_pg_to_pg.py` 4/4 PASS
- [x] `FFENGINE_ENABLE_CROSS_DB_TESTS=1` ile `test_cross_db_etl.py` 9/9 PASS
- [x] `FFENGINE_ENABLE_CROSS_DB_TESTS=1` ile `test_mapping_chain.py` 1/1 PASS
- [x] Unit testler 466+ PASS, 0 FAIL
- [x] `README.md` durumu güncel

> Not: Bu baseline tamamlandi. Sonraki gelisimler yeni epic/story/task'ler ile planlanacaktir.


## Airflow Runtime Model (Current)


- Airflow tarafinda tek kanonik yol: `FFEngineOperator`.
- `plan/prepare/run` fazlari operator icinde calisir.
- Performans icin XCom minimal tutulur:
  - `rows_transferred`
  - `duration_seconds`
  - `rows_per_second`
  - `retry_telemetry`
  - `error_summary` (yalnizca hata durumunda)
- Ara fazlara ait buyuk payload'lar XCom'a yazilmaz.

### Migration Note

- `ffengine.airflow.XComKeys` ve `ffengine.airflow.build_task_group` kullanimi kaldirildi.
- Yeni ve mevcut DAG akislarinda `FFEngineOperator` kullanilmalidir.

## Type Conversion Reference

- Type conversion policy and matrix:
  `docs/type-mapping-policy.md`

## Flow Studio Update Mode

- DAG detail deep-link supported: `/flow-studio/?dag_id=<dag_id>`.
- New naming DAG'lerde form preload edilip update mode acilir.
- Ayni flow klasorunde birden fazla group DAG olabilir; bu desteklenir.
- `POST /flow-studio/api/update-dag` cagrisi `dag_id` query parami ile hedef DAG'i zorunlu olarak belirtmelidir.
- `DELETE /flow-studio/api/delete-dag?dag_id=<dag_id>` sadece Flow Studio marker'li DAG'lerde calisir ve DAG bundle'i (dag.py + YAML + auto mapping + history) siler.

## Flow Studio Form Notes

- Schema/table autocomplete case-insensitive calisir.
- MSSQL baglantilarinda schema genelde `dbo` olur.
- `Target Table` alaninda listede olmayan adlar manuel yazilabilir.
- `Airflow Variable` listesi sadece `Filter & Bindings` altindaki binding kaynagi icindir; DB connection listesiyle ayni sey degildir.

## Debug UAT (Opsiyonel)

- Debugpy ile UI -> service -> scheduler asamali UAT icin:
  `docs/debugpy-uat-playbook.md`


