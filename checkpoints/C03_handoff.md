# HANDOFF: C03 — Dialect Layer

**Tarih:** 2026-03-26
**Wave:** 3
**Durum:** COMPLETE
**Source Agent:** builder
**Target Agent:** builder (C04 / C05)
**Checkpoint Ref:** checkpoints/C03_checkpoint.yaml

## Değişen Dosyalar

| Dosya | İşlem | Açıklama |
|---|---|---|
| `src/ffengine/dialects/base.py` | CREATE | BaseDialect ABC + ColumnInfo dataclass |
| `src/ffengine/dialects/postgres.py` | CREATE | PostgresDialect — psycopg3, named server-side cursor, `"identifier"` quoting |
| `src/ffengine/dialects/mssql.py` | CREATE | MSSQLDialect — pyodbc, `[identifier]` quoting, OFFSET/FETCH pagination |
| `src/ffengine/dialects/oracle.py` | CREATE | OracleDialect — oracledb, `"IDENTIFIER"` quoting, DUAL health_check, thick_mode |
| `src/ffengine/dialects/type_mapper.py` | CREATE | TypeMapper — lossless cross-dialect tip dönüşümü, UnsupportedTypeError |
| `src/ffengine/dialects/__init__.py` | MODIFY | Tüm public sınıflar `__all__`'a eklendi |
| `tests/unit/test_dialect_postgres.py` | CREATE | 15 test — connect, cursor, schema discovery, DDL, quoting, health_check |
| `tests/unit/test_dialect_mssql.py` | CREATE/MODIFY | 18 test — @patch hedefi `pyodbc.connect` olarak düzeltildi |
| `tests/unit/test_dialect_oracle.py` | CREATE/MODIFY | 17 test — @patch hedefi `oracledb.connect` + `oracledb.init_oracle_client` olarak düzeltildi |
| `tests/unit/test_type_mapper.py` | CREATE | 25 test — critical mappings, precision, round-trip, case insensitivity, error handling |
| `checkpoints/C03_checkpoint.yaml` | CREATE | Epic checkpoint |

## Tamamlanan Kabul Kriterleri

- TypeMapper lossless: `NUMBER(38,10)` → `NUMERIC(38,10)`, precision/scale tüm yollarda korunuyor
- DDL deterministik: aynı girdi her zaman aynı çıktıyı üretiyor (3 dialect test edildi)
- Metadata discovery: nullable, precision, scale doğru döndürülüyor
- Quoting kuralları: Postgres `"x"`, MSSQL `[x]`, Oracle `"X"` (uppercase)
- BaseDialect kontratı: 10 abstract metod, tüm concrete sınıflar eksiksiz implemente ediyor
- Oracle `health_check` DUAL override
- 88/88 test PASSED (C03: 75, C02 regresyon: 13)

## Açık Riskler

- **API imza sapması (düşük risk):** `generate_ddl(table_name, columns)` — `API_CONTRACTS.md`'de `(schema, table, columns)` formu tanımlı. Mevcut implementasyon `table_name`'i `"schema.table"` şeklinde qualified string alıyor. C04 TargetWriter yazılırken uyum gözden geçirilmeli.
- **`list_tables` eksik parametreler (düşük risk):** `API_CONTRACTS.md`'de `search=None, limit=50` tanımlı, implementasyonda yok. C08 ETL Studio gerektirdiğinde eklenebilir.
- **`UnsupportedTypeError` exception hiyerarşisi:** Şu an `Exception`'dan türüyor. C10'da `DialectError(FFEngineError)` kurulduğunda `UnsupportedTypeError(DialectError)` olarak refactor edilmeli.

## Sonraki Wave İçin Notlar

- **C04 (Core Engine):** `TargetWriter` ve `SourceReader` dialect objelerini doğrudan kullanacak. `BaseDialect` import path: `from ffengine.dialects import PostgresDialect, MSSQLDialect, OracleDialect`
- **C05 (YAML Config):** Dialect tip bilgisi config'de `dialect: postgres|mssql|oracle` olarak gelecek. `TypeMapper.map_type(type, src_dialect, tgt_dialect)` çağrısı için dialect adı lowercase string bekleniyor.
- **C09 (Mapping Tools):** `TypeMapper` doğrudan kullanılacak — `from ffengine.dialects import TypeMapper`
- Tüm driver'lar (`psycopg`, `pyodbc`, `oracledb`) lazy import ile yükleniyor; connection olmadan dialect nesnesi oluşturmak güvenli.
