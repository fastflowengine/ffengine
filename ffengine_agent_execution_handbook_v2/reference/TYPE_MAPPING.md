# Type Mapping

## İlke
TypeMapper kayıpsız ve deterministik olmalıdır. Özellikle finansal NUMERIC/DECIMAL alanlarda precision kaybı kabul edilmez.

## Kritik Eşleşmeler
- Oracle `NUMBER(38,10)` → PostgreSQL `NUMERIC(38,10)`
- MSSQL `DECIMAL(p,s)` → PostgreSQL `NUMERIC(p,s)`
- Oracle `CLOB` → PostgreSQL `TEXT`
- Oracle `BLOB` → PostgreSQL `BYTEA`
- MSSQL `DATETIME2` → PostgreSQL `TIMESTAMP`

## Kural
- Precision/scale bilgisi korunur.
- Hedef DB'de doğrudan karşılık yoksa kontrollü fallback tanımlanır ve testlenir.
