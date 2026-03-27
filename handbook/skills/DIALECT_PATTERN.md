# Dialect Pattern

## Her Dialect İçin Zorunlu Metodlar
- `connect`
- `create_cursor`
- `get_table_schema`
- `generate_ddl`
- `generate_bulk_insert_query`
- `get_data_type_map`
- `get_pagination_query`
- `quote_identifier`
- `list_schemas`
- `list_tables`

## Opsiyonel / Dialect-Specific
- `generate_bulk_extract_query` → yalnızca PostgresDialect Enterprise bulk extract path için

## Postgres
- Psycopg3
- Server-side cursor için named cursor
- Identifier quoting: `"name"`
- `get_pagination_query()` tipik olarak `LIMIT {limit} OFFSET {offset}`

## MSSQL
- pyodbc
- Placeholder: `?`
- Identifier quoting: `[name]`
- `get_pagination_query()` için `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY`

## Oracle
- python-oracledb
- Placeholder: `:1`, `:2`
- Identifier quoting: `"NAME"`
- `get_pagination_query()` için `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY` veya rownum türevi yaklaşım

## Agent Kontrol Listesi
1. Her dialect `BaseDialect` kontratını eksiksiz implement ediyor mu?
2. Pagination query unutulmuş mu?
3. Bulk insert query Community ve Enterprise farkına göre doğru mu?
4. Schema discovery nullable / precision / scale bilgisini döndürüyor mu?
