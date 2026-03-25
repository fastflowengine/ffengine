# Logging Schema

FFEngine structured logging JSON formatı kullanır.

## Zorunlu Alanlar
- `timestamp`
- `level`
- `logger`
- `stage` (`config|extract|transform|load|delivery|airflow`)
- `task_group_id`
- `source_db`
- `target_db`
- `rows`
- `duration_seconds`
- `message`

## Opsiyonel Alanlar
- `partition_id`
- `batch_no`
- `throughput`
- `error_type`
- `error_message`
- `delivery_semantics`

## Kural
PII veya payload içeriği log'a yazılmaz. SQL metni yalnızca debug modunda ve maskeleme ile yazılabilir.
