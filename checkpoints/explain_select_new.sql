EXPLAIN ANALYZE
SELECT "SystemEntryDateTime"
FROM oc_epr."EventLogs"
WHERE "SystemEntryDateTime" > TIMESTAMP '2025-11-14 03:00:56.567276'
  AND "SystemEntryDateTime" < DATE '2026-01-01'
  AND "SystemEntryDateTime" >= TIMESTAMP '2025-12-19 18:37:29.816566'
  AND "SystemEntryDateTime" <= TIMESTAMP '2025-12-31 16:09:32.661714';
