# CONTEXT: E03 — Native Bulk API ve Worker Pool

## Amaç
Enterprise bulk path'ini ve N Reader / M Writer worker modelini tanımla.

## Dahil
- PostgreSQL: `COPY TO STDOUT` / `COPY FROM STDIN`
- MSSQL: `BCP` veya `fast_executemany` Enterprise bulk yolu
- Oracle: `OCI_BATCH_ERRORS`
- `worker_pool.py`
- `reader_workers`, `writer_workers`, `pipe_queue_max`

## Temel Kural
Community içinde native bulk API çağrısı yapılmaz. Enterprise path aynı config şeması üzerinden açılır.

## Worker Mimarisi
- Reader ve Writer sayıları bağımsızdır.
- Varsayılan:
  - `reader_workers = 3`
  - `writer_workers = 5`
  - `pipe_queue_max = 8`
- Asimetrik yapı darboğaza göre ayarlanır.

## Delivery Notu
- Cross-DB bulk writer tek başına exactly-once vermez.
- `load_method=upsert` ile effectively exactly-once benzeri sonuç elde edilebilir.
- `truncate+insert` tam yenileme senaryosudur; partial failure riski ayrıca not edilir.

## Çıktı
- bulk adapter sözleşmesi
- worker pool pattern
- gate throughput hedefleri
