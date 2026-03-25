# Delivery Guarantee Matrix

FFEngine Enterprise tarafında delivery semantics **koşulludur**. Varsayılan güvence `AT_LEAST_ONCE`'dır. `EXACTLY_ONCE` yalnızca belirli kombinasyonlarda seçilebilir.

| # | Çalışma Kombinasyonu | Aktif Bileşenler | Semantik | Agent Notu |
|---|---|---|---|---|
| 1 | Queue-backed + idempotent sink + ordering key | Ingress/Egress Queue + CheckpointStore + hedef PK/UNIQUE + `ordering_key` | `EXACTLY_ONCE` (koşullu) | Lane içi sıra korunur; duplicate oluşmaz |
| 2 | Queue-backed + idempotent sink, ordering key yok | Queue + Checkpoint + hedef PK/UNIQUE | `AT_LEAST_ONCE + NO_DUPLICATE` | Duplicate yazılmaz; sıra garanti edilmez |
| 3 | Native queue (Oracle AQ / Service Broker) + idempotent sink | Native queue semantiği + FFEngine ack/nack + hedef idempotency | `EXACTLY_ONCE` (koşullu) | Native queue delivery desteklese de hedef idempotency yine zorunlu |
| 4 | Native queue + hedef idempotency yok | Native queue + ack/nack | `AT_LEAST_ONCE` | Retry durumunda duplicate oluşabilir |
| 5 | Cross-DB bulk writer | Egress Queue + native bulk API + checkpoint | `AT_LEAST_ONCE` | COPY / BCP / OCI tek başına idempotent değildir |
| 6 | Cross-DB bulk writer + `load_method: upsert` | Bulk API + checkpoint + hedef UPSERT mantığı | `EFFECTIVELY_EXACTLY_ONCE` | Teknik olarak at-least-once; pratikte duplicate güncellemeye döner |
| 7 | Cross-DB bulk writer + `load_method: truncate+insert` | Bulk API + tam yenileme | `EXACTLY_ONCE_RELOAD` | Her çalışmada tablo yeniden kurulur; partial failure riski ayrıca yönetilmeli |
| 8 | Checkpoint yok | Queue veya bulk path ama checkpoint kapalı | `BEST_EFFORT / AT_LEAST_ONCE` | Resume ve duplicate kontrolü zayıflar |

## Community Kuralı
Community tarafında:
- `DLQ = yok`
- `CheckpointStore = yok`
- `Ack/Nack = yok`
- Semantik: `BEST_EFFORT`, hata halinde chunk rollback

## Agent Karar Kuralları
1. `upsert` seçiliyorsa hedef PK/UNIQUE yoksa warning yaz.
2. `truncate+insert` seçiliyorsa staging veya swap-table önerisi ekle.
3. `ordering_key` yalnızca sıra kritik akışlarda zorunlu tutulmalı.
4. Guarantee seçimini config veya kod yorumunda açıkça belirt.
