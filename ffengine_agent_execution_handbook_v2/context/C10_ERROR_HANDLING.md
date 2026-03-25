# CONTEXT: C10 — Hata Yönetimi & Delivery Guarantee

## Amaç
Community ve Enterprise hata modelini scope'a uygun şekilde uygula.

## Kural
- Community: chunk rollback + task retry
- Enterprise: ack/nack, checkpoint, DLQ, delivery policy
