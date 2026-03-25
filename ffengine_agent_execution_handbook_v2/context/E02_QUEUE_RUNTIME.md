# CONTEXT: E02 — Queue Runtime

## Amaç
Queue-aware runtime çekirdeğini tanımla.

## Temel Nesneler
- `FFEnvelope`
- `IngressQueue`
- `EgressQueue`
- `CheckpointStore`
- `DeliveryManager`
- `BackpressureController`

## FFEnvelope
Header + payload modelidir.
Header içinde en az:
- `message_id`
- `partition_key`
- `sequence`
- `attempt`
- `created_at`
- `metadata`

Payload:
- veri chunk'ı veya binary blok

## IngressQueue
- Source Producer → Transformer köprüsü
- thread-safe
- `maxsize = pipe_queue_max`
- queue doluluk oranına göre backpressure uygular

## EgressQueue
- Transformer → Target Consumer köprüsü
- delivery policy uygular
- ordered / at-least-once / exactly-once(koşullu) kararını yürütür

## CheckpointStore
- source offset + target ack takibi
- JSON veya SQLite olabilir
- corruption recovery notu zorunlu

## DeliveryManager
- retry sayaçları
- nack işleme
- poison message tespiti
- DLQ yönlendirme tetikleme noktası

## Hariç
- Native queue adapter'ları ana fazda zorunlu değil
- Kafka / RabbitMQ bu fazda kapsam dışı
