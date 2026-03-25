# CONTEXT: E04 — DLQ Policy, Retry ve Multi-Lane

## Amaç
Enterprise delivery policy katmanını üret.

## Dahil
- `DLQPolicy`
- `RetryPolicy`
- `DeliveryPolicy`
- `MultiLanePipeline`

## Retry İlkeleri
- exponential backoff
- `max_retries`
- poison message ayırımı
- retry sonrası ack/nack kararı

## DLQ İlkeleri
- maksimum deneme sonrası DLQ'ya taşı
- envelope metadata korunmalı
- root cause / error class kaydı tutulmalı

## Multi-Lane
- bağımsız lane'ler
- `ordering_key` lane içi sıra için kullanılır
- lane'ler arası global sıra garanti edilmez

## Guarantee Kuralı
- Exactly-once ancak Guarantee Matrix koşulları ile seçilebilir.
- Koşullar yoksa varsayılan `AT_LEAST_ONCE`.

## Test Kapsamı
- DLQ yönlendirme
- retry backoff
- ordering key lane testi
- delivery fallback testi
