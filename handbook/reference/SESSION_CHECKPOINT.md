# Session Checkpoint Modeli

## 1. Amaç
Agent oturumları arası ilerleme takibi ve devam edebilirlik için checkpoint şema ve kuralları.
Yeni bir session açıldığında agent, önceki session'ın kaldığı yerden devam edebilmelidir.

## 2. Checkpoint Dosya Formatı

```yaml
session_id: "sess_20260320_c04_001"
epic_id: C04
wave: 4
scope: Community                    # Community | Enterprise
started_at: "2026-03-20T09:00:00Z"
last_updated_at: "2026-03-20T11:30:00Z"
status: IN_PROGRESS                 # IN_PROGRESS | BLOCKED | COMPLETED | FAILED

completed_tasks:
  - task_id: "C04_T01"
    description: "SourceReader implementasyonu"
    files_created:
      - src/ffengine/pipeline/source_reader.py
    files_modified: []
    tests_passed:
      - tests/unit/test_source_reader.py
    completed_at: "2026-03-20T10:15:00Z"

pending_tasks:
  - task_id: "C04_T02"
    description: "Streamer implementasyonu"
    priority: 1

created_files:
  - src/ffengine/pipeline/source_reader.py

modified_files: []

test_results:
  passed:
    - tests/unit/test_source_reader.py
  failed: []

open_risks:
  - "Oracle dialect server-side cursor desteği doğrulanmadı"

blocked_reason: null                # null veya engel açıklaması
next_action: "Streamer sınıfını implemente et ve test_streamer.py yaz"
```

## 3. Checkpoint Yaşam Döngüsü

```
OLUŞTUR ─→ GÜNCELLE ─→ KAPAT
   ↑            │          │
   │            ↓          ↓
   │         BLOCKED    HANDOFF
   │            │
   └── DEVAM ──┘
```

| Aşama    | Tetikleyici                          | Aksiyon                                          |
|----------|--------------------------------------|--------------------------------------------------|
| OLUŞTUR  | `/start-epic` workflow'u çalıştığında | Boş checkpoint dosyası oluştur, status: IN_PROGRESS |
| GÜNCELLE | Her alt task tamamlandığında          | completed_tasks'a ekle, pending_tasks'tan çıkar  |
| BLOCKED  | Devam edilemez engel oluştuğunda     | status: BLOCKED, blocked_reason doldur           |
| DEVAM    | Yeni session checkpoint'u okuduğunda | Status'e göre kaldığı yerden devam et            |
| KAPAT    | Tüm task'lar tamamlandığında         | status: COMPLETED, handoff üret                  |
| FAILED   | Kurtarılamaz hata oluştuğunda       | status: FAILED, error-recovery workflow'una yönlen |

## 4. Depolama Konumu

```
projects/{project}/checkpoints/{epic_id}_checkpoint.yaml
```

Örnek: `projects/sample_project/checkpoints/C04_checkpoint.yaml`

## 5. Yeni Session Başlangıç Protokolü

```
1. Checkpoint dosyası var mı?
   ├─ EVET → Oku
   │   ├─ status: IN_PROGRESS → pending_tasks listesinden devam et
   │   ├─ status: BLOCKED → blocked_reason'ı kullanıcıya göster, çözüm sor
   │   ├─ status: FAILED → error-recovery workflow'unu başlat
   │   └─ status: COMPLETED → "Bu epic zaten tamamlandı" bildir
   └─ HAYIR → /start-epic workflow'u ile yeni checkpoint oluştur
```

## 6. Güncelleme Kuralları

- Her alt task tamamlandığında checkpoint **hemen** güncellenir (batch güncelleme yapılmaz).
- `last_updated_at` her güncellemede yenilenir.
- `created_files` ve `modified_files` dosya bazında takip edilir.
- `test_results` her test çalıştırmasında üzerine yazılır (kümülatif değil, son durum).
- `next_action` her zaman bir sonraki adımı açıkça belirtir.

## 7. Checkpoint ile Handoff İlişkisi

Checkpoint **COMPLETED** olduğunda, `wbs/HANDOFF_TEMPLATE.md` formatında bir handoff belgesi üretilir.
Handoff belgesi checkpoint'taki verileri kullanır:

| Checkpoint Alanı   | Handoff Karşılığı         |
|---------------------|---------------------------|
| `created_files`     | Değişen Dosyalar tablosu  |
| `completed_tasks`   | Tamamlanan Kabul Kriterleri|
| `open_risks`        | Açık Riskler              |
| `next_action`       | Sonraki Wave İçin Notlar  |
