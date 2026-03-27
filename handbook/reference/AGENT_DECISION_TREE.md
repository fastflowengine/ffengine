# Agent Karar Ağacı

## 1. Amaç
Agent'ın epic/task çalışma sürecinde otomatik karar alabilmesi için bağlayıcı karar kuralları.
Bu doküman, agent'ın ne zaman devam edeceğini, ne zaman duracağını ve ne zaman eskalasyon yapacağını belirler.

## 2. Karar Matrisi

| # | Durum | Koşul | Aksiyon | Referans |
|---|-------|-------|---------|----------|
| K01 | Epic başlangıcı | Checkpoint dosyası yok | `/start-epic` workflow'unu çalıştır | `.agent/workflows/start-epic.md` |
| K02 | Epic başlangıcı | Checkpoint var, status: IN_PROGRESS | Kaldığı yerden devam et | `reference/SESSION_CHECKPOINT.md` |
| K03 | Epic başlangıcı | Wave bağımlılığı tamamlanmamış | **DURDUR** — önceki wave'in gate testini kontrol et | `wbs/WBS_COMMUNITY.md` |
| K04 | Kod üretimi | Test yazılmamış | Önce test yaz (test-first) | `GEMINI.md §3` |
| K05 | Kod üretimi | Fonksiyon > 40 satır | Fonksiyonu böl | `skills/CODING_STANDARDS.md` |
| K06 | Kod üretimi | Community/Enterprise scope karışmış | **DURDUR** — scope ihlalini raporla | `GEMINI.md §3` |
| K07 | Test çalıştırma | Tüm testler geçti | Checkpoint güncelle, sonraki task'a geç | `reference/SESSION_CHECKPOINT.md` |
| K08 | Test çalıştırma | Test başarısız (< 3 dosya etkilenmiş) | Otomatik düzeltme dene (max 2 deneme) | `.agent/workflows/error-recovery.md` |
| K09 | Test çalıştırma | Test başarısız (2 deneme tükendi) | **DURDUR** — eskalasyon yap | `reference/SESSION_CHECKPOINT.md` |
| K10 | Gate kontrolü | Gate testi geçmedi | Sonraki wave'e **GEÇME** | `wbs/WBS_COMMUNITY.md` |
| K11 | Dış bağımlılık | Paket/driver eksik | **DURDUR** — DEPENDENCY_POLICY.md kontrol et | `reference/DEPENDENCY_POLICY.md` |
| K12 | Config üretimi | Zorunlu alan eksik | Validasyon hatası üret | `reference/CONFIG_SCHEMA.md` |
| K13 | API imzası | Mevcut imza ile uyumsuz | **DURDUR** — kırılıcı değişiklik protokolünü uygula | `reference/BREAKING_CHANGE_POLICY.md` |
| K14 | Belirsizlik | Scope/mimari kararı gerekli | **DURDUR** — kullanıcıya sor | — |
| K15 | Epic tamamlama | Tüm task'lar ve gate'ler geçti | Handoff üret, checkpoint kapat | `wbs/HANDOFF_TEMPLATE.md` |

## 3. Task Dekompozisyon Kuralları

### 3.1 Epic → Alt Task Dönüşümü

```
1. İlgili context dosyasını oku: context/{EPIC_ID}_*.md
2. "Dahil" listesindeki her öğe = bir alt task
3. WBS dosyasındaki "Dosyalar" listesi = dosya bazlı alt task sınırları
4. WBS dosyasındaki "Test" listesi = test task sınırları
```

### 3.2 Alt Task Sıralama Kuralı

Her alt task grubu aşağıdaki sırada uygulanır:

```
1. Interface / ABC tanımı (varsa)
2. Implementasyon kodu
3. Unit test
4. Integration test (epic bazında, tüm alt task'lar tamamlandıktan sonra)
```

### 3.3 Örnek: C04 Dekompozisyon

| Task ID  | Açıklama                      | Dosya                                   | Bağımlılık |
|----------|-------------------------------|-----------------------------------------|------------|
| C04_T01  | SourceReader implementasyonu  | `pipeline/source_reader.py`             | C02, C03   |
| C04_T02  | SourceReader unit test        | `tests/unit/test_source_reader.py`      | C04_T01    |
| C04_T03  | Streamer implementasyonu      | `pipeline/streamer.py`                  | C04_T01    |
| C04_T04  | Streamer unit test            | `tests/unit/test_streamer.py`           | C04_T03    |
| C04_T05  | TargetWriter implementasyonu  | `pipeline/target_writer.py`             | C02, C03   |
| C04_T06  | TargetWriter unit test        | `tests/unit/test_target_writer.py`      | C04_T05    |
| C04_T07  | Transformer implementasyonu   | `pipeline/transformer.py`               | —          |
| C04_T08  | ETLManager implementasyonu    | `core/etl_manager.py`                   | C04_T01..T07 |
| C04_T09  | ETLManager unit test          | `tests/unit/test_etl_manager.py`        | C04_T08    |
| C04_T10  | PG→PG integration test        | `tests/integration/test_pg_to_pg.py`    | C04_T09    |
| C04_T11  | Gate kontrolü                 | chunk rollback + RAM sınırı + is_available | C04_T10 |

## 4. Eskalasyon Protokolü

| Seviye | Koşul | Aksiyon |
|--------|-------|---------|
| 0 — Bilgi eksik | Teknik detay belirsiz | `context/*.md` ve `reference/*.md` dosyalarını oku |
| 1 — Deneme tükendi | 2 otomatik düzeltme denemesi başarısız | Checkpoint'u güncelle (BLOCKED), kullanıcıya hata özeti sun |
| 2 — Scope kararı | Community/Enterprise sınırı belirsiz | **DURDUR** — karar seçeneklerini listele, kullanıcıya sor |
| 3 — Mimari etki | Mevcut interface'de değişiklik gerekli | **DURDUR** — etki analizi üret, kırılıcı değişiklik protokolünü referans ver |

## 5. Dur / Devam Sinyal Tablosu

| Sinyal | Anlam | Agent Davranışı |
|--------|-------|-----------------|
| YESIL | Tüm kontroller geçti | Devam et, sonraki task'a geç |
| SARI | Belirsizlik veya uyarı mevcut | Kullanıcıya bilgi ver, cevap bekle |
| KIRMIZI | Kural ihlali veya kritik hata | Hemen dur, checkpoint güncelle, eskalasyon yap |

### Kırmızı Sinyal Örnekleri (Hemen Dur)
- Community kodunda Enterprise import'u tespit edildi
- Wave bağımlılığı henüz tamamlanmamış
- API imzası `reference/API_CONTRACTS.md` ile uyumsuz
- Gate testi başarısız ve 2 deneme tükendi
- Scope dışı teknoloji kullanımı

### Sarı Sinyal Örnekleri (Sor)
- Birden fazla geçerli implementasyon yaklaşımı mevcut
- Performance trade-off kararı gerekli
- Config'de opsiyonel alan davranışı belirsiz
- Test coverage eşiği sınırda

### Yeşil Sinyal Örnekleri (Devam Et)
- Test geçti, lint temiz, imza uyumlu
- Scope net, bağımlılıklar hazır
- Checkpoint güncel, sonraki task tanımlı
