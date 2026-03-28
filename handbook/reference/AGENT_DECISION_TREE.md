# Agent Karar AÄŸacÄ±

## 1. AmaÃ§
Agent'Ä±n epic/task Ã§alÄ±ÅŸma sÃ¼recinde otomatik karar alabilmesi iÃ§in baÄŸlayÄ±cÄ± karar kurallarÄ±.
Bu dokÃ¼man, agent'Ä±n ne zaman devam edeceÄŸini, ne zaman duracaÄŸÄ±nÄ± ve ne zaman eskalasyon yapacaÄŸÄ±nÄ± belirler.

## 2. Karar Matrisi

| # | Durum | KoÅŸul | Aksiyon | Referans |
|---|-------|-------|---------|----------|
| K01 | Epic baÅŸlangÄ±cÄ± | Checkpoint dosyasÄ± yok | `/start-epic` workflow'unu Ã§alÄ±ÅŸtÄ±r | `.agent/workflows/start-epic.md` |
| K02 | Epic baÅŸlangÄ±cÄ± | Checkpoint var, status: IN_PROGRESS | KaldÄ±ÄŸÄ± yerden devam et | `reference/SESSION_CHECKPOINT.md` |
| K03 | Epic baÅŸlangÄ±cÄ± | Wave baÄŸÄ±mlÄ±lÄ±ÄŸÄ± tamamlanmamÄ±ÅŸ | **DURDUR** â€” Ã¶nceki wave'in gate testini kontrol et | `wbs/WBS_COMMUNITY.md` |
| K04 | Kod Ã¼retimi | Test yazÄ±lmamÄ±ÅŸ | Ã–nce test yaz (test-first) | `AGENTS.md` |
| K05 | Kod Ã¼retimi | Fonksiyon > 40 satÄ±r | Fonksiyonu bÃ¶l | `skills/CODING_STANDARDS.md` |
| K06 | Kod Ã¼retimi | Community/Enterprise scope karÄ±ÅŸmÄ±ÅŸ | **DURDUR** â€” scope ihlalini raporla | `AGENTS.md` |
| K07 | Test Ã§alÄ±ÅŸtÄ±rma | TÃ¼m testler geÃ§ti | Checkpoint gÃ¼ncelle, sonraki task'a geÃ§ | `reference/SESSION_CHECKPOINT.md` |
| K08 | Test Ã§alÄ±ÅŸtÄ±rma | Test baÅŸarÄ±sÄ±z (< 3 dosya etkilenmiÅŸ) | Otomatik dÃ¼zeltme dene (max 2 deneme) | `.agent/workflows/error-recovery.md` |
| K09 | Test Ã§alÄ±ÅŸtÄ±rma | Test baÅŸarÄ±sÄ±z (2 deneme tÃ¼kendi) | **DURDUR** â€” eskalasyon yap | `reference/SESSION_CHECKPOINT.md` |
| K10 | Gate kontrolÃ¼ | Gate testi geÃ§medi | Sonraki wave'e **GEÃ‡ME** | `wbs/WBS_COMMUNITY.md` |
| K11 | DÄ±ÅŸ baÄŸÄ±mlÄ±lÄ±k | Paket/driver eksik | **DURDUR** â€” DEPENDENCY_POLICY.md kontrol et | `reference/DEPENDENCY_POLICY.md` |
| K12 | Config Ã¼retimi | Zorunlu alan eksik | Validasyon hatasÄ± Ã¼ret | `reference/CONFIG_SCHEMA.md` |
| K13 | API imzasÄ± | Mevcut imza ile uyumsuz | **DURDUR** â€” kÄ±rÄ±lÄ±cÄ± deÄŸiÅŸiklik protokolÃ¼nÃ¼ uygula | `reference/BREAKING_CHANGE_POLICY.md` |
| K14 | Belirsizlik | Scope/mimari kararÄ± gerekli | **DURDUR** â€” kullanÄ±cÄ±ya sor | â€” |
| K15 | Epic tamamlama | TÃ¼m task'lar ve gate'ler geÃ§ti | Handoff Ã¼ret, checkpoint kapat | `wbs/HANDOFF_TEMPLATE.md` |

## 3. Task Dekompozisyon KurallarÄ±

### 3.1 Epic â†’ Alt Task DÃ¶nÃ¼ÅŸÃ¼mÃ¼

```
1. Ä°lgili context dosyasÄ±nÄ± oku: context/{EPIC_ID}_*.md
2. "Dahil" listesindeki her Ã¶ÄŸe = bir alt task
3. WBS dosyasÄ±ndaki "Dosyalar" listesi = dosya bazlÄ± alt task sÄ±nÄ±rlarÄ±
4. WBS dosyasÄ±ndaki "Test" listesi = test task sÄ±nÄ±rlarÄ±
```

### 3.2 Alt Task SÄ±ralama KuralÄ±

Her alt task grubu aÅŸaÄŸÄ±daki sÄ±rada uygulanÄ±r:

```
1. Interface / ABC tanÄ±mÄ± (varsa)
2. Implementasyon kodu
3. Unit test
4. Integration test (epic bazÄ±nda, tÃ¼m alt task'lar tamamlandÄ±ktan sonra)
```

### 3.3 Ã–rnek: C04 Dekompozisyon

| Task ID  | AÃ§Ä±klama                      | Dosya                                   | BaÄŸÄ±mlÄ±lÄ±k |
|----------|-------------------------------|-----------------------------------------|------------|
| C04_T01  | SourceReader implementasyonu  | `pipeline/source_reader.py`             | C02, C03   |
| C04_T02  | SourceReader unit test        | `tests/unit/test_source_reader.py`      | C04_T01    |
| C04_T03  | Streamer implementasyonu      | `pipeline/streamer.py`                  | C04_T01    |
| C04_T04  | Streamer unit test            | `tests/unit/test_streamer.py`           | C04_T03    |
| C04_T05  | TargetWriter implementasyonu  | `pipeline/target_writer.py`             | C02, C03   |
| C04_T06  | TargetWriter unit test        | `tests/unit/test_target_writer.py`      | C04_T05    |
| C04_T07  | Transformer implementasyonu   | `pipeline/transformer.py`               | â€”          |
| C04_T08  | ETLManager implementasyonu    | `core/etl_manager.py`                   | C04_T01..T07 |
| C04_T09  | ETLManager unit test          | `tests/unit/test_etl_manager.py`        | C04_T08    |
| C04_T10  | PGâ†’PG integration test        | `tests/integration/test_pg_to_pg.py`    | C04_T09    |
| C04_T11  | Gate kontrolÃ¼                 | chunk rollback + RAM sÄ±nÄ±rÄ± + is_available | C04_T10 |

## 4. Eskalasyon ProtokolÃ¼

| Seviye | KoÅŸul | Aksiyon |
|--------|-------|---------|
| 0 â€” Bilgi eksik | Teknik detay belirsiz | `context/*.md` ve `reference/*.md` dosyalarÄ±nÄ± oku |
| 1 â€” Deneme tÃ¼kendi | 2 otomatik dÃ¼zeltme denemesi baÅŸarÄ±sÄ±z | Checkpoint'u gÃ¼ncelle (BLOCKED), kullanÄ±cÄ±ya hata Ã¶zeti sun |
| 2 â€” Scope kararÄ± | Community/Enterprise sÄ±nÄ±rÄ± belirsiz | **DURDUR** â€” karar seÃ§eneklerini listele, kullanÄ±cÄ±ya sor |
| 3 â€” Mimari etki | Mevcut interface'de deÄŸiÅŸiklik gerekli | **DURDUR** â€” etki analizi Ã¼ret, kÄ±rÄ±lÄ±cÄ± deÄŸiÅŸiklik protokolÃ¼nÃ¼ referans ver |

## 5. Dur / Devam Sinyal Tablosu

| Sinyal | Anlam | Agent DavranÄ±ÅŸÄ± |
|--------|-------|-----------------|
| YESIL | TÃ¼m kontroller geÃ§ti | Devam et, sonraki task'a geÃ§ |
| SARI | Belirsizlik veya uyarÄ± mevcut | KullanÄ±cÄ±ya bilgi ver, cevap bekle |
| KIRMIZI | Kural ihlali veya kritik hata | Hemen dur, checkpoint gÃ¼ncelle, eskalasyon yap |

### KÄ±rmÄ±zÄ± Sinyal Ã–rnekleri (Hemen Dur)
- Community kodunda Enterprise import'u tespit edildi
- Wave baÄŸÄ±mlÄ±lÄ±ÄŸÄ± henÃ¼z tamamlanmamÄ±ÅŸ
- API imzasÄ± `reference/API_CONTRACTS.md` ile uyumsuz
- Gate testi baÅŸarÄ±sÄ±z ve 2 deneme tÃ¼kendi
- Scope dÄ±ÅŸÄ± teknoloji kullanÄ±mÄ±

### SarÄ± Sinyal Ã–rnekleri (Sor)
- Birden fazla geÃ§erli implementasyon yaklaÅŸÄ±mÄ± mevcut
- Performance trade-off kararÄ± gerekli
- Config'de opsiyonel alan davranÄ±ÅŸÄ± belirsiz
- Test coverage eÅŸiÄŸi sÄ±nÄ±rda

### YeÅŸil Sinyal Ã–rnekleri (Devam Et)
- Test geÃ§ti, lint temiz, imza uyumlu
- Scope net, baÄŸÄ±mlÄ±lÄ±klar hazÄ±r
- Checkpoint gÃ¼ncel, sonraki task tanÄ±mlÄ±

