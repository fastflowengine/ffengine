# Multi-Agent İşbirliği Protokolü

## 1. Amaç
Birden fazla agent'ın aynı proje üzerinde güvenli ve tutarlı çalışabilmesi için handoff ve koordinasyon kuralları.
Dosya tabanlı iletişim ile agent'lar arası görev devri ve çatışma çözümü tanımlanır.

## 2. Agent Rolleri

| Rol | Sorumluluk | Scope |
|-----|------------|-------|
| **Builder** | Kod üretimi, dosya oluşturma, test yazımı | Tek epic, tek wave |
| **Review** | `wbs/REVIEW_PROMPT.md` checklist'ini uygulama, kod inceleme | Tamamlanmış task'lar |
| **Test** | `/run-tests` workflow'u çalıştırma, gate kontrolü | Epic veya wave bazlı |
| **Orchestrator** | Wave planlama, checkpoint yönetimi, agent atama | Proje geneli |

## 3. Agent-to-Agent Handoff Formatı

`wbs/HANDOFF_TEMPLATE.md` formatını genişleten versiyon:

```markdown
# AGENT HANDOFF: {EPIC_ID} — {TASK_DESCRIPTION}

**Tarih:** {YYYY-MM-DD}
**Wave:** {N}
**Durum:** REVIEW_REQUEST / TEST_REQUEST / FIX_REQUEST / APPROVAL

## Agent Bilgileri
| Alan | Değer |
|------|-------|
| source_agent | {builder / review / test / orchestrator} |
| target_agent | {builder / review / test / orchestrator} |
| handoff_type | {REVIEW_REQUEST / TEST_REQUEST / FIX_REQUEST / APPROVAL} |
| blocking | {true / false} |
| checkpoint_ref | {checkpoint dosya yolu} |

## Context Dosyaları
Hedef agent'ın okuması gereken minimum dosya seti:
- ...

## Değişen Dosyalar
| Dosya | İşlem | Açıklama |
|---|---|---|

## Beklenen Aksiyon
- ...

## Kabul Kriterleri
- ...
```

## 4. Handoff Akışları

### 4.1 Builder → Review
```
Builder: Epic task'ını tamamlar
  → Checkpoint günceller (task COMPLETED)
  → Handoff üretir (handoff_type: REVIEW_REQUEST)
  → Review agent devralır
```

### 4.2 Review → Builder (düzeltme gerekiyorsa)
```
Review: Checklist'te sorun tespit eder
  → Handoff üretir (handoff_type: FIX_REQUEST)
  → Sorunlu maddeleri listeler
  → Builder agent düzeltme yapar
```

### 4.3 Review → Test
```
Review: Checklist geçti
  → Handoff üretir (handoff_type: TEST_REQUEST)
  → Test agent gate testlerini çalıştırır
```

### 4.4 Test → Orchestrator
```
Test: Gate testleri geçti
  → Handoff üretir (handoff_type: APPROVAL)
  → Orchestrator sonraki wave'i açar
```

## 5. Koordinasyon Kuralları

| Kural | Açıklama |
|-------|----------|
| Tek builder | Aynı epic üzerinde aynı anda yalnızca **1 builder** agent çalışır |
| Ayrı session | Review agent, builder ile **aynı session'da** çalışmaz |
| Gate otoritesi | Gate kararı yalnızca **test agent** veya **insan** tarafından verilebilir |
| Checkpoint sahipliği | Checkpoint dosyasını yalnızca aktif agent günceller |
| Wave kilidi | Wave açma/kapama kararı yalnızca **orchestrator** veya **insan** verir |
| Scope tutarlılığı | Tüm agent'lar aynı `GEMINI.md` authority order'a tabidir |

## 6. Çatışma Çözümü

| Çatışma Türü | Çözüm |
|-------------|-------|
| Aynı dosyayı iki agent değiştirmiş | Son checkpoint geçerlidir; önceki agent'ın değişiklikleri merge edilir veya override edilir |
| Scope çatışması | `GEMINI.md` authority order geçerlidir |
| Karar çatışması (farklı yaklaşımlar) | İnsan hakem olarak devreye girer |
| Checkpoint kilidi | Aktif agent'ın checkpoint'u geçerlidir; diğer agent bekler |

## 7. İletişim Kanalı

Agent'lar arası iletişim **dosya tabanlıdır**:

```
projects/{project}/handoffs/
  ├── {epic_id}_{timestamp}_{handoff_type}.md
  ├── C04_20260320_REVIEW_REQUEST.md
  ├── C04_20260320_FIX_REQUEST.md
  └── C04_20260321_APPROVAL.md
```

### Dosya Adlandırma Kuralı
```
{epic_id}_{YYYYMMDD}_{handoff_type}.md
```

### Handoff Yaşam Döngüsü
1. Source agent handoff dosyasını oluşturur
2. Target agent handoff dosyasını okur ve işlemi başlatır
3. İşlem tamamlandığında yeni handoff dosyası oluşturulur (yanıt olarak)
4. Epic tamamlandığında tüm handoff'lar arşivlenir

## 8. Tek Agent Senaryosu

Tek agent çalışıyorsa (en yaygın senaryo):
- Agent tüm rolleri sırayla üstlenir: Builder → Review → Test
- Handoff dosyası oluşturma **opsiyoneldir** ama checkpoint güncellemesi **zorunludur**
- `wbs/REVIEW_PROMPT.md` checklist'i yine uygulanır (self-review)
- Gate testi yine çalıştırılır

## Referanslar
- `wbs/HANDOFF_TEMPLATE.md` — Temel handoff formatı
- `wbs/REVIEW_PROMPT.md` — Review checklist'i
- `reference/SESSION_CHECKPOINT.md` — Checkpoint yönetimi
- `GEMINI.md` — Authority order
