# Multi-Agent Ä°ÅŸbirliÄŸi ProtokolÃ¼

## 1. AmaÃ§
Birden fazla agent'Ä±n aynÄ± proje Ã¼zerinde gÃ¼venli ve tutarlÄ± Ã§alÄ±ÅŸabilmesi iÃ§in handoff ve koordinasyon kurallarÄ±.
Dosya tabanlÄ± iletiÅŸim ile agent'lar arasÄ± gÃ¶rev devri ve Ã§atÄ±ÅŸma Ã§Ã¶zÃ¼mÃ¼ tanÄ±mlanÄ±r.

## 2. Agent Rolleri

| Rol | Sorumluluk | Scope |
|-----|------------|-------|
| **Builder** | Kod Ã¼retimi, dosya oluÅŸturma, test yazÄ±mÄ± | Tek epic, tek wave |
| **Review** | `wbs/REVIEW_PROMPT.md` checklist'ini uygulama, kod inceleme | TamamlanmÄ±ÅŸ task'lar |
| **Test** | `/run-tests` workflow'u Ã§alÄ±ÅŸtÄ±rma, gate kontrolÃ¼ | Epic veya wave bazlÄ± |
| **Orchestrator** | Wave planlama, checkpoint yÃ¶netimi, agent atama | Proje geneli |

## 3. Agent-to-Agent Handoff FormatÄ±

`wbs/HANDOFF_TEMPLATE.md` formatÄ±nÄ± geniÅŸleten versiyon:

```markdown
# AGENT HANDOFF: {EPIC_ID} â€” {TASK_DESCRIPTION}

**Tarih:** {YYYY-MM-DD}
**Wave:** {N}
**Durum:** REVIEW_REQUEST / TEST_REQUEST / FIX_REQUEST / APPROVAL

## Agent Bilgileri
| Alan | DeÄŸer |
|------|-------|
| source_agent | {builder / review / test / orchestrator} |
| target_agent | {builder / review / test / orchestrator} |
| handoff_type | {REVIEW_REQUEST / TEST_REQUEST / FIX_REQUEST / APPROVAL} |
| blocking | {true / false} |
| checkpoint_ref | {checkpoint dosya yolu} |

## Context DosyalarÄ±
Hedef agent'Ä±n okumasÄ± gereken minimum dosya seti:
- ...

## DeÄŸiÅŸen Dosyalar
| Dosya | Ä°ÅŸlem | AÃ§Ä±klama |
|---|---|---|

## Beklenen Aksiyon
- ...

## Kabul Kriterleri
- ...
```

## 4. Handoff AkÄ±ÅŸlarÄ±

### 4.1 Builder â†’ Review
```
Builder: Epic task'Ä±nÄ± tamamlar
  â†’ Checkpoint gÃ¼nceller (task COMPLETED)
  â†’ Handoff Ã¼retir (handoff_type: REVIEW_REQUEST)
  â†’ Review agent devralÄ±r
```

### 4.2 Review â†’ Builder (dÃ¼zeltme gerekiyorsa)
```
Review: Checklist'te sorun tespit eder
  â†’ Handoff Ã¼retir (handoff_type: FIX_REQUEST)
  â†’ Sorunlu maddeleri listeler
  â†’ Builder agent dÃ¼zeltme yapar
```

### 4.3 Review â†’ Test
```
Review: Checklist geÃ§ti
  â†’ Handoff Ã¼retir (handoff_type: TEST_REQUEST)
  â†’ Test agent gate testlerini Ã§alÄ±ÅŸtÄ±rÄ±r
```

### 4.4 Test â†’ Orchestrator
```
Test: Gate testleri geÃ§ti
  â†’ Handoff Ã¼retir (handoff_type: APPROVAL)
  â†’ Orchestrator sonraki wave'i aÃ§ar
```

## 5. Koordinasyon KurallarÄ±

| Kural | AÃ§Ä±klama |
|-------|----------|
| Tek builder | AynÄ± epic Ã¼zerinde aynÄ± anda yalnÄ±zca **1 builder** agent Ã§alÄ±ÅŸÄ±r |
| AyrÄ± session | Review agent, builder ile **aynÄ± session'da** Ã§alÄ±ÅŸmaz |
| Gate otoritesi | Gate kararÄ± yalnÄ±zca **test agent** veya **insan** tarafÄ±ndan verilebilir |
| Checkpoint sahipliÄŸi | Checkpoint dosyasÄ±nÄ± yalnÄ±zca aktif agent gÃ¼nceller |
| Wave kilidi | Wave aÃ§ma/kapama kararÄ± yalnÄ±zca **orchestrator** veya **insan** verir |
| Scope tutarlÄ±lÄ±ÄŸÄ± | TÃ¼m agent'lar aynÄ± `AGENTS.md` authority order'a tabidir |

## 6. Ã‡atÄ±ÅŸma Ã‡Ã¶zÃ¼mÃ¼

| Ã‡atÄ±ÅŸma TÃ¼rÃ¼ | Ã‡Ã¶zÃ¼m |
|-------------|-------|
| AynÄ± dosyayÄ± iki agent deÄŸiÅŸtirmiÅŸ | Son checkpoint geÃ§erlidir; Ã¶nceki agent'Ä±n deÄŸiÅŸiklikleri merge edilir veya override edilir |
| Scope Ã§atÄ±ÅŸmasÄ± | `AGENTS.md` authority order geÃ§erlidir |
| Karar Ã§atÄ±ÅŸmasÄ± (farklÄ± yaklaÅŸÄ±mlar) | Ä°nsan hakem olarak devreye girer |
| Checkpoint kilidi | Aktif agent'Ä±n checkpoint'u geÃ§erlidir; diÄŸer agent bekler |

## 7. Ä°letiÅŸim KanalÄ±

Agent'lar arasÄ± iletiÅŸim **dosya tabanlÄ±dÄ±r**:

```
projects/{project}/handoffs/
  â”œâ”€â”€ {epic_id}_{timestamp}_{handoff_type}.md
  â”œâ”€â”€ C04_20260320_REVIEW_REQUEST.md
  â”œâ”€â”€ C04_20260320_FIX_REQUEST.md
  â””â”€â”€ C04_20260321_APPROVAL.md
```

### Dosya AdlandÄ±rma KuralÄ±
```
{epic_id}_{YYYYMMDD}_{handoff_type}.md
```

### Handoff YaÅŸam DÃ¶ngÃ¼sÃ¼
1. Source agent handoff dosyasÄ±nÄ± oluÅŸturur
2. Target agent handoff dosyasÄ±nÄ± okur ve iÅŸlemi baÅŸlatÄ±r
3. Ä°ÅŸlem tamamlandÄ±ÄŸÄ±nda yeni handoff dosyasÄ± oluÅŸturulur (yanÄ±t olarak)
4. Epic tamamlandÄ±ÄŸÄ±nda tÃ¼m handoff'lar arÅŸivlenir

## 8. Tek Agent Senaryosu

Tek agent Ã§alÄ±ÅŸÄ±yorsa (en yaygÄ±n senaryo):
- Agent tÃ¼m rolleri sÄ±rayla Ã¼stlenir: Builder â†’ Review â†’ Test
- Handoff dosyasÄ± oluÅŸturma **opsiyoneldir** ama checkpoint gÃ¼ncellemesi **zorunludur**
- `wbs/REVIEW_PROMPT.md` checklist'i yine uygulanÄ±r (self-review)
- Gate testi yine Ã§alÄ±ÅŸtÄ±rÄ±lÄ±r

## Referanslar
- `wbs/HANDOFF_TEMPLATE.md` â€” Temel handoff formatÄ±
- `wbs/REVIEW_PROMPT.md` â€” Review checklist'i
- `reference/SESSION_CHECKPOINT.md` â€” Checkpoint yÃ¶netimi
- `AGENTS.md` â€” Authority order

