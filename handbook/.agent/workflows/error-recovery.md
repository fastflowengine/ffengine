# /error-recovery

Hata durumlarında agent'ın izleyeceği kurtarma workflow'u.

## Girdi

| Alan | Tip | Açıklama |
|------|-----|----------|
| `epic_id` | string | Aktif epic kodu (ör. C04) |
| `hata_turu` | enum | `test_failure` \| `lint_failure` \| `import_error` \| `runtime_error` \| `gate_failure` |
| `hata_mesaji` | string | Hata çıktısının özeti |
| `etkilenen_dosyalar` | list | Hatadan etkilenen dosya yolları |

## Protokol

### Adım 1 — Hata Sınıflandırması

Hatayı `reference/EXCEPTION_MODEL.md` hiyerarşisine göre sınıflandır:

| Hata Türü | Exception Sınıfı | Ciddiyet |
|-----------|-------------------|----------|
| test_failure | EngineError / ValidationError | ORTA |
| lint_failure | — (araç hatası) | DÜŞÜK |
| import_error | ConfigError / DialectError | YÜKSEK |
| runtime_error | EngineError / ConnectionError | YÜKSEK |
| gate_failure | — (süreç hatası) | KRİTİK |

### Adım 2 — Etki Analizi

```
1. Hatadan etkilenen dosya sayısını belirle
2. Bu dosyalara bağımlı olan diğer dosyaları tespit et
3. Etkilenen test sayısını belirle
4. Etki kapsamını raporla: DÜŞÜK (1-2 dosya) / ORTA (3-5 dosya) / YÜKSEK (5+ dosya)
```

### Adım 3 — Checkpoint Güncelleme

```yaml
status: FAILED
blocked_reason: "{hata_turu}: {hata_mesaji}"
last_updated_at: "{güncel timestamp}"
```

Checkpoint dosyası: `projects/{project}/checkpoints/{epic_id}_checkpoint.yaml`

### Adım 4 — Kurtarma Stratejisi Seçimi

---

#### Strateji A: Otomatik Düzeltme

**Koşul:** `test_failure` veya `lint_failure` VE etkilenen dosya sayısı < 3

**Adımlar:**
1. Hata mesajını analiz et
2. Kök nedeni belirle
3. Düzeltmeyi uygula
4. Testi tekrar çalıştır (`/run-tests`)
5. **Başarılı** → Checkpoint'u IN_PROGRESS'e geri al, devam et
6. **Başarısız** → Deneme sayacını artır

**Kural:** Maksimum **2 deneme**. 2 denemeden sonra başarısız → Strateji B'ye geç.

---

#### Strateji B: Parçalı Rollback

**Koşul:** Birden fazla dosya etkilenmiş VEYA `runtime_error` VEYA Strateji A tükendi

**Adımlar:**
1. Son çalışan checkpoint'taki `completed_tasks` listesini oku
2. Yalnızca hata üreten task'ın dosyalarını geri al:
   ```bash
   git stash push -m "error-recovery-{epic_id}" -- {etkilenen_dosyalar}
   ```
3. Son geçen test durumuna dön
4. Checkpoint'u güncelle:
   - Hatalı task'ı `pending_tasks`'a geri taşı
   - `status: IN_PROGRESS`
   - `next_action: "{task} yeniden implemente edilecek"`
5. Kullanıcıya bilgi ver: ne geri alındı, ne korundu

---

#### Strateji C: Tam Rollback + Eskalasyon

**Koşul:** `gate_failure` VEYA mimari/interface hatası VEYA etki kapsamı YÜKSEK

**Adımlar:**
1. Tüm değişiklikleri geri al:
   ```bash
   git stash push -m "full-rollback-{epic_id}" -- {tüm_değişen_dosyalar}
   ```
2. Checkpoint'u güncelle:
   - `status: BLOCKED`
   - `blocked_reason: "Tam rollback uygulandı: {sebep}"`
3. Kullanıcıya eskalasyon raporu sun (aşağıdaki formatta)

---

## Strateji Seçim Matrisi

| Hata Türü | Etkilenen Dosya | Deneme | Strateji |
|-----------|-----------------|--------|----------|
| lint_failure | 1-2 | 1. deneme | A |
| test_failure | 1-2 | 1. deneme | A |
| test_failure | 1-2 | 2. deneme sonrası | B |
| test_failure | 3+ | — | B |
| runtime_error | herhangi | — | B |
| import_error | herhangi | — | B |
| gate_failure | herhangi | — | C |
| herhangi | 5+ | — | C |

## Çıktı Formatı

```markdown
# ERROR RECOVERY REPORT

## Epic
{epic_id} — {epic_name}

## Hata Sınıfı
{hata_turu} → {exception_sinifi} — Ciddiyet: {ciddiyet}

## Etki Analizi
- Etkilenen dosyalar: {liste}
- Etkilenen testler: {liste}
- Bağımlı modüller: {liste}
- Etki kapsamı: DÜŞÜK / ORTA / YÜKSEK

## Uygulanan Strateji
Strateji {A/B/C}: {açıklama}

## Yapılan İşlemler
- {adım 1}
- {adım 2}
- ...

## Sonuç
ÇÖZÜLDÜ / ROLLBACK UYGULANDTI / ESKALASYoN GEREKLİ

## Checkpoint Durumu
- Status: {IN_PROGRESS / BLOCKED / FAILED}
- Dosya: {checkpoint_yolu}

## Sonraki Adım
{ne yapılması gerektiği}
```

## Referanslar
- `reference/EXCEPTION_MODEL.md` — Hata sınıflandırması
- `reference/SESSION_CHECKPOINT.md` — Checkpoint güncelleme
- `.agent/workflows/run-tests.md` — Test tekrar çalıştırma
- `wbs/HANDOFF_TEMPLATE.md` — Eskalasyon raporu formatı
