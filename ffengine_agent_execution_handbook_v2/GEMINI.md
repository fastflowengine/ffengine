# FFEngine — Antigravity Workspace Rules

> Bu dosya Antigravity workspace giriş kural setidir.
> Agent her session başında önce bu dosyayı, sonra ilgili `wbs/`, `context/`, `reference/`, `.agent/workflows/` ve `skills/` dosyalarını okumalıdır.

## 1. Amaç
Bu paket, FFEngine projesinin **agent execution handbook** sürümüdür. Amaç; AI agent'ların FFEngine'i wave-bazlı, sürüm sınırlarına sadık, test-first ve engine-swap uyumlu şekilde geliştirmesini sağlamaktır.

## 2. Öncelik Sırası
1. `GEMINI.md`
2. İlgili wave için `wbs/WBS_COMMUNITY.md` veya `wbs/WBS_ENTERPRISE.md`
3. İlgili epic için `context/*.md`
4. Bağlayıcı sözleşmeler için `reference/*.md`
5. Uygun çalışma protokolü için `.agent/workflows/*.md`
6. Uygulama kalıpları için `skills/*.md`

## 3. Mutlak Kurallar
- Önce scope belirle: **Common / Community / Enterprise**.
- Wave sırasını bozma. Ön koşulu tamamlanmamış task için kod üretme.
- Community için yalnızca **Python Engine** geliştir.
- Enterprise için yalnızca **engine-swap ile C Engine** katmanını geliştir; UI, dialect, YAML, Airflow ve DB bağlantı katmanlarını kırma.
- Community için delivery modeli **best-effort / chunk rollback**; gelişmiş checkpoint-resume ve DLQ Enterprise'a aittir.
- Enterprise için exactly-once yalnızca **Delivery Guarantee Matrix** koşulları sağlandığında seçilebilir.
- Her değişiklikte test yaz. Gate testleri geçmeden bir sonraki wave'e geçme.
- Her teslimde review ve handoff üret.

## 4. Workflow Zorunluluğu
Aşağıdaki durumlarda workflow kullanımı zorunludur:
- Yeni epic başlatırken: `.agent/workflows/start-epic.md`
- Config üretirken: `.agent/workflows/generate-config.md`
- Mapping üretirken: `.agent/workflows/generate-mapping.md`
- Test / gate çalıştırırken: `.agent/workflows/run-tests.md`
- Hata kurtarma gerektiğinde: `.agent/workflows/error-recovery.md`

## 5. Antigravity Notu
Antigravity'de workflow'lar markdown dosyaları olarak tutulur ve Agent içinden komutla tetiklenebilir. Skill'ler ise `SKILL.md` tabanlı yeniden kullanılabilir görev klasörleridir. Bu repository o çalışma modeline göre organize edilmiştir.

## 6. Agent Otonomi Kuralları
- Her session başında checkpoint kontrolü zorunludur: `reference/SESSION_CHECKPOINT.md`
- Karar belirsizliğinde agent karar ağacını uygular: `reference/AGENT_DECISION_TREE.md`
- Hata durumunda error-recovery workflow'u tetiklenir: `.agent/workflows/error-recovery.md`
- Epic tamamlama kriterleri artefakt listesinden doğrulanır: `wbs/EPIC_ARTIFACTS.md`
- Dondurulmuş arayüz değişikliği onay gerektirir: `reference/BREAKING_CHANGE_POLICY.md`
- Çoklu agent çalışmasında koordinasyon protokolü uygulanır: `reference/MULTI_AGENT_HANDOFF.md`

## 7. Başlangıç Kararı
- Mevcut aktif geliştirme başlangıç noktası: **Community GA wave'leri**
- Enterprise wave'leri yalnızca Community GA kabul kriterleri sağlandıktan sonra açılır.
