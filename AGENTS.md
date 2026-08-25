# FFEngine Agent Master Guide

Bu dosya tüm coding agent'lar için kök çalışma sözleşmesidir. Her görevin başlangıcında okunur. Ayrıntılı uygulama `handbook/EXECUTION_HANDBOOK.md` ve ilgili epic/test dosyasındadır.

## 1. Ürün ve hedef

FFEngine; Airflow-native, YAML/config-driven, Flow Studio UI'lı kurumsal veri aktarım platformudur. Hedef, bankaya satılabilir on-prem/air-gapped ETL'dir. Paket modeli:

- `ffengine`: Community, PyPI
- `ffengine_enterprise`: private wheel
- Bağımlılık yalnız `ffengine_enterprise -> ffengine`; tersi yasak.

Konumlandırma **dar ve derin**: RDBMS/DWH/lakehouse derinliği, kaynak koruma, fail-loud, credential tutmama, push-down ve denetim izi.

### FF Governance — ayrı ürün

`ffgovernance` (`FFGovernance/`) **FFEngine'in bir parçası değildir** (EX-D039.1): kendi paketi, kendi sürümü, kendi tag'i vardır. Airflow-native governance katmanıdır (Explorer · Lineage · Runs · Comparisons · Integrations) ve şema drift karşılaştırmasını `ffgovernance.compare` ile birlikte teslim eder.

Bağımlılık **iki yönde de yoktur** ve ölçülmüştür (2026-08-25):

- FFGov → FFEngine: modül düzeyinde `import ffengine` **yok**; tespit runtime'dadır (GOV-INV-5), `pyproject` extra'sı bilerek boştur
- Community/Enterprise → FFGov: `pyproject`'te `ffgovernance` **geçmez**; tek dokunuş `ui/dag_explorer_compat.py`'deki fail-safe runtime tespitidir (her hata → "aktif değil" → yerleşik Explorer servis edilir)
- OM da opsiyoneldir: seçili değilse veya erişilemezse karşılaştırma canlı DB'den okur

Bu depoda çalışacak agent **önce `FFGovernance/AGENTS.md`'yi okur**; oradaki GOV-INV kuralları, geçit sayısı ve UI kabuğu tuzakları bağlayıcıdır. Epic kaydı: `handbook/epics/epic-F7.0.md` … `epic-F7.6.md`.

## 2. Kaynak otorite

1. `SOURCE_LOCK.md`
2. `handbook/DECISIONS.md`, `handbook/CONTRACTS.md`, `handbook/TERMINOLOGY.md`
3. İlgili `handbook/epics/epic-<FID>.md` ve `handbook/tests/test-<FID>.md`
4. Gerçek repository source code

Çelişki varsa: güncel insan onaylı TAD/FAD kararı > AAD > bu handbook > agent varsayımı. Kaynaksız mimari karar üretme. FAD v26.1/v26.2 referans uyuşmazlığı `SOURCE_LOCK.md` içinde kayıtlıdır; işlevsel çelişki görülürse dur.

## 3. Değişmez kurallar

- **Fail-loud:** desteklenmeyen veya belirsiz durumda sessiz fallback yok.
- **Frozen contracts:** W2-W5 imzaları yalnız additive genişletilir; breaking değişiklikte dur.
- **No core bloat:** dar/nadir iş çekirdeğe eklenmez; hedef DB, dbt veya ayrı engine'e gider.
- **Push-down:** enrichment/transform/data-control hedef DB'de; `transformer.py` passthrough kalır.
- **No credentials:** secret yalnız Airflow Connection/Secrets Backend'de; config/log/UI'da yok.
- **Airflow-native:** retry, rerun, notification, params, freshness ve auth yeniden inşa edilmez.
- **Developer control:** engine/bulk/M/load_method açık seçimdir; sessiz otomatik degrade yok.
- **Edition gating:** Enterprise özellik UI + backend iki katmanla korunur; Community tek başına yeşil.
- **Code-grounded:** önce gerçek kodu grep/view et; line number ve eski hafızaya güvenme.
- **Terminoloji:** `handbook/TERMINOLOGY.md` bağlayıcıdır.

Tam kurallar `.agents/rules/` altındadır.

## 4. Engine ve paralellik

- `StandardEngine`: Community, senkron memory-streaming
- `PipelineEngine`: Enterprise, P x (1 reader / M writer), bounded queue
- `SparkEngine`: Enterprise, kendi executor modeli; PipelineEngine knob'larının yerine geçer

Okuma paralelliği = Airflow partition P + DBMS sunucu içi `parallel_degree`. Motor-içi N reader yok. Kaynak bağlantı = P; hedef bağlantı = P x M. `parallel_degree` yeni bağlantı açmaz.

## 5. Görev alma protokolü

1. `/session-start`
2. `state/WORK_QUEUE.md` içinden READY epic seç.
3. `/take-epic <FID>` ile epic'i claim et; owner/branch/worktree kaydet.
4. `/plan-epic <FID>`; plan artifact'i kullanıcı incelemesine hazırla.
5. Onay gerektiren risk yoksa `/implement-epic <FID>`.
6. `/verify-epic <FID>`; kanıtları `state/evidence/<FID>/` altında markdown olarak kaydet.
7. Farklı agent `/review-epic <FID>` çalıştırır.
8. Merge sonrası `state/WORK_QUEUE.md`, `WORK_LOG.md`, `HANDOFF.md` güncellenir.

Agent aynı anda yalnız bir epic'in owner'ı olabilir. Aynı dosyada iki agent paralel çalışamaz. Paralel çalışma için ayrı branch/worktree ve dosya sahipliği gerekir.

## 6. TDD ve gate

Önce test, sonra minimum implementasyon, sonra refactor. Gate sırası:

1. repository/CI preflight
2. ilgili unit testler
3. `flake8 src tests`
4. tam unit suite
5. gerekli integration testler
6. Community-only regression
7. contract/edition/security review
8. evidence report

Fonksiyon hedefi <=40 satır. Testsiz yeni artefakt tamamlanmış sayılmaz. Sabit RAM ve cross-dialect gereksinimleri ilgili epic'te uygulanır.

## 7. Değişiklik sınırları

Agent açık insan onayı olmadan şunları yapmaz:

- dependency/base image/version yükseltme
- public API/frozen signature değiştirme
- migration veya kalıcı schema değişikliği
- lisans/edition politikasını değiştirme
- dosya silme, history rewrite, force-push
- production deploy/production DB erişimi
- F4+ tasarım-niyetini implement etme

## 8. Stop/escalation koşulları

Aşağıdakilerde kod üretimini durdur ve `state/BLOCKERS.md` kaydı aç:

- FAD/TAD/AAD ile kod arasında çözülmemiş mimari çelişki
- W2-W5 breaking değişiklik ihtiyacı
- kapsamın F4+ veya açıkça spec dışı olması
- test ortamı production'a işaret ediyor olması
- gerekli secret/credential'ın config'e yazılması talebi
- destructive işlem, veri kaybı veya package/deploy riski
- acceptance criterion'ın ölçülemez veya çelişkili olması

## 9. Tamamlanma çıktısı

Her epic sonunda şu dosyalar güncel olmalıdır:

- kod + testler
- `state/WORK_LOG.md`
- `state/evidence/<FID>/EVIDENCE.md`
- gerekiyorsa `state/DECISION_LOG.md` / `BLOCKERS.md`
- `state/HANDOFF.md`
- ilgili handbook/epic/test dokümanı, davranış değiştiyse

Final mesaj tek başına kanıt değildir; completion, repository state ve test evidence ile belirlenir.
