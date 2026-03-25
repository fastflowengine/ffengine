# FFEngine — Agent Execution Handbook

Bu paket, FFEngine için **uygulanabilir agent execution handbook** setidir.

## Paket Bölümleri
- `skills/`: Kod yazma kalıpları ve çalıştırılabilir örüntüler
- `context/`: Epic ve dalga bazlı bağlam dosyaları
- `reference/`: Bağlayıcı teknik sözleşmeler
- `wbs/`: Wave planı, gate testleri, review ve handoff süreçleri
- `.agent/workflows/`: `/komut` ile tetiklenen yürütme protokolleri
- `.antigravity/skills/`: Hızlı yüklenen özet skill girişleri

## Agent Otonomi Dokümanları
- `reference/SESSION_CHECKPOINT.md`: Oturumlar arası ilerleme takibi ve checkpoint şeması
- `reference/AGENT_DECISION_TREE.md`: Agent karar ağacı — dur/devam/eskalasyon kuralları
- `.agent/workflows/error-recovery.md`: Hata kurtarma workflow'u
- `wbs/EPIC_ARTIFACTS.md`: Epic bazlı beklenen çıktı artefaktları
- `reference/BREAKING_CHANGE_POLICY.md`: Kırılıcı değişiklik yönetim politikası
- `reference/MULTI_AGENT_HANDOFF.md`: Multi-agent işbirliği protokolü

## Authority Order
Çelişki halinde aşağıdaki sıra geçerlidir:
1. `GEMINI.md`
2. İlgili `wbs/*.md`
3. İlgili `context/*.md`
4. `reference/*.md`
5. `.agent/workflows/*.md`
6. `skills/*.md`
7. Kod içi eski örnekler / geçmiş çıktılar

## Temel Mimari Özeti
- **Community**: Query-centric Python Engine, `fetchmany + executemany`, standart DBAPI, Airflow-native orchestration
- **Enterprise**: Queue-aware C Engine, ingress/egress queue, native bulk API, adaptive micro-batch, multi-lane runtime
- **Ortak Katmanlar**: ETL Studio UI, dialect layer, TypeMapper, YAML config, DBSession, FFEngineOperator, Auto-DAG Generator, 3 fazlı DAG pattern

## Çalışma Şekli
1. Scope'u belirle: Common / Community / Enterprise.
2. İşi WBS task seviyesinde al.
3. Gerekirse `.agent/workflows/` altındaki uygun workflow'u başlat.
4. Context ile kapsamı netleştir.
5. Reference dosyalarına göre kontratları kilitle.
6. Skills içindeki örüntülerle uygula.
7. Review prompt ile doğrula.
8. Handoff üret.

## Zorunlu Kurallar
- Wave sırasını bozma.
- Community fazında Enterprise capability sızdırma.
- Enterprise fazında ortak katmanları kırma.
- Testsiz teslim yapma.
- Her görevde kapsam sınırı ve delivery semantics kararını açıkla.
