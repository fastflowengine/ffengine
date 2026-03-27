# C11 Handoff — Integration Test + Release Prep (Community)

## Status
🟩 **COMPLETED** — C11_T01..C11_T06 tüm task'lar tamamlandı. Wave 6 kapanışı yapıldı.

## Scope

- Container tabanlı gerçek DB integration testleri
- Cross-DB akışları: PG->PG, PG->MSSQL, PG->Oracle
- Mapping generator -> config -> DAG -> run -> verify zinciri
- Community release prep dokümantasyonu

## Planned Tasks

- C11_T01..C11_T06 (`checkpoints/C11_checkpoint.yaml`)

## Progress

- ✅ **C11_T01** tamamlandı:
  - test containerlar doğrulandı: `test-postgres`, `test-mssql`, `test-oracle` = Up
  - port erişimleri doğrulandı: 5435 / 1433 / 1521 = Open
  - worktree kökünde `.env` oluşturuldu (compose + integration test değişkenleri hizalı)
  - `docker compose -f docker/docker-compose.test.yml --env-file .env config` doğrulaması PASS
- ✅ **C11_T02** tamamlandı:
  - `tests/integration/test_cross_db_etl.py` koşullu aktivasyon stratejisi finalize edildi.
  - `FFENGINE_ENABLE_CROSS_DB_TESTS=1` olmadan kontrollü skip; aktivasyon açıkken preflight/bağlantı problemlerinde reason ile skip.
  - `integration` marker için pytest config çakışması temizlendi.
- ✅ **C11_T03** tamamlandı:
  - MSSQL sürücü parametresi env destekli yapıldı (`MSSQL_TEST_DRIVER`).
  - `PG->PG` akışında aynı connection kaynaklı cursor hatası, self-transfer için ayrı target session ile giderildi.
  - Seçili zorunlu akış koşumu PASS: `test_pg_to_pg`, `test_pg_to_mssql`, `test_pg_to_oracle`.
- ✅ **C11_T04** tamamlandı:
  - `tests/integration/test_mapping_chain.py` ile uçtan uca mapping zinciri entegrasyon testi eklendi.
  - Zincir doğrulandı: mapping üretimi -> config yazımı -> DAG üretimi -> operator execute -> hedef doğrulama.
  - Seçili Wave 6 koşumu PASS: zorunlu 3 cross-DB akışı + mapping chain testi.
- ✅ **C11_T05** tamamlandı:
  - `docs/community_quickstart.md` oluşturuldu.
  - `README.md` içine Wave 6 quickstart/test komutları eklendi.
  - `handbook/wbs/WBS_COMMUNITY.md` C11 ilerleme durumu güncellendi.
- ✅ **C11_T06** tamamlandı:
  - C11 epic durumu `COMPLETED` olarak işaretlendi.
  - `README.md` Wave Plan tablosu `✅ Done` olarak güncellendi.
  - `handbook/wbs/WBS_COMMUNITY.md` C11 durumu `✅ COMPLETED`, tüm task'lar `DONE`.
  - Wave 6 completion checklist tüm maddeleri `DONE`.

## Wave 6 Closure Path

1. ✅ Integration ortamı doğrulandı ve testler stabilize edildi.
2. ✅ Zorunlu akışlar PASS alındı (PG->PG / PG->MSSQL / PG->Oracle).
3. ✅ Mapping zinciri ve release prep dokümanları tamamlandı.
4. ✅ C11 checkpoint/handoff kapanışı ve WBS/README durum güncellemesi yapıldı.

## Notes

- `tests/integration/test_cross_db_etl.py` aktivasyon/skip stratejisi C11_T02 kapsamında netleştirildi.
- `test_community_e2e.py` artefaktı bu repoda yok; gerekli ise C11 kapsamında eklenir.
