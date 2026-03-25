# Load Methods

| load_method | Davranış | Kullanım |
|---|---|---|
| create_if_not_exists_or_truncate | Tablo yoksa CREATE, varsa TRUNCATE + INSERT | Varsayılan staging |
| append | Mevcut tabloya INSERT | Incremental |
| replace | DROP + CREATE + INSERT | Full refresh |
| upsert | PK bazlı INSERT/UPDATE | Delta, idempotency |
| delete_from_table | WHERE ile sil + INSERT | Kısmi yeniden yükleme |
| drop_if_exists_and_create | DROP IF EXISTS + CREATE + INSERT | Baştan üretim |
| script | Hedef DB'de SQL script çalıştır | Operasyonel script |
