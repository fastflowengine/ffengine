# /generate-mapping

Mapping YAML üretim workflow'u.

## Girdi
- kaynak tablo şeması
- hedef tablo şeması
- source_db
- target_db
- kolon eşleme modu

## Protokol
1. `reference/TYPE_MAPPING.md` ve `skills/DIALECT_PATTERN.md` oku.
2. Kaynak metadata'yı kolon bazında çıkar.
3. Hedef tip eşlemesini üret.
4. İsim farklıysa explicit mapping yaz.
5. Precision/scale kaybı varsa uyarı notu ekle.
6. LOB/BLOB/CLOB alanlarını ayrıca işaretle.
7. `column_mapping_mode=source` uygunsa mapping dosyasına gerçekten gerek var mı kontrol et.

## LOB Uyarısı
Aşağıdaki tiplerde otomatik karar notu yaz:
- `BLOB`
- `CLOB`
- `TEXT` / büyük metin
- `BYTEA`
- `RAW`
- `LONG RAW`

## Çıktı Formatı
```yaml
columns:
  - source: COL_A
    target: col_a
    source_type: NUMERIC(16,0)
    target_type: BIGINT
    nullable: false
    rule: null
```
