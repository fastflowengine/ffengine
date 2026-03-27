# /run-tests

Wave gate testlerini standardize eden workflow.

## Girdi
- `wave_or_epic`
- `changed_files`
- `scope`

## Protokol
1. İlgili WBS dosyasından gate kriterlerini oku.
2. `reference/TEST_MATRIX.md` içinden ilgili unit ve integration test setini seç.
3. Yeni dosyalar için eksik test stub varsa önce onları yaz.
4. Aşağıdaki sırayla test çalıştır:
   - lint / import smoke
   - ilgili unit testler
   - ilgili integration testler
   - wave gate testleri
5. Başarısızlık varsa root cause + düzeltme önerisi üret.
6. Sonuçları handoff formatına ekle.

## Standart Sonuç Formatı
```markdown
# TEST REPORT

## Scope
- ...

## Executed
- ...

## Passed
- ...

## Failed
- ...

## Gate Decision
- PASS / FAIL

## Follow-up
- ...
```

## Özel Kural
- Wave gate testleri geçmeden bir sonraki epic veya wave başlatılamaz.
