# /start-epic

Bir epic için güvenli başlangıç workflow'u.

## Girdi
- `epic_id`
- `hedef_scope`
- `istenen_dosya_veya_modül` (opsiyonel)

## Protokol
1. `epic_id` için ilgili WBS satırını bul.
2. Bağımlılıkları doğrula.
3. İlgili `context/*.md` dosyasını oku.
4. Gerekli `reference/*.md` dosyalarını listele.
5. Oluşturulacak / değiştirilecek dosyaları çıkar.
6. Test stub listesini oluştur.
7. Çalışma planını kullanıcıya veya review notuna yaz.

## Zorunlu Kontroller
- Scope doğru mu?
- Ön koşul wave'ler tamamlandı mı?
- Community içine Enterprise feature sızıyor mu?
- Engine interface bozuluyor mu?

## Çıktı Formatı
```markdown
# EPIC START PLAN: {EPIC_ID}

## Scope
- Common / Community / Enterprise

## Dependencies
- ...

## Files To Create
- ...

## Files To Modify
- ...

## Required References
- ...

## Required Tests
- unit:
- integration:

## Gate To Exit
- ...
```
