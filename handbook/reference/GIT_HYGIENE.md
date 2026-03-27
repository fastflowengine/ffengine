# Git Hygiene

## Amac

Agent'larin local, generated ve binary artefaktlari yanlislikla Git reposuna
eklememesini saglamak.

## Zorunlu Kurallar

- Push veya teslim oncesi `git status` kontrol edilir.
- Gerekirse `git ls-files` ile track edilen dosyalar gozden gecirilir.
- Asagidaki dosyalar varsayilan olarak repoya alinmaz:
  - sanal ortam klasorleri (`venv/`, `venv2/` ve benzerleri)
  - `.env` ve `.env.*`
  - cache ve `__pycache__` klasorleri
  - `*.pyc`
  - `*.egg-info/`
  - `*.log`
  - kok dizindeki `*.pdf`
  - local zip ve archive paketleri
- Bu tip dosyalar `.gitignore` icinde tanimli olmalidir.

## Track Edilen Gereksiz Dosyalar

Bir dosya `.gitignore` kapsaminda olsa bile daha once track edildiyse:

```bash
git rm --cached <path>
```

Bu islem dosyayi diskten silmez, sadece Git index'inden cikarir.

## Review Checklist

- Push oncesi local/runtime artefaktlar kontrol edildi mi?
- `.gitignore` gerekli desenleri kapsiyor mu?
- Yanlislikla track edilen binary veya local dosyalar index'ten cikarildi mi?
