# Project Structure

```text
ffengine/
├── GEMINI.md
├── AGENTS.md
├── .env                  # Yerel geliştirme şifreleri (Git'e atılmaz)
├── .gitignore            # Git dışı bırakılacak dizin/dosyalar
├── src/
│   └── ffengine/
│       ├── core/
│       ├── db/
│       ├── dialects/
│       ├── engine/
│       ├── airflow/
│       ├── config/
│       ├── mapping/
│       ├── partition/
│       ├── logging/
│       ├── errors/
│       ├── tools/
│       └── ui/
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
├── docker/
│   ├── docker-compose.yml       # Sadece Core Airflow sunucuları
│   └── docker-compose.test.yml  # Dev/Test veritabanları (MSSQL, Oracle vb.)
├── projects/
│   └── {project}/{domain}/{level}/...
├── skills/
├── context/
├── reference/
└── wbs/
```

## Kural
- Doküman klasörleri (`skills`, `context`, `reference`, `wbs`) kaynak koddan ayrı tutulur.
- Test veritabanı ayrıştırma kuralı: Testler için `docker-compose.test.yml` + `.env` kullanılır, test DB'leri ana Compose'a veya GitHub'a sızamaz.
- `projects/` altındaki YAML, SQL ve mapping çıktıları runtime artefact kabul edilir.
- Enterprise modülleri Community modüllerini override etmez; yalnızca engine detect/fallback zincirine eklenir.
