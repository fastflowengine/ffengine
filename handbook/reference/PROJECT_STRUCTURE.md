# Project Structure

```text
FFEngineCommunity/                          # Repo kökü
├── .env                                    # Yerel geliştirme şifreleri (Git'e atılmaz)
├── .gitignore
├── README.md
├── pyproject.toml
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
├── dags/                                   # Airflow DAG dosyaları
├── logs/                                   # Airflow log çıktıları (Git'e atılmaz)
├── docs/
├── docker/
│   ├── docker-compose.yml                  # Sadece Core Airflow sunucuları
│   └── docker-compose.test.yml             # Dev/Test veritabanları (MSSQL, Oracle vb.)
├── checkpoints/                            # Epic tamamlanma kayıtları
│   └── C0X_checkpoint.yaml
├── projects/
│   └── {project}/{domain}/{level}/...
└── handbook/                                # Agent yürütme kılavuzu (kaynak koddan ayrı)
    ├── GEMINI.md
    ├── AGENTS.md
    ├── skills/
    ├── context/
    ├── reference/
    └── wbs/
```

## Kural
- Agent doküman klasörleri (`skills`, `context`, `reference`, `wbs`) `handbook/` altında tutulur; repo köküne sızmamalıdır.
- Test veritabanı ayrıştırma kuralı: Testler için `docker-compose.test.yml` + `.env` kullanılır, test DB'leri ana Compose'a veya GitHub'a sızamaz.
- `projects/` altındaki YAML, SQL ve mapping çıktıları runtime artefact kabul edilir.
- Enterprise modülleri Community modüllerini override etmez; yalnızca engine detect/fallback zincirine eklenir.
