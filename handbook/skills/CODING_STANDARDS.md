# Coding Standards

## Dil ve Versiyon
- Python 3.12 — minimum 3.12, 3.10 hedef değil
- Tüm public fonksiyon ve metod imzalarında tip anotasyonu zorunlu
- `from __future__ import annotations` kullanılmaz — 3.12'de gerekli değil

## Dosya Yapısı
Her modül dosyası bu sırayı izler:
1. Modül docstring
2. `__all__` tanımı (public API varsa)
3. Standart kütüphane importları
4. Üçüncü taraf importları
5. İç proje importları
6. Sabitler
7. Sınıflar / fonksiyonlar
```python
# Doğru import sırası
import os
import json
from dataclasses import dataclass

import psycopg
import yaml

from ffengine.core.config import TaskConfig
from ffengine.dialect.base import BaseDialect

BATCH_SIZE_DEFAULT = 10_000   # magic number değil, sabit
```

## Fonksiyon ve Sınıf Kuralları
- Maksimum fonksiyon uzunluğu: **40 satır** (docstring + blank line dahil)
- 40 satırı aşıyorsa özel yardımcı metodlara böl; yardımcı ismi `_` ile başlar
- Her public sınıf ve metod için docstring zorunlu:
  - Tek satırlık metodlar için `"""Ne döndürür."""` yeterli
  - Karmaşık metodlar için `Args:` / `Returns:` / `Raises:` bloğu
```python
# Tek satır yeterli
def is_available(self) -> bool:
    """C Engine native kütüphanesi yüklüyse True döner."""
    ...

# Çok satır gerekli
def run(self, config_path: str, task_group_id: str) -> ETLResult:
    """ETL task'ını çalıştırır.

    Args:
        config_path: YAML config dosyasının yolu.
        task_group_id: Çalıştırılacak task'ın ID'si.

    Returns:
        ETLResult: rows, duration, throughput, errors.

    Raises:
        ConfigError: Config dosyası okunamazsa.
        EngineError: ETL çalışması başarısız olursa.
    """
    ...
```

## Logging — print() Yasak
`print()` kullanılmaz. Her çıktı `logging` veya `ProgressTracker` üzerinden geçer.
```python
# Yasak
print(f"Transferred {rows} rows")

# Doğru
import logging
logger = logging.getLogger(__name__)
logger.info("rows_transferred=%d task=%s", rows, task_group_id)

# Metrik için ProgressTracker
tracker.record(rows=rows, elapsed=elapsed)
```

Log alanları `reference/LOGGING_SCHEMA.md` ile uyumlu olmalı:
`stage`, `task_group_id`, `rows`, `duration_seconds` zorunlu alanlardır.

## Hata Yönetimi
Çıplak `ValueError`, `RuntimeError` veya sürücü exception'ları dışa taşınmaz;
domain exception'a wrap edilir. `reference/EXCEPTION_MODEL.md` sınıf listesi geçerlidir.
```python
# Yasak
raise RuntimeError(f"DB connection failed: {e}")

# Doğru
from ffengine.errors import ConnectionError as FFConnectionError
raise FFConnectionError(f"DB connection failed: {e}") from e
```

## Araç Konfigürasyonu

### ruff
```toml
# pyproject.toml
[tool.ruff]
target-version = "py312"
line-length = 100
select = ["E", "F", "I", "UP", "B", "SIM"]
ignore = ["E501"]  # satır uzunluğu ayrıca kontrol edilir

[tool.ruff.isort]
known-first-party = ["ffengine"]
```

### mypy
```toml
[tool.mypy]
python_version = "3.12"
strict = true
ignore_missing_imports = false
disallow_untyped_defs = true
warn_return_any = true
```

### pytest
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --tb=short"
```

## Mimari Kurallar
- Interface önce gelir, implementasyon sonra — `BaseEngine` ve `BaseDialect` imzaları donmuştur
- Ortak katmanlarda Community/Enterprise özel branch'leri `if is_enterprise:` ile gömülmez;
  polimorfizm ve engine-swap kullanılır
- Scope dışı iş için `raise NotImplementedError("Gelecek faz")` yaz; boş geçme

## Yasaklar Özeti
| Yasak | Doğrusu |
|---|---|
| `print()` | `logger.info()` |
| Çıplak `RuntimeError` fırlat | `FFEngineError` alt sınıfına wrap et |
| Community içinde `COPY` / `BCP` / `OCI_BATCH` | Yalnızca `enterprise/bulk/` altında |
| `if is_enterprise:` ortak kodda | Polimorfizm / engine-swap |
| 40+ satır fonksiyon | Özel metodlara böl |
| Magic number | İsimli sabit veya config parametresi |
| `import *` | Explicit import |