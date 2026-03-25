# Dependency Policy

## Community
- Python >= 3.12
- apache-airflow >= 3.1.6
- psycopg[binary] >= 3.1
- pyodbc >= 5.0
- oracledb >= 2.0
- pyyaml >= 6.0
- pydantic veya dataclass tabanlı config validation

## Dev
- pytest
- pytest-cov
- ruff
- mypy

## Enterprise
- C/C++ toolchain
- CMake
- opsiyonel Valgrind / ASAN
- Native DB client libraries

## Yasaklar
- Community kodunda C-extension varsayımı yapma
- Scope dışı broker bağımlılıklarını ekleme
- Yeni bağımlılık eklerken gerekçe ve test etkisini açıklamadan ekleme
