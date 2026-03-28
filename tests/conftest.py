import pytest
import os
import shutil
import uuid
from pathlib import Path

@pytest.fixture(scope="session")
def postgres_credentials():
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", 5432),
        "user": os.getenv("PG_USER", "airflow"),
        "password": os.getenv("PG_PASSWORD", "airflow_password"),
        "database": os.getenv("PG_DATABASE", "airflow")
    }


@pytest.fixture
def tmp_path():
    """
    Windows ACL kaynakli pytest tmp_path sorununu bypass eder.

    Built-in tmp_path yerine workspace altinda gecici bir dizin olusturur.
    """
    root = Path("logs") / "pytest_tmp"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
