import pytest
import os
import shutil
import uuid
from pathlib import Path


def _load_dotenv():
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()

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
