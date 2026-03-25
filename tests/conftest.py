import pytest
import os

@pytest.fixture(scope="session")
def postgres_credentials():
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", 5432),
        "user": os.getenv("PG_USER", "airflow"),
        "password": os.getenv("PG_PASSWORD", "airflow_password"),
        "database": os.getenv("PG_DATABASE", "airflow")
    }
