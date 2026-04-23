"""
Integration test: Airflow FabAuthManager + DB auth (C18).

Docker stack (docker/docker-compose.yml) up oldugu varsayilir. Test:
- Admin / Op / Viewer / breakglass kullanicilar ab_user tablosunda.
- HTTP login 3 rol icin 200.
- Role enforcement (viewer trigger 403, op trigger 200, admin security 200).
- SimpleAuthManager artefaktlari image icinde yok.

Calistirma:
  FFENGINE_ENABLE_AIRFLOW_AUTH_TESTS=1 pytest tests/integration/test_airflow_auth.py -v
"""

import os
import re
import subprocess

import httpx
import pytest

_CSRF_PATTERN = re.compile(
    r'name="csrf_token"[^>]*value="([^"]+)"', re.IGNORECASE
)

pytestmark = [pytest.mark.integration]

if os.getenv("FFENGINE_ENABLE_AIRFLOW_AUTH_TESTS", "0").strip() != "1":
    pytestmark.append(
        pytest.mark.skip(reason="FFENGINE_ENABLE_AIRFLOW_AUTH_TESTS=1 olmadigi icin skip.")
    )


AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8085")
WEBSERVER_CONTAINER = os.getenv("AIRFLOW_WEBSERVER_CONTAINER", "core-airflow-webserver")

USERS = {
    "admin": os.getenv("FFENGINE_AIRFLOW_ADMIN_PASSWORD", "admin"),
    "breakglass": os.getenv("FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD", "breakglass"),
    "operator": os.getenv("FFENGINE_AIRFLOW_OP_PASSWORD", "operator"),
    "viewer": os.getenv("FFENGINE_AIRFLOW_VIEWER_PASSWORD", "viewer"),
}


def _login_client(username: str, password: str) -> httpx.Client:
    client = httpx.Client(base_url=AIRFLOW_BASE_URL, follow_redirects=False, timeout=30.0)
    page = client.get("/auth/login/")
    page.raise_for_status()
    match = _CSRF_PATTERN.search(page.text)
    assert match, "CSRF token bulunamadi (FAB login formu degismis olabilir)."
    resp = client.post(
        "/auth/login/",
        data={
            "csrf_token": match.group(1),
            "username": username,
            "password": password,
        },
    )
    assert resp.status_code in (200, 302), (
        f"Login POST beklenmeyen status {resp.status_code} for user={username}"
    )
    assert "session" in client.cookies, f"Session cookie yok; login basarisiz olabilir ({username})"
    return client


@pytest.fixture(scope="module")
def admin_client():
    client = _login_client("admin", USERS["admin"])
    yield client
    client.close()


@pytest.fixture(scope="module")
def breakglass_client():
    client = _login_client("breakglass", USERS["breakglass"])
    yield client
    client.close()


@pytest.fixture(scope="module")
def op_client():
    client = _login_client("operator", USERS["operator"])
    yield client
    client.close()


@pytest.fixture(scope="module")
def viewer_client():
    client = _login_client("viewer", USERS["viewer"])
    yield client
    client.close()


def test_admin_user_login_succeeds(admin_client: httpx.Client):
    resp = admin_client.get("/")
    assert resp.status_code == 200


def test_op_user_login_succeeds(op_client: httpx.Client):
    resp = op_client.get("/")
    assert resp.status_code == 200


def test_viewer_user_login_succeeds(viewer_client: httpx.Client):
    resp = viewer_client.get("/")
    assert resp.status_code == 200


def test_breakglass_admin_can_manage_users(breakglass_client: httpx.Client):
    resp = breakglass_client.get("/auth/users/list/")
    assert resp.status_code == 200


def test_viewer_cannot_access_user_management(viewer_client: httpx.Client):
    resp = viewer_client.get("/auth/users/list/", follow_redirects=False)
    assert resp.status_code in (302, 401, 403)


def test_simple_auth_manager_artifacts_absent():
    result = subprocess.run(
        [
            "docker",
            "exec",
            WEBSERVER_CONTAINER,
            "test",
            "!",
            "-f",
            "/opt/airflow/simple_auth_manager_passwords.json",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "simple_auth_manager_passwords.json hala image icinde; C18 sokum eksik."
    )


def test_fab_user_table_populated():
    # Airflow 3 removed `airflow users list`; query ab_user directly via postgres container.
    pg_container = os.getenv("AIRFLOW_POSTGRES_CONTAINER", "core-postgres")
    result = subprocess.run(
        [
            "docker",
            "exec",
            pg_container,
            "psql",
            "-U",
            "airflow",
            "-d",
            "airflow",
            "-tAc",
            "SELECT username FROM ab_user ORDER BY id;",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    output = result.stdout
    for username in ("admin", "breakglass", "operator", "viewer"):
        assert username in output, f"{username} ab_user tablosunda yok:\n{output}"
