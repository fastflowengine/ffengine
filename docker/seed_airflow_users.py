"""Seed Airflow FAB users for FFEngine public/runtime images.

Airflow 3.x'te `airflow users create` CLI komutu kaldirildi. FAB provider'in
SecurityManager API'sini dogrudan kullanarak idempotent user seed yapar.

Parolalar env var'dan okunur; eksikse islem bilincli olarak fail eder.
Yeniden calistirildiginda mevcut kullanicilari dokunmadan gecer.
"""
from __future__ import annotations

import logging
import os
import sys


logging.basicConfig(level=logging.INFO, format="[seed_users] %(message)s")
log = logging.getLogger(__name__)


USERS = [
    {
        "username": "admin",
        "first_name": "Admin",
        "last_name": "User",
        "email": "admin@ffengine.local",
        "role": "Admin",
        "password_env": "FFENGINE_AIRFLOW_ADMIN_PASSWORD",
    },
    {
        "username": "breakglass",
        "first_name": "Break",
        "last_name": "Glass",
        "email": "breakglass@ffengine.local",
        "role": "Admin",
        "password_env": "FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD",
    },
    {
        "username": "operator",
        "first_name": "Ops",
        "last_name": "User",
        "email": "operator@ffengine.local",
        "role": "Op",
        "password_env": "FFENGINE_AIRFLOW_OP_PASSWORD",
    },
    {
        "username": "viewer",
        "first_name": "View",
        "last_name": "User",
        "email": "viewer@ffengine.local",
        "role": "Viewer",
        "password_env": "FFENGINE_AIRFLOW_VIEWER_PASSWORD",
    },
]

ROLE_PERMISSION_CANDIDATES: dict[str, list[tuple[str, str]]] = {
    "Viewer": [
        ("can_read", "DAG"),
        ("can_read", "DAG Run"),
        ("can_read", "Task Instance"),
        ("can_read", "Task Instances"),
        ("can_read", "HITL Detail"),
        ("can_read", "Task Logs"),
    ],
    "Op": [
        ("can_read", "DAG"),
        ("can_read", "DAG Run"),
        ("can_read", "Task Instance"),
        ("can_create", "DAG Run"),
    ],
}


def _sync_role_permissions(sm, role_name: str, candidates: list[tuple[str, str]]) -> None:
    if not hasattr(sm, "find_permission_view_menu") or not hasattr(sm, "add_permission_role"):
        log.warning("permission sync API not available on security manager; skipping role=%s", role_name)
        return

    role = sm.find_role(role_name)
    if role is None:
        log.warning("role %s not found for permission sync", role_name)
        return

    for permission_name, view_menu_name in candidates:
        pv = sm.find_permission_view_menu(permission_name, view_menu_name)
        if pv is None:
            continue

        if sm.add_permission_role(role, pv):
            log.info("granted: role=%s permission=%s view=%s", role_name, permission_name, view_menu_name)


def main() -> int:
    from airflow.providers.fab.www.app import create_app

    app = create_app(enable_plugins=False)
    with app.app_context():
        sm = app.appbuilder.sm
        sm.sync_roles()
        for role_name, candidates in ROLE_PERMISSION_CANDIDATES.items():
            _sync_role_permissions(sm, role_name, candidates)

        for spec in USERS:
            role = sm.find_role(spec["role"])
            if role is None:
                log.warning("role %s not found, skipping %s", spec["role"], spec["username"])
                continue

            password = os.environ.get(spec["password_env"])
            if not password:
                log.error(
                    "required password env var is missing or empty: %s",
                    spec["password_env"],
                )
                return 2

            existing = sm.find_user(username=spec["username"])
            if existing is not None:
                log.info("exists: %s (%s)", spec["username"], spec["role"])
                continue

            created = sm.add_user(
                username=spec["username"],
                first_name=spec["first_name"],
                last_name=spec["last_name"],
                email=spec["email"],
                role=role,
                password=password,
            )
            if created is False or created is None:
                log.error("failed to create %s", spec["username"])
            else:
                log.info("created: %s (%s)", spec["username"], spec["role"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
