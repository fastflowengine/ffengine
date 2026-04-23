"""Seed Airflow FAB users for C18.

Airflow 3.x'te `airflow users create` CLI komutu kaldirildi. FAB provider'in
SecurityManager API'sini dogrudan kullanarak idempotent user seed yapar.

Parolalar env var'dan okunur; eksikse dev default kullanilir.
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
        "password_default": "admin",
    },
    {
        "username": "breakglass",
        "first_name": "Break",
        "last_name": "Glass",
        "email": "breakglass@ffengine.local",
        "role": "Admin",
        "password_env": "FFENGINE_AIRFLOW_BREAKGLASS_PASSWORD",
        "password_default": "breakglass",
    },
    {
        "username": "operator",
        "first_name": "Ops",
        "last_name": "User",
        "email": "operator@ffengine.local",
        "role": "Op",
        "password_env": "FFENGINE_AIRFLOW_OP_PASSWORD",
        "password_default": "operator",
    },
    {
        "username": "viewer",
        "first_name": "View",
        "last_name": "User",
        "email": "viewer@ffengine.local",
        "role": "Viewer",
        "password_env": "FFENGINE_AIRFLOW_VIEWER_PASSWORD",
        "password_default": "viewer",
    },
]


def main() -> int:
    from airflow.providers.fab.www.app import create_app

    app = create_app(enable_plugins=False)
    with app.app_context():
        sm = app.appbuilder.sm
        sm.sync_roles()

        for spec in USERS:
            role = sm.find_role(spec["role"])
            if role is None:
                log.warning("role %s not found, skipping %s", spec["role"], spec["username"])
                continue

            password = os.environ.get(spec["password_env"], spec["password_default"])
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
