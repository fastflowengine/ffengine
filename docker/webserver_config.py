"""Airflow 3.x webserver + FAB auth config.

AUTH_TYPE = AUTH_DB: kullanicilar ab_user tablosunda tutulur.
OIDC/LDAP/OAuth eklenmek istenirse AUTH_TYPE degistirilir (C19+ kapsam).
"""
import os

from flask_appbuilder.const import AUTH_DB

# Workaround for apache-airflow-providers-fab 3.6.1 bug: AirflowAppBuilder.__init__
# re-calls create_auth_manager() which overwrites the api_fastapi.app.auth_manager
# global, discarding the instance that has flask_app set. Result: GET /auth/logout
# raises 500 "Flask app is not initialized" because the login.py route reads the
# (stale) global. Patching create_auth_manager to return the existing instance
# keeps appbuilder and flask_app on the same singleton.
import airflow.api_fastapi.app as _ff_api_app
import airflow.providers.fab.www.extensions.init_appbuilder as _ff_init_appbuilder

_ff_orig_create_auth_manager = _ff_api_app.create_auth_manager


def _ff_patched_create_auth_manager():
    if _ff_api_app.auth_manager is not None:
        return _ff_api_app.auth_manager
    return _ff_orig_create_auth_manager()


_ff_api_app.create_auth_manager = _ff_patched_create_auth_manager
# init_appbuilder did `from airflow.api_fastapi.app import create_auth_manager` at
# module load, binding the original in its own namespace — patch that binding too.
_ff_init_appbuilder.create_auth_manager = _ff_patched_create_auth_manager

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN", ""),
)

AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False
AUTH_ROLE_ADMIN = "Admin"
AUTH_ROLE_PUBLIC = "Public"

SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
