"""Airflow 3.x webserver + FAB auth config.

AUTH_TYPE = AUTH_DB: kullanicilar ab_user tablosunda tutulur.
OIDC/LDAP/OAuth eklenmek istenirse AUTH_TYPE degistirilir (C19+ kapsam).
"""
import os

from flask_appbuilder.const import AUTH_DB

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
