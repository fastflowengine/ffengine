
from __future__ import annotations

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5436"))
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow_password")
ADMIN_DB = os.getenv("ADMIN_DB", "airflow_password")

SOURCE_DB = os.getenv("SOURCE_DB", "source_payment")
TARGET_DB = os.getenv("TARGET_DB", "target_dwh")

RESET_DEMO_DBS = os.getenv("RESET_DEMO_DBS", "false").lower() in {"1", "true", "yes", "y"}


CARD_INFO_DDL = """
CREATE TABLE IF NOT EXISTS public.card_info (
    card_id              BIGINT PRIMARY KEY,
    customer_id          BIGINT NOT NULL,
    card_token           VARCHAR(64) NOT NULL,
    masked_card_number   VARCHAR(32) NOT NULL,
    card_brand           VARCHAR(20) NOT NULL,
    card_type            VARCHAR(20) NOT NULL,
    card_status          VARCHAR(20) NOT NULL,
    currency_code        CHAR(3) NOT NULL,
    credit_limit         NUMERIC(14,2) NOT NULL,
    current_balance      NUMERIC(14,2) NOT NULL,
    issue_date           DATE NOT NULL,
    expiry_date          DATE NOT NULL,
    created_at           TIMESTAMP NOT NULL
);
"""

CARD_TRANSACTION_DDL = """
CREATE TABLE IF NOT EXISTS public.card_transaction (
    transaction_id        BIGINT NOT NULL,
    card_id               BIGINT NOT NULL,
    customer_id           BIGINT NOT NULL,
    transaction_ts        TIMESTAMP NOT NULL,
    merchant_id           INTEGER NOT NULL,
    merchant_name         VARCHAR(120) NOT NULL,
    merchant_category     VARCHAR(60) NOT NULL,
    merchant_city         VARCHAR(60) NOT NULL,
    merchant_country      CHAR(2) NOT NULL,
    transaction_type      VARCHAR(20) NOT NULL,
    channel               VARCHAR(20) NOT NULL,
    amount                NUMERIC(14,2) NOT NULL,
    currency_code         CHAR(3) NOT NULL,
    authorization_code    VARCHAR(12) NOT NULL,
    response_code         VARCHAR(8) NOT NULL,
    transaction_status    VARCHAR(20) NOT NULL,
    terminal_id           VARCHAR(32) NOT NULL,
    rrn                   VARCHAR(32) NOT NULL,
    installment_count     SMALLINT NOT NULL,
    is_international      BOOLEAN NOT NULL,
    is_contactless        BOOLEAN NOT NULL,
    metadata_text         TEXT NOT NULL,
    created_at            TIMESTAMP NOT NULL
);
"""

TARGET_CARD_INFO_DDL = CARD_INFO_DDL
TARGET_CARD_TRANSACTION_DDL = CARD_TRANSACTION_DDL


def connect(dbname: str):
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=dbname,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def database_exists(conn, dbname: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        return cur.fetchone() is not None


def terminate_connections(conn, dbname: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s
              AND pid <> pg_backend_pid();
            """,
            (dbname,),
        )


def recreate_or_create_database(admin_conn, dbname: str) -> None:
    exists = database_exists(admin_conn, dbname)

    with admin_conn.cursor() as cur:
        if exists and RESET_DEMO_DBS:
            print(f"[db] dropping database: {dbname}")
            terminate_connections(admin_conn, dbname)
            cur.execute(f'DROP DATABASE "{dbname}"')
            exists = False

        if not exists:
            print(f"[db] creating database: {dbname}")
            cur.execute(f'DATABASE_NOT_ALLOWED')


def create_database_safe(admin_conn, dbname: str) -> None:
    exists = database_exists(admin_conn, dbname)

    with admin_conn.cursor() as cur:
        if exists and RESET_DEMO_DBS:
            print(f"[db] dropping database: {dbname}")
            terminate_connections(admin_conn, dbname)
            cur.execute(f'DROP DATABASE "{dbname}"')
            exists = False

        if not exists:
            print(f"[db] creating database: {dbname}")
            cur.execute(f'CREATE DATABASE "{dbname}" OWNER "{PG_USER}"')
        else:
            print(f"[db] exists: {dbname}")


def create_tables(dbname: str, is_source: bool) -> None:
    print(f"[ddl] creating tables in {dbname}")

    with connect(dbname) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS public;")

            if is_source:
                cur.execute(CARD_INFO_DDL)
                cur.execute(CARD_TRANSACTION_DDL)
            else:
                cur.execute(TARGET_CARD_INFO_DDL)
                cur.execute(TARGET_CARD_TRANSACTION_DDL)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_transaction_card_id ON public.card_transaction(card_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_transaction_ts ON public.card_transaction(transaction_ts);")

        conn.commit()


def truncate_demo_tables(dbname: str) -> None:
    print(f"[truncate] cleaning demo tables in {dbname}")

    with connect(dbname) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.card_transaction;")
            cur.execute("TRUNCATE TABLE public.card_info;")
        conn.commit()


def main() -> int:
    print("[config]")
    print(f"  host={PG_HOST}")
    print(f"  port={PG_PORT}")
    print(f"  user={PG_USER}")
    print(f"  source_db={SOURCE_DB}")
    print(f"  target_db={TARGET_DB}")
    print(f"  reset={RESET_DEMO_DBS}")

    admin_conn = connect(ADMIN_DB)
    admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        create_database_safe(admin_conn, SOURCE_DB)
        create_database_safe(admin_conn, TARGET_DB)
    finally:
        admin_conn.close()

    create_tables(SOURCE_DB, is_source=True)
    create_tables(TARGET_DB, is_source=False)

    if RESET_DEMO_DBS:
        truncate_demo_tables(SOURCE_DB)
        truncate_demo_tables(TARGET_DB)

    print("[done] databases and tables are ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())