from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta

import psycopg2


PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")
SOURCE_DB = os.getenv("SOURCE_DB", "source_payment")

ROW_COUNT = int(os.getenv("CARD_INFO_ROWS", "1000"))


BRANDS = ["VISA", "MASTERCARD", "TROY"]
CARD_TYPES = ["CREDIT", "DEBIT", "PREPAID"]
STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "BLOCKED", "EXPIRED"]
CURRENCIES = ["TRY", "EUR", "USD", "GBP"]


def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=SOURCE_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def build_card_row(i: int):
    card_id = i
    customer_id = 100000 + i
    brand = BRANDS[i % len(BRANDS)]
    card_type = CARD_TYPES[i % len(CARD_TYPES)]
    status = STATUSES[i % len(STATUSES)]
    currency = CURRENCIES[i % len(CURRENCIES)]

    bin_prefix = {
        "VISA": "454360",
        "MASTERCARD": "552879",
        "TROY": "979200",
    }[brand]

    last4 = f"{1000 + (i % 9000):04d}"
    masked_card_number = f"{bin_prefix}******{last4}"
    card_token = f"TKN-{brand}-{card_id:012d}"

    credit_limit = float(5000 + ((i * 137) % 95000))
    current_balance = round(credit_limit * ((i % 80) / 100.0), 2)

    issue_date = date(2020, 1, 1) + timedelta(days=i % 1600)
    expiry_date = date(issue_date.year + 5, issue_date.month, 1)
    created_at = datetime(issue_date.year, issue_date.month, min(issue_date.day, 28), 10, 0, 0)

    return (
        card_id,
        customer_id,
        card_token,
        masked_card_number,
        brand,
        card_type,
        status,
        currency,
        credit_limit,
        current_balance,
        issue_date,
        expiry_date,
        created_at,
    )


def main() -> int:
    print(f"[seed] inserting {ROW_COUNT} rows into {SOURCE_DB}.public.card_info")

    insert_sql = """
    INSERT INTO public.card_info (
        card_id,
        customer_id,
        card_token,
        masked_card_number,
        card_brand,
        card_type,
        card_status,
        currency_code,
        credit_limit,
        current_balance,
        issue_date,
        expiry_date,
        created_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (card_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        card_token = EXCLUDED.card_token,
        masked_card_number = EXCLUDED.masked_card_number,
        card_brand = EXCLUDED.card_brand,
        card_type = EXCLUDED.card_type,
        card_status = EXCLUDED.card_status,
        currency_code = EXCLUDED.currency_code,
        credit_limit = EXCLUDED.credit_limit,
        current_balance = EXCLUDED.current_balance,
        issue_date = EXCLUDED.issue_date,
        expiry_date = EXCLUDED.expiry_date,
        created_at = EXCLUDED.created_at;
    """

    with connect() as conn:
        with conn.cursor() as cur:
            rows = [build_card_row(i) for i in range(1, ROW_COUNT + 1)]
            cur.executemany(insert_sql, rows)
        conn.commit()

    print("[done] card_info seed completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())