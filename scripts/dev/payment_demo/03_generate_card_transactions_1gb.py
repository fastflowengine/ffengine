from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timedelta

import psycopg2


PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")
SOURCE_DB = os.getenv("SOURCE_DB", "source_payment")

TARGET_GB = float(os.getenv("TARGET_GB", "1"))
TARGET_BYTES = int(TARGET_GB * 1024 * 1024 * 1024)

BATCH_ROWS = int(os.getenv("BATCH_ROWS", "50000"))
CHECK_EVERY_BATCHES = int(os.getenv("CHECK_EVERY_BATCHES", "2"))

TRUNCATE_FIRST = os.getenv("TRUNCATE_FIRST", "true").lower() in {"1", "true", "yes", "y"}

TABLE_NAME = "public.card_transaction"


MERCHANT_CATEGORIES = [
    "GROCERY",
    "FUEL",
    "RESTAURANT",
    "E_COMMERCE",
    "ELECTRONICS",
    "TRAVEL",
    "HEALTHCARE",
    "CLOTHING",
    "MARKETPLACE",
    "SUBSCRIPTION",
]

MERCHANT_NAMES = [
    "Fresh Market",
    "Metro Fuel",
    "Blue Cafe",
    "Nova Online Store",
    "Techno Center",
    "City Airlines",
    "Health Plus",
    "Urban Wear",
    "Global Marketplace",
    "StreamBox",
]

CITIES = [
    "Istanbul",
    "Ankara",
    "Izmir",
    "Bursa",
    "Antalya",
    "Frankfurt",
    "London",
    "Paris",
    "Amsterdam",
    "Madrid",
]

COUNTRIES = ["TR", "TR", "TR", "TR", "TR", "DE", "GB", "FR", "NL", "ES"]
TRANSACTION_TYPES = ["POS", "ECOMMERCE", "ATM", "RECURRING"]
CHANNELS = ["CHIP", "CONTACTLESS", "ECOM", "MOBILE", "ATM"]
STATUSES = ["APPROVED", "APPROVED", "APPROVED", "DECLINED", "TECHNICAL_ERROR"]
RESPONSE_CODES = ["00", "00", "00", "05", "51", "91"]
CURRENCIES = ["TRY", "TRY", "TRY", "EUR", "USD"]


def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=SOURCE_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def get_heap_size_bytes(cur) -> int:
    cur.execute("SELECT pg_relation_size(%s::regclass);", (TABLE_NAME,))
    return int(cur.fetchone()[0])


def get_total_size_bytes(cur) -> int:
    cur.execute("SELECT pg_total_relation_size(%s::regclass);", (TABLE_NAME,))
    return int(cur.fetchone()[0])


def get_row_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM public.card_transaction;")
    return int(cur.fetchone()[0])


def format_bytes(value: int) -> str:
    gb = value / (1024 * 1024 * 1024)
    mb = value / (1024 * 1024)
    if gb >= 1:
        return f"{gb:.2f} GB"
    return f"{mb:.2f} MB"


def build_metadata_text(i: int, merchant_category: str, status: str) -> str:
    # Bu alan tabloyu 1 GB'a makul satır sayısıyla ulaştırmak için kontrollü geniş tutuldu.
    # Gerçek müşteri verisi değildir; tamamen synthetic demo verisidir.
    risk_score = (i * 17) % 100
    campaign_id = (i * 31) % 10000
    device_id = f"DEV-{(i * 13) % 1000000:06d}"
    session_id = f"SES-{i:012d}"

    filler = (
        "synthetic_payment_demo_payload "
        "ffengine_streaming_transfer_benchmark "
        "banking_payment_card_authorization_event "
        "no_real_customer_data "
    )

    return (
        f"risk_score={risk_score};"
        f"campaign_id={campaign_id};"
        f"device_id={device_id};"
        f"session_id={session_id};"
        f"category={merchant_category};"
        f"status={status};"
        f"{filler}"
    )


def generate_batch(start_id: int, batch_rows: int) -> io.StringIO:
    buffer = io.StringIO()
    base_ts = datetime(2026, 1, 1, 0, 0, 0)

    for offset in range(batch_rows):
        i = start_id + offset

        card_id = 1 + (i % 1000)
        customer_id = 100000 + card_id

        ts = base_ts + timedelta(seconds=i % (90 * 24 * 3600))

        merchant_idx = i % len(MERCHANT_NAMES)
        merchant_id = 1000 + (i % 5000)
        merchant_name = MERCHANT_NAMES[merchant_idx]
        merchant_category = MERCHANT_CATEGORIES[merchant_idx]
        merchant_city = CITIES[merchant_idx]
        merchant_country = COUNTRIES[merchant_idx]

        transaction_type = TRANSACTION_TYPES[i % len(TRANSACTION_TYPES)]
        channel = CHANNELS[i % len(CHANNELS)]

        amount = ((i * 37) % 250000) / 100.0 + 1.0
        currency = CURRENCIES[i % len(CURRENCIES)]

        auth_code = f"A{i % 1000000:06d}"
        response_code = RESPONSE_CODES[i % len(RESPONSE_CODES)]
        status = STATUSES[i % len(STATUSES)]

        terminal_id = f"TERM-{merchant_id:06d}"
        rrn = f"RRN{i:012d}"

        installment_count = 1 + (i % 12 if transaction_type in {"POS", "ECOMMERCE"} else 0)
        is_international = merchant_country != "TR"
        is_contactless = channel == "CONTACTLESS"

        metadata_text = build_metadata_text(i, merchant_category, status)
        created_at = ts

        # COPY delimiter: tab. İçerikler tab içermediği için güvenli.
        row = [
            str(i),
            str(card_id),
            str(customer_id),
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            str(merchant_id),
            merchant_name,
            merchant_category,
            merchant_city,
            merchant_country,
            transaction_type,
            channel,
            f"{amount:.2f}",
            currency,
            auth_code,
            response_code,
            status,
            terminal_id,
            rrn,
            str(installment_count),
            "true" if is_international else "false",
            "true" if is_contactless else "false",
            metadata_text,
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
        ]

        buffer.write("\t".join(row))
        buffer.write("\n")

    buffer.seek(0)
    return buffer


def main() -> int:
    print("[config]")
    print(f"  db={SOURCE_DB}")
    print(f"  table={TABLE_NAME}")
    print(f"  target_heap_size={TARGET_GB} GB")
    print(f"  batch_rows={BATCH_ROWS}")
    print(f"  truncate_first={TRUNCATE_FIRST}")

    copy_sql = """
    COPY public.card_transaction (
        transaction_id,
        card_id,
        customer_id,
        transaction_ts,
        merchant_id,
        merchant_name,
        merchant_category,
        merchant_city,
        merchant_country,
        transaction_type,
        channel,
        amount,
        currency_code,
        authorization_code,
        response_code,
        transaction_status,
        terminal_id,
        rrn,
        installment_count,
        is_international,
        is_contactless,
        metadata_text,
        created_at
    )
    FROM STDIN WITH (
        FORMAT csv,
        DELIMITER E'\\t',
        NULL ''
    );
    """

    with connect() as conn:
        with conn.cursor() as cur:
            if TRUNCATE_FIRST:
                print("[truncate] public.card_transaction")
                cur.execute("TRUNCATE TABLE public.card_transaction;")
                conn.commit()

            current_heap = get_heap_size_bytes(cur)
            next_id = 1
            batch_no = 0

            while current_heap < TARGET_BYTES:
                batch_no += 1
                buffer = generate_batch(next_id, BATCH_ROWS)

                cur.copy_expert(copy_sql, buffer)
                conn.commit()

                next_id += BATCH_ROWS

                if batch_no % CHECK_EVERY_BATCHES == 0:
                    current_heap = get_heap_size_bytes(cur)
                    total_size = get_total_size_bytes(cur)
                    row_count = get_row_count(cur)

                    print(
                        f"[progress] batch={batch_no} "
                        f"rows={row_count:,} "
                        f"heap={format_bytes(current_heap)} "
                        f"total={format_bytes(total_size)}"
                    )

            current_heap = get_heap_size_bytes(cur)
            total_size = get_total_size_bytes(cur)
            row_count = get_row_count(cur)

    print("[done]")
    print(f"  rows={row_count:,}")
    print(f"  heap_size={format_bytes(current_heap)}")
    print(f"  total_size={format_bytes(total_size)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())