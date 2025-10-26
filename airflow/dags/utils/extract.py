"""Data extraction helpers (rolling window faker seeding)."""
from __future__ import annotations

import random
from datetime import date, timedelta

from faker import Faker
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.dags import config

faker = Faker()


def _purchase_date_string(target_date: date) -> str:
    return f"{target_date.day}/{target_date.month}/{target_date.year}"


def _ensure_rows_for_day(cur, target_date: date, min_rows: int) -> None:
    day_str = _purchase_date_string(target_date)
    cur.execute(
        """
        SELECT COUNT(1)
        FROM raw.BT_CRYPTO_TRANSACTION_HISTORY
        WHERE purchase_date = %s
        """,
        (day_str,),
    )
    (current_rows,) = cur.fetchone()
    missing = max(0, min_rows - current_rows)
    if missing == 0:
        return

    insert_sql = """
        INSERT INTO raw.BT_CRYPTO_TRANSACTION_HISTORY (
            site_id, user_id, purchase_date, crypto_type, purchase_price, purchase_units, inserted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, now())
    """
    for _ in range(missing):
        site = random.choice(config.ALLOWED_SITES)
        user_id = faker.random_int(min=100000, max=999999)
        crypto = random.choice(config.ALLOWED_CRYPTOS)
        base_price = 20000 if crypto == "BTC" else 1200 if crypto == "ETH" else 1
        price = round(random.uniform(0.8, 1.2) * base_price, 4)
        units = round(random.uniform(0.2, 5.0), 8)
        cur.execute(insert_sql, (site, user_id, day_str, crypto, price, units))


def ensure_recent_history(min_rows: int | None = None) -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    target_rows = min_rows or config.MIN_ROWS_PER_DAY
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for offset in range(config.WINDOW_DAYS):
                day = date.today() - timedelta(days=offset)
                _ensure_rows_for_day(cur, day, target_rows)
        conn.commit()
