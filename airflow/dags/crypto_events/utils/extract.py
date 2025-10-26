"""Data extraction helpers (rolling window faker seeding)."""
from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

from faker import Faker
from airflow.providers.postgres.hooks.postgres import PostgresHook

from crypto_events import config


def _purchase_date_string(target_date: date) -> str:
    return f"{target_date.day}/{target_date.month}/{target_date.year}"


def _ensure_rows_for_day(cur, target_date: date, min_rows: int, seed: int) -> None:
    day_faker = Faker()
    day_faker.seed_instance(seed)
    rng = random.Random(seed)
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
        site = rng.choice(config.ALLOWED_SITES)
        user_id = day_faker.random_int(min=100000, max=999999)
        crypto = rng.choice(config.ALLOWED_CRYPTOS)
        base_price = 20000 if crypto == "BTC" else 1200 if crypto == "ETH" else 1
        price = round(rng.uniform(0.8, 1.2) * base_price, 4)
        units = round(rng.uniform(0.2, 5.0), 8)
        cur.execute(insert_sql, (site, user_id, day_str, crypto, price, units))


def _anchor_date_from_context(context: dict[str, Any]) -> date:
    logical = context.get("logical_date")
    if logical is None:
        return date.today()
    logical_dt = logical
    if hasattr(logical_dt, "in_timezone"):
        logical_dt = logical_dt.in_timezone("UTC")
    return logical_dt.date()


def ensure_recent_history(min_rows: int | None = None, **context: Any) -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    target_rows = min_rows or config.MIN_ROWS_PER_DAY
    anchor_date = _anchor_date_from_context(context)
    base_seed = anchor_date.toordinal() * 1000
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for offset in range(config.WINDOW_DAYS):
                day = anchor_date - timedelta(days=offset)
                seed = base_seed + day.toordinal()
                _ensure_rows_for_day(cur, day, target_rows, seed)
        conn.commit()
