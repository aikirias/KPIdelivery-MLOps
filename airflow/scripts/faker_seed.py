import os
import random
from datetime import date, timedelta

import psycopg2
from faker import Faker

faker = Faker()
SITES = ["ARGENTINA", "BRASIL", "MEXICO"]
CRYPTOS = ["BTC", "ETH", "USDC"]


def purchase_date_string(target_date: date) -> str:
    return f"{target_date.day}/{target_date.month}/{target_date.year}"


def get_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", 5432)),
        dbname=os.getenv("PGDATABASE", "crypto_db"),
        user=os.getenv("PGUSER", "airflow"),
        password=os.getenv("PGPASSWORD", "airflow"),
    )


def ensure_rows_for_day(cur, target_date: date, target_rows: int) -> None:
    day_str = purchase_date_string(target_date)
    cur.execute(
        """
        SELECT COUNT(1)
        FROM raw.BT_CRYPTO_TRANSACTION_HISTORY
        WHERE purchase_date = %s
          AND to_date(purchase_date, 'DD/MM/YYYY') BETWEEN CURRENT_DATE - INTERVAL '30 day' AND CURRENT_DATE + INTERVAL '1 day'
        """,
        (day_str,),
    )
    (current_rows,) = cur.fetchone()
    missing = max(0, target_rows - current_rows)
    if missing == 0:
        return

    insert_sql = """
        INSERT INTO raw.BT_CRYPTO_TRANSACTION_HISTORY (
            site_id, user_id, purchase_date, crypto_type, purchase_price, purchase_units, inserted_at
        ) VALUES (%s, %s, %s, %s, %s, %s, now())
    """
    for _ in range(missing):
        site = random.choice(SITES)
        user_id = faker.random_int(min=100000, max=999999)
        crypto = random.choice(CRYPTOS)
        base_price = 20000 if crypto == "BTC" else 1200 if crypto == "ETH" else 1
        price = round(random.uniform(0.8, 1.2) * base_price, 4)
        units = round(random.uniform(0.2, 5.0), 8)
        cur.execute(
            insert_sql,
            (site, user_id, day_str, crypto, price, units),
        )


def main():
    min_rows = int(os.getenv("MIN_ROWS_PER_DAY", 4))
    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            for offset in range(5):
                current_day = date.today() - timedelta(days=offset)
                ensure_rows_for_day(cur, current_day, min_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
