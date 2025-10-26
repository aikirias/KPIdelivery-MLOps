"""Database tasks for the crypto events pipeline."""
from __future__ import annotations

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.dags import config


def build_events_candidate() -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(
        """
        BEGIN;
        TRUNCATE TABLE staging.bt_crypto_events_candidate;
        WITH source AS (
            SELECT site_id,
                   user_id,
                   CASE
                       WHEN purchase_date ~ %(regex)s THEN to_date(purchase_date, 'DD/MM/YYYY')
                       ELSE NULL
                   END AS purchase_date_converted,
                   crypto_type,
                   purchase_price,
                   purchase_units
            FROM raw.bt_crypto_transaction_history
            WHERE inserted_at >= now() - (%(window)s)::interval
        ),
        filtered AS (
            SELECT *
            FROM source
            WHERE purchase_date_converted BETWEEN CURRENT_DATE - (%(range)s)::interval AND CURRENT_DATE
        )
        INSERT INTO staging.bt_crypto_events_candidate (
            site_id,
            user_id,
            purchase_date,
            crypto_type,
            purchase_price,
            purchase_units,
            purchase_value,
            staged_at
        )
        SELECT site_id,
               user_id,
               purchase_date_converted,
               crypto_type,
               purchase_price,
               purchase_units,
               ROUND(purchase_price * purchase_units, 8) AS purchase_value,
               now()
        FROM filtered;
        COMMIT;
        """,
        parameters={
            "regex": config.RAW_DATE_REGEX,
            "window": config.WINDOW_INTERVAL,
            "range": config.RANGE_INTERVAL,
        },
    )


def merge_to_prod() -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(
        f"""
        BEGIN;
        WITH windowed AS (
            SELECT *
            FROM staging.bt_crypto_events_candidate
            WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{config.RANGE_INTERVAL}' AND CURRENT_DATE
        )
        INSERT INTO prod.bt_crypto_events (
            site_id,
            user_id,
            purchase_date,
            crypto_type,
            purchase_price,
            purchase_units,
            purchase_value,
            is_active,
            created_at,
            updated_at
        )
        SELECT site_id,
               user_id,
               purchase_date,
               crypto_type,
               purchase_price,
               purchase_units,
               purchase_value,
               true,
               now(),
               now()
        FROM windowed
        ON CONFLICT (site_id, user_id, purchase_date, crypto_type)
        DO UPDATE SET
            purchase_price = EXCLUDED.purchase_price,
            purchase_units = EXCLUDED.purchase_units,
            purchase_value = EXCLUDED.purchase_value,
            is_active = true,
            updated_at = now();

        UPDATE prod.bt_crypto_events p
        SET is_active = false,
            updated_at = now()
        WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{config.RANGE_INTERVAL}' AND CURRENT_DATE
          AND NOT EXISTS (
              SELECT 1
              FROM staging.bt_crypto_events_candidate s
              WHERE s.site_id = p.site_id
                AND s.user_id = p.user_id
                AND s.purchase_date = p.purchase_date
                AND s.crypto_type = p.crypto_type
          );
        COMMIT;
        """
    )
