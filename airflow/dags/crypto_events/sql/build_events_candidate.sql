BEGIN;
TRUNCATE TABLE staging.bt_crypto_events_candidate;
WITH source AS (
    SELECT site_id,
           user_id,
           CASE
               WHEN purchase_date ~ '{{ params.regex }}' THEN to_date(purchase_date, 'DD/MM/YYYY')
               ELSE NULL
           END AS purchase_date_converted,
           crypto_type,
           purchase_price,
           purchase_units
    FROM raw.bt_crypto_transaction_history
    WHERE inserted_at >= now() - INTERVAL '{{ params.window_interval }}'
),
filtered AS (
    SELECT *
    FROM source
    WHERE purchase_date_converted BETWEEN CURRENT_DATE - INTERVAL '{{ params.range_interval }}' AND CURRENT_DATE
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
