INSERT INTO dqm.bt_crypto_events_rejects (
    suite_name,
    site_id,
    user_id,
    purchase_date_raw,
    crypto_type,
    purchase_price,
    purchase_units,
    reason
)
SELECT %(suite)s,
       site_id,
       user_id,
       purchase_date_raw,
       crypto_type,
       purchase_price,
       purchase_units,
       %(reason)s
FROM (
    SELECT site_id,
           user_id::text AS user_id,
           purchase_date AS purchase_date_raw,
           crypto_type,
           purchase_price::text AS purchase_price,
           purchase_units::text AS purchase_units
    FROM raw.bt_crypto_transaction_history
    WHERE inserted_at >= now() - INTERVAL '{{ window_interval }}'
      AND user_id IS NULL
) src;
