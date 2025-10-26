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
           to_char(purchase_date, 'YYYY-MM-DD') AS purchase_date_raw,
           crypto_type,
           purchase_price::text AS purchase_price,
           purchase_units::text AS purchase_units
    FROM staging.bt_crypto_events_candidate
    WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{{ range_interval }}' AND CURRENT_DATE
      AND purchase_value <> ROUND(purchase_price * purchase_units, 8)
) src;
