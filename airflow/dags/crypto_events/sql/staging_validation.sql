SELECT site_id,
       user_id,
       purchase_date,
       crypto_type,
       purchase_price,
       purchase_units,
       purchase_value,
       ROUND(purchase_price * purchase_units, 8) AS purchase_value_calc,
       staged_at
FROM staging.bt_crypto_events_candidate
WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{{ range_interval }}' AND CURRENT_DATE
