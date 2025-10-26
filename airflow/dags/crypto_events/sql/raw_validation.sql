SELECT site_id,
       user_id,
       purchase_date,
       crypto_type,
       purchase_price,
       purchase_units,
       inserted_at
FROM raw.bt_crypto_transaction_history
WHERE inserted_at >= now() - INTERVAL '{{ window_interval }}'
