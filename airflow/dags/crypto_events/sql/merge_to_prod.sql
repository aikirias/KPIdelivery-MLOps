BEGIN;
WITH windowed AS (
    SELECT *
    FROM staging.bt_crypto_events_candidate
    WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{{ params.range_interval }}' AND CURRENT_DATE
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
        WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{{ params.range_interval }}' AND CURRENT_DATE
  AND NOT EXISTS (
      SELECT 1
      FROM staging.bt_crypto_events_candidate s
      WHERE s.site_id = p.site_id
        AND s.user_id = p.user_id
        AND s.purchase_date = p.purchase_date
        AND s.crypto_type = p.crypto_type
  );
COMMIT;
