INSERT INTO raw.BT_CRYPTO_TRANSACTION_HISTORY (site_id, user_id, purchase_date, crypto_type, purchase_price, purchase_units)
SELECT site_id, user_id, purchase_date, crypto_type, purchase_price, purchase_units
FROM (VALUES
    ('ARGENTINA',100000,'1/1/2023','BTC',20000,2),
    ('ARGENTINA',100012,'1/1/2023','BTC',20010,1.4),
    ('BRASIL',200234,'1/1/2023','BTC',20200,0.5),
    ('BRASIL',200234,'1/2/2023','ETH',1200,1.3),
    ('ARGENTINA',105013,'2/1/2023','USDC',1,4000),
    ('ARGENTINA',116821,'2/1/2023','ETH',1350,0.5),
    ('ARGENTINA',143159,'2/1/2023','BTC',19983,5),
    ('ARGENTINA',132169,'2/1/2023','ETH',1246,1.2),
    ('MEXICO',315951,'3/1/2023','BTC',20200,0.5),
    ('MEXICO',356479,'14/1/2023','ETH',1200,1.3),
    ('BRASIL',200234,'1/1/2023','USDC',1,5500),
    ('BRASIL',200234,'1/2/2023','ETH',1200,1.3)
) AS seed(site_id, user_id, purchase_date, crypto_type, purchase_price, purchase_units)
WHERE NOT EXISTS (
    SELECT 1 FROM raw.BT_CRYPTO_TRANSACTION_HISTORY
);
