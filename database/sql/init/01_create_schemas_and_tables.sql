CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS prod;
CREATE SCHEMA IF NOT EXISTS dqm;

CREATE TABLE IF NOT EXISTS raw.BT_CRYPTO_TRANSACTION_HISTORY (
    site_id TEXT NOT NULL,
    user_id BIGINT NOT NULL,
    purchase_date TEXT NOT NULL,
    crypto_type TEXT NOT NULL,
    purchase_price NUMERIC(18,4) NOT NULL,
    purchase_units NUMERIC(18,8) NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.BT_CRYPTO_EVENTS_CANDIDATE (
    site_id TEXT NOT NULL,
    user_id BIGINT NOT NULL,
    purchase_date DATE NOT NULL,
    crypto_type TEXT NOT NULL,
    purchase_price NUMERIC(18,4) NOT NULL,
    purchase_units NUMERIC(18,8) NOT NULL,
    purchase_value NUMERIC(28,8) NOT NULL,
    staged_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (site_id, user_id, purchase_date, crypto_type)
);

CREATE TABLE IF NOT EXISTS prod.BT_CRYPTO_EVENTS (
    site_id TEXT NOT NULL,
    user_id BIGINT NOT NULL,
    purchase_date DATE NOT NULL,
    crypto_type TEXT NOT NULL,
    purchase_price NUMERIC(18,4) NOT NULL,
    purchase_units NUMERIC(18,8) NOT NULL,
    purchase_value NUMERIC(28,8) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (site_id, user_id, purchase_date, crypto_type)
);

CREATE TABLE IF NOT EXISTS dqm.BT_CRYPTO_EVENTS_REJECTS (
    rejected_at TIMESTAMPTZ DEFAULT now(),
    suite_name TEXT,
    site_id TEXT,
    user_id TEXT,
    purchase_date_raw TEXT,
    crypto_type TEXT,
    purchase_price TEXT,
    purchase_units TEXT,
    reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dqm.DQ_RUNS (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suite_name TEXT NOT NULL,
    status TEXT NOT NULL,
    checked_at TIMESTAMPTZ DEFAULT now(),
    details JSONB
);
