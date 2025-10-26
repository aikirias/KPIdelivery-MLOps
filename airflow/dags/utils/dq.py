"""Data quality helpers leveraging Great Expectations."""
from __future__ import annotations

import json

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from great_expectations.data_context import FileDataContext
from great_expectations.expectations import registry as ge_registry

from airflow.dags import config

ge_registry.register_core_expectations()


def _record_dq_run(suite_name: str, success: bool, result: dict) -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO dqm.dq_runs (suite_name, status, details)
        VALUES (%(suite)s, %(status)s, %(details)s::jsonb)
        """,
        parameters={
            "suite": suite_name,
            "status": "success" if success else "failed",
            "details": json.dumps(result, default=str),
        },
    )


def _insert_rejects(query: str, suite_name: str, reason: str, parameters=None) -> None:
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    params = {"suite": suite_name, "reason": reason}
    if parameters:
        params.update(parameters)
    hook.run(
        f"""
        INSERT INTO dqm.bt_crypto_events_rejects (
            suite_name, site_id, user_id, purchase_date_raw, crypto_type,
            purchase_price, purchase_units, reason
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
            {query}
        ) q
        """,
        parameters=params,
    )


def _capture_raw_rejects() -> None:
    allowed_sites = ",".join(f"'{site}'" for site in config.ALLOWED_SITES)
    allowed_cryptos = ",".join(f"'{crypto}'" for crypto in config.ALLOWED_CRYPTOS)
    base = f"""
        SELECT site_id,
               user_id::text AS user_id,
               purchase_date AS purchase_date_raw,
               crypto_type,
               purchase_price::text AS purchase_price,
               purchase_units::text AS purchase_units
        FROM raw.bt_crypto_transaction_history
        WHERE inserted_at >= now() - INTERVAL '{config.WINDOW_INTERVAL}'
    """
    _insert_rejects(base + f" AND site_id NOT IN ({allowed_sites})", "suite_raw_history", "invalid_site_id")
    _insert_rejects(base + " AND user_id IS NULL", "suite_raw_history", "missing_user_id")
    _insert_rejects(
        base + " AND (purchase_date IS NULL OR purchase_date !~ %(regex)s)",
        "suite_raw_history",
        "invalid_purchase_date",
        parameters={"regex": config.RAW_DATE_REGEX},
    )
    _insert_rejects(
        base + f" AND crypto_type NOT IN ({allowed_cryptos})",
        "suite_raw_history",
        "invalid_crypto_type",
    )
    _insert_rejects(base + " AND purchase_price < 0", "suite_raw_history", "negative_purchase_price")
    _insert_rejects(base + " AND purchase_units <= 0", "suite_raw_history", "non_positive_units")


def _capture_staging_rejects() -> None:
    base = f"""
        SELECT site_id,
               user_id::text AS user_id,
               to_char(purchase_date, 'YYYY-MM-DD') AS purchase_date_raw,
               crypto_type,
               purchase_price::text AS purchase_price,
               purchase_units::text AS purchase_units
        FROM staging.bt_crypto_events_candidate
        WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{config.RANGE_INTERVAL}' AND CURRENT_DATE
    """
    _insert_rejects(
        base + " AND purchase_value <> ROUND(purchase_price * purchase_units, 8)",
        "suite_staging_candidate",
        "mismatched_value",
    )
    _insert_rejects(
        f"""
        SELECT site_id,
               user_id::text AS user_id,
               to_char(purchase_date, 'YYYY-MM-DD') AS purchase_date_raw,
               crypto_type,
               purchase_price::text AS purchase_price,
               purchase_units::text AS purchase_units
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY site_id, user_id, purchase_date, crypto_type ORDER BY staged_at DESC) AS rn
            FROM staging.bt_crypto_events_candidate
            WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{config.RANGE_INTERVAL}' AND CURRENT_DATE
        ) ranked
        WHERE rn > 1
        """,
        "suite_staging_candidate",
        "duplicate_candidate",
    )


def run_ge_validation(suite_name: str, validation_payload: dict) -> None:
    context = FileDataContext(context_root_dir=str(config.GE_ROOT))
    result = context.run_checkpoint(
        checkpoint_name=config.CHECKPOINT_NAME,
        validations=[
            {
                "batch_request": {
                    "datasource_name": "crypto_postgres",
                    "data_connector_name": "default_runtime_data_connector_name",
                    **validation_payload,
                },
                "expectation_suite_name": suite_name,
            }
        ],
    )
    _record_dq_run(suite_name, result["success"], result)
    if not result["success"]:
        if suite_name == "suite_raw_history":
            _capture_raw_rejects()
        else:
            _capture_staging_rejects()
        raise AirflowException(f"Great Expectations validation failed for {suite_name}")
