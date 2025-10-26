"""Data quality helpers leveraging Great Expectations."""
from __future__ import annotations

import json

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from great_expectations.data_context import FileDataContext
from great_expectations.expectations import registry as ge_registry

from crypto_events import config

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


def _run_reject_template(
    template_name: str,
    suite_name: str,
    reason: str,
    *,
    sql_context: dict | None = None,
    parameters: dict | None = None,
) -> None:
    context = {
        "window_interval": config.WINDOW_INTERVAL,
        "range_interval": config.RANGE_INTERVAL,
    }
    if sql_context:
        context.update(sql_context)
    sql = config.render_sql(template_name, **context)
    params = {"suite": suite_name, "reason": reason}
    if parameters:
        params.update(parameters)
    hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    hook.run(sql, parameters=params)


def _capture_raw_rejects() -> None:
    _run_reject_template(
        "raw_reject_invalid_site.sql",
        "suite_raw_history",
        "invalid_site_id",
        sql_context={"allowed_sites": config.ALLOWED_SITES},
    )
    _run_reject_template(
        "raw_reject_missing_user.sql",
        "suite_raw_history",
        "missing_user_id",
    )
    _run_reject_template(
        "raw_reject_invalid_purchase_date.sql",
        "suite_raw_history",
        "invalid_purchase_date",
        parameters={"regex": config.RAW_DATE_REGEX},
    )
    _run_reject_template(
        "raw_reject_invalid_crypto.sql",
        "suite_raw_history",
        "invalid_crypto_type",
        sql_context={"allowed_cryptos": config.ALLOWED_CRYPTOS},
    )
    _run_reject_template(
        "raw_reject_negative_price.sql",
        "suite_raw_history",
        "negative_purchase_price",
    )
    _run_reject_template(
        "raw_reject_non_positive_units.sql",
        "suite_raw_history",
        "non_positive_units",
    )


def _capture_staging_rejects() -> None:
    _run_reject_template(
        "staging_reject_mismatched_value.sql",
        "suite_staging_candidate",
        "mismatched_value",
    )
    _run_reject_template(
        "staging_reject_duplicates.sql",
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
