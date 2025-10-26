"""Airflow DAG orchestrating MELI crypto event pipeline with validate-before-load."""
from __future__ import annotations

import json
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from great_expectations.data_context import FileDataContext
from great_expectations.expectations import registry as ge_registry

ge_registry.register_core_expectations()

GE_ROOT = Path("/opt/airflow/ge")
CHECKPOINT_NAME = "crypto_checkpoint"
POSTGRES_CONN_ID = "crypto_postgres"
RAW_DATE_REGEX = r"^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/20[0-9]{2}$"
WINDOW_DAYS = 5
WINDOW_INTERVAL = f"{WINDOW_DAYS} days"
RANGE_INTERVAL = f"{WINDOW_DAYS - 1} day"

RAW_VALIDATION = {
    "runtime_parameters": {
        "query": f"""
            SELECT site_id,
                   user_id,
                   purchase_date,
                   crypto_type,
                   purchase_price,
                   purchase_units,
                   inserted_at
            FROM raw.bt_crypto_transaction_history
            WHERE inserted_at >= now() - INTERVAL '{WINDOW_INTERVAL}'
        """
    },
    "batch_identifiers": {"default_identifier_name": "raw_history_window"},
    "data_asset_name": "raw_history_last5",
}

STAGING_VALIDATION = {
    "runtime_parameters": {
        "query": f"""
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
            WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{RANGE_INTERVAL}' AND CURRENT_DATE
        """
    },
    "batch_identifiers": {"default_identifier_name": "staging_candidate_window"},
    "data_asset_name": "staging_candidate_last5",
}


def build_events_candidate() -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        BEGIN;
        TRUNCATE TABLE staging.bt_crypto_events_candidate;
        WITH source AS (
            SELECT site_id,
                   user_id,
                   CASE
                       WHEN purchase_date ~ %(regex)s THEN to_date(purchase_date, 'DD/MM/YYYY')
                       ELSE NULL
                   END AS purchase_date_converted,
                   crypto_type,
                   purchase_price,
                   purchase_units
            FROM raw.bt_crypto_transaction_history
            WHERE inserted_at >= now() - (%(window)s)::interval
        ),
        filtered AS (
            SELECT *
            FROM source
            WHERE purchase_date_converted BETWEEN CURRENT_DATE - (%(range)s)::interval AND CURRENT_DATE
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
        """,
        parameters={
            "regex": RAW_DATE_REGEX,
            "window": WINDOW_INTERVAL,
            "range": RANGE_INTERVAL,
        },
    )


def record_dq_run(suite_name: str, success: bool, result: dict) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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


def insert_rejects(
    query: str, suite_name: str, reason: str, parameters: dict | None = None
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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


def capture_raw_rejects() -> None:
    base_select = f"""
        SELECT site_id,
               user_id::text AS user_id,
               purchase_date AS purchase_date_raw,
               crypto_type,
               purchase_price::text AS purchase_price,
               purchase_units::text AS purchase_units
        FROM raw.bt_crypto_transaction_history
        WHERE inserted_at >= now() - INTERVAL '{WINDOW_INTERVAL}'
    """
    insert_rejects(
        base_select + " AND site_id NOT IN ('ARGENTINA','BRASIL','MEXICO')",
        "suite_raw_history",
        "invalid_site_id",
    )
    insert_rejects(
        base_select + " AND user_id IS NULL",
        "suite_raw_history",
        "missing_user_id",
    )
    insert_rejects(
        base_select + " AND (purchase_date IS NULL OR purchase_date !~ %(regex)s)",
        "suite_raw_history",
        "invalid_purchase_date",
        parameters={"regex": RAW_DATE_REGEX},
    )
    insert_rejects(
        base_select + " AND crypto_type NOT IN ('BTC','ETH','USDC')",
        "suite_raw_history",
        "invalid_crypto_type",
    )
    insert_rejects(
        base_select + " AND purchase_price < 0",
        "suite_raw_history",
        "negative_purchase_price",
    )
    insert_rejects(
        base_select + " AND purchase_units <= 0",
        "suite_raw_history",
        "non_positive_units",
    )


def capture_staging_rejects() -> None:
    base_select = f"""
        SELECT site_id,
               user_id::text AS user_id,
               to_char(purchase_date, 'YYYY-MM-DD') AS purchase_date_raw,
               crypto_type,
               purchase_price::text AS purchase_price,
               purchase_units::text AS purchase_units
        FROM staging.bt_crypto_events_candidate
        WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{RANGE_INTERVAL}' AND CURRENT_DATE
    """
    insert_rejects(
        base_select + " AND purchase_value <> ROUND(purchase_price * purchase_units, 8)",
        "suite_staging_candidate",
        "mismatched_value",
    )
    insert_rejects(
        """
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
            WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{RANGE_INTERVAL}' AND CURRENT_DATE
        ) ranked
        WHERE rn > 1
        """,
        "suite_staging_candidate",
        "duplicate_candidate",
    )


def run_ge_validation(suite_name: str, validation_payload: dict) -> None:
    context = FileDataContext(context_root_dir=str(GE_ROOT))
    result = context.run_checkpoint(
        checkpoint_name=CHECKPOINT_NAME,
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
        run_name=f"{suite_name}_{pendulum.now('UTC').format('YYYYMMDDHHmmss')}",
    )
    record_dq_run(suite_name, result["success"], result)
    if not result["success"]:
        if suite_name == "suite_raw_history":
            capture_raw_rejects()
        else:
            capture_staging_rejects()
        raise AirflowException(f"Great Expectations validation failed for {suite_name}")


def merge_to_prod() -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        BEGIN;
        WITH windowed AS (
            SELECT *
            FROM staging.bt_crypto_events_candidate
            WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{RANGE_INTERVAL}' AND CURRENT_DATE
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
        WHERE purchase_date BETWEEN CURRENT_DATE - INTERVAL '{RANGE_INTERVAL}' AND CURRENT_DATE
          AND NOT EXISTS (
              SELECT 1
              FROM staging.bt_crypto_events_candidate s
              WHERE s.site_id = p.site_id
                AND s.user_id = p.user_id
                AND s.purchase_date = p.purchase_date
                AND s.crypto_type = p.crypto_type
          );
        COMMIT;
        """
    )


def vacuum_analyze_prod():
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(
        "VACUUM (ANALYZE) prod.bt_crypto_events"
    )


def notify_success(**_):
    print("Crypto events pipeline run finished successfully")


def build_dag() -> DAG:
    with DAG(
        dag_id="crypto_events_dag",
        schedule="@daily",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        default_args={"owner": "data-eng", "depends_on_past": False},
        max_active_runs=1,
        tags=["crypto", "meli", "dq"],
    ) as dag:
        generate_fake_history = BashOperator(
            task_id="generate_fake_history_last_5_days",
            bash_command="python /opt/airflow/scripts/faker_seed.py",
            env={
                "PGHOST": "postgres",
                "PGPORT": "5432",
                "PGDATABASE": "crypto_db",
                "PGUSER": "airflow",
                "PGPASSWORD": "airflow",
            },
        )

        build_candidate = PythonOperator(
            task_id="build_events_candidate",
            python_callable=build_events_candidate,
        )

        ge_validate_raw = PythonOperator(
            task_id="ge_validate_raw",
            python_callable=run_ge_validation,
            op_kwargs={"suite_name": "suite_raw_history", "validation_payload": RAW_VALIDATION},
        )

        ge_validate_candidate = PythonOperator(
            task_id="ge_validate_candidate",
            python_callable=run_ge_validation,
            op_kwargs={"suite_name": "suite_staging_candidate", "validation_payload": STAGING_VALIDATION},
        )

        merge_prod = PythonOperator(
            task_id="merge_to_prod",
            python_callable=merge_to_prod,
        )

        notify = PythonOperator(
            task_id="notify_success",
            python_callable=notify_success,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        generate_fake_history >> build_candidate
        build_candidate >> ge_validate_raw >> ge_validate_candidate >> merge_prod >> notify

    return dag


dag = build_dag()
