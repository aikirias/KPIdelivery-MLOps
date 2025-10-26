"""Airflow DAG orchestrating MELI crypto event pipeline with validate-before-load."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow.dags import config
from airflow.dags.utils import db_tasks, dq, extract


def _notify_success(**_):
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
        extract_task = PythonOperator(
            task_id="extract_generate_history",
            python_callable=extract.ensure_recent_history,
        )

        transform_task = PythonOperator(
            task_id="transform_build_candidate",
            python_callable=db_tasks.build_events_candidate,
        )

        with TaskGroup(group_id="qa_quality_checks", prefix_group_id=False) as qa_group:
            ge_raw = PythonOperator(
                task_id="qa_validate_raw",
                python_callable=dq.run_ge_validation,
                op_kwargs={
                    "suite_name": "suite_raw_history",
                    "validation_payload": config.raw_validation_payload(),
                },
            )

            ge_candidate = PythonOperator(
                task_id="qa_validate_candidate",
                python_callable=dq.run_ge_validation,
                op_kwargs={
                    "suite_name": "suite_staging_candidate",
                    "validation_payload": config.staging_validation_payload(),
                },
            )

            ge_raw >> ge_candidate

        load_task = PythonOperator(
            task_id="load_merge_to_prod",
            python_callable=db_tasks.merge_to_prod,
        )

        notify = PythonOperator(
            task_id="notify_success",
            python_callable=_notify_success,
        )

        extract_task >> transform_task >> qa_group >> load_task >> notify

    return dag


dag = build_dag()
