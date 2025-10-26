"""Churn training DAG orchestrating end-to-end MLOps pipeline."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from churn.datasets import CSV_DATASETS
from churn.tasks import data_prep, modeling


def _notify_success(**_):
    print("Churn pipeline run finished successfully.")


def build_dag() -> DAG:
    with DAG(
        dag_id="churn_training_dag",
        schedule=CSV_DATASETS,
        start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args={"owner": "ds-mlops"},
        tags=["mlops", "churn", "training"],
    ) as dag:
        clean = PythonOperator(
            task_id="clean_raw_sources",
            python_callable=data_prep.clean_sources,
        )

        features = PythonOperator(
            task_id="feature_engineering",
            python_callable=data_prep.build_feature_matrix,
        )

        train = PythonOperator(
            task_id="train_spark_model",
            python_callable=modeling.train_with_spark,
        )

        evaluate = PythonOperator(
            task_id="evaluate_candidate",
            python_callable=modeling.evaluate_candidate,
        )

        decide = BranchPythonOperator(
            task_id="decide_promotion",
            python_callable=modeling.decide_next_step,
        )

        promote = PythonOperator(
            task_id="promote_model",
            python_callable=modeling.promote_model,
        )

        skip = PythonOperator(
            task_id="skip_promotion",
            python_callable=modeling.skip_model,
        )

        finalize = PythonOperator(
            task_id="notify_success",
            python_callable=_notify_success,
        )

        clean >> features >> train >> evaluate >> decide
        decide >> promote
        decide >> skip
        promote >> finalize
        skip >> finalize

    return dag


dag = build_dag()
