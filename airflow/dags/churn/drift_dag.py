"""Weekly drift monitoring DAG leveraging Evidently."""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from churn.tasks import drift


def _notify(**kwargs):
    result = kwargs["ti"].xcom_pull(task_ids="run_drift_report")
    if result and result.get("drift_detected"):
        print("Drift detectado. Revisar reporte en MinIO key", result["s3_key"])
    else:
        print("Drift no detectado para esta semana.")


def build_dag() -> DAG:
    with DAG(
        dag_id="churn_drift_monitor_w",
        schedule="@weekly",
        start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
        catchup=False,
        tags=["mlops", "churn", "drift"],
    ) as dag:
        snapshot = PythonOperator(
            task_id="build_current_snapshot",
            python_callable=drift.generate_current_snapshot,
        )

        report = PythonOperator(
            task_id="run_drift_report",
            python_callable=drift.run_drift_report,
        )

        notify = PythonOperator(
            task_id="log_drift_result",
            python_callable=_notify,
        )

        snapshot >> report >> notify

    return dag


dag = build_dag()
