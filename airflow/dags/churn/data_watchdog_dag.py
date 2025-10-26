"""DAG that emits dataset updates when churn CSVs change."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

from churn import config
from churn.datasets import CSV_DATASETS

STATE_FILE = config.ARTIFACT_DIR / "source_hashes.json"
SOURCE_FILES = [
    config.PAYMENTS_PATH,
    config.ACTIVE_PATH,
    config.DEMOGRAPHICS_PATH,
    config.FUNDS_PATH,
    config.MARKETPLACE_PATH,
]


def _hash_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def detect_file_changes(**_) -> bool:
    for path in SOURCE_FILES:
        if not path.exists():
            raise FileNotFoundError(f"CSV requerido no encontrado: {path}")

    previous = {}
    if STATE_FILE.exists():
        previous = json.loads(STATE_FILE.read_text())

    current = {str(path): _hash_file(path) for path in SOURCE_FILES}
    has_changes = current != previous
    if has_changes or not STATE_FILE.exists():
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(current, indent=2))

    if has_changes:
        print("Detectados cambios en los CSV, se emitirá actualización de Dataset.")
    else:
        print("Sin cambios en CSV, no se emite Dataset.")
    return has_changes


def build_dag() -> DAG:
    with DAG(
        dag_id="churn_data_watchdog",
        schedule="@daily",
        start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
        catchup=False,
        default_args={"owner": "ds-mlops"},
        tags=["mlops", "churn", "datasets"],
    ) as dag:
        changes = ShortCircuitOperator(
            task_id="detect_csv_changes",
            python_callable=detect_file_changes,
        )

        emit = EmptyOperator(
            task_id="emit_dataset_update",
            outlets=CSV_DATASETS,
        )

        changes >> emit

    return dag


dag = build_dag()
