"""Dataset definitions for churn data assets."""
from __future__ import annotations

from airflow.datasets import Dataset

from churn import config

CSV_DATASETS = [
    Dataset(f"file://{config.PAYMENTS_PATH}"),
    Dataset(f"file://{config.ACTIVE_PATH}"),
    Dataset(f"file://{config.DEMOGRAPHICS_PATH}"),
    Dataset(f"file://{config.FUNDS_PATH}"),
    Dataset(f"file://{config.MARKETPLACE_PATH}"),
]
