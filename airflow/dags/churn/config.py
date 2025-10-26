from __future__ import annotations

import os
from pathlib import Path

DATA_ROOT = Path(os.getenv("CHURN_DATA_ROOT", "/opt/airflow/mlops"))
ARTIFACT_DIR = DATA_ROOT / "artifacts"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

PAYMENTS_PATH = DATA_ROOT / "PAYMENTS.csv"
ACTIVE_PATH = DATA_ROOT / "ACTIVE_USER.csv"
DEMOGRAPHICS_PATH = DATA_ROOT / "DEMOGRAFICOS.csv"
FUNDS_PATH = DATA_ROOT / "DINERO_CUENTA.csv"
MARKETPLACE_PATH = DATA_ROOT / "MARKETPLACE_DATA.csv"

PAYMENTS_CLEAN_PATH = ARTIFACT_DIR / "payments_clean.parquet"
USER_BASE_PATH = ARTIFACT_DIR / "user_base.parquet"
FEATURES_PATH = ARTIFACT_DIR / "churn_features.parquet"
REFERENCE_HISTORY_PATH = ARTIFACT_DIR / "reference_history.parquet"
CURRENT_SNAPSHOT_PATH = ARTIFACT_DIR / "current_snapshot.parquet"
DRIFT_REPORTS_DIR = ARTIFACT_DIR / "drift_reports"
DRIFT_REPORTS_DIR.mkdir(exist_ok=True, parents=True)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("CHURN_MODEL_NAME", "churn-model")
METRIC_KEY = "roc_auc"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
MINIO_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
MLFLOW_BUCKET = "mlflow-artifacts"

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")

FEATURE_COLUMNS = [
    "payments_last_60d",
    "payments_sum",
    "avg_discount",
    "mau_mp_1",
    "mau_ml_1",
    "mau_ml_2",
    "saldo_mes_actual",
    "saldo_mes_previo",
    "spent_ml",
    "frequency_ml",
    "has_investment",
    "gender_is_female",
    "recency_ml_days",
]
