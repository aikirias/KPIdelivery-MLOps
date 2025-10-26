"""Evidently drift monitoring helpers."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import boto3
import numpy as np
import pandas as pd
from botocore.client import Config
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report

from churn import config


def generate_current_snapshot(**context: Any) -> str:
    logical_date: datetime = context["logical_date"]
    features = pd.read_parquet(config.FEATURES_PATH)
    rng = np.random.default_rng(int(logical_date.timestamp()))
    current = features.copy()
    for col in config.FEATURE_COLUMNS:
        current[col] = current[col].astype(float) * (1 + rng.normal(0, 0.02, size=len(current)))
    current["snapshot_date"] = logical_date.date().isoformat()
    current.to_parquet(config.CURRENT_SNAPSHOT_PATH, index=False)
    return str(config.CURRENT_SNAPSHOT_PATH)


def run_drift_report(**context: Any) -> Dict[str, Any]:
    execution_date: datetime = context["logical_date"]
    if not config.REFERENCE_HISTORY_PATH.exists():
        raise FileNotFoundError("Reference history not found. Run the training DAG first.")

    reference = pd.read_parquet(config.REFERENCE_HISTORY_PATH)
    current = pd.read_parquet(config.CURRENT_SNAPSHOT_PATH)

    reference_dates = sorted(reference["snapshot_date"].unique())
    keep_dates = reference_dates[-8:]
    reference_df = reference[reference["snapshot_date"].isin(keep_dates)]
    current_df = current.copy()

    report = Report(metrics=[DataDriftPreset()])
    report.run(
        reference_data=reference_df[config.FEATURE_COLUMNS],
        current_data=current_df[config.FEATURE_COLUMNS],
    )

    report_path = config.DRIFT_REPORTS_DIR / f"drift_report_{execution_date.date()}.html"
    report.save_html(str(report_path))
    summary = report.as_dict()
    drift_detected = summary["metrics"][0]["result"].get("drift_detected", False)

    session = boto3.session.Session()
    s3_client = session.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    s3_key = f"drift-reports/{execution_date.date()}.html"
    with open(report_path, "rb") as fp:
        s3_client.put_object(
            Bucket=config.MLFLOW_BUCKET,
            Key=s3_key,
            Body=fp.read(),
            ContentType="text/html",
        )

    if drift_detected:
        print("⚠️ Evidently detectó drift en las features de scoring.")
    else:
        print("No se detectó drift para la semana analizada.")

    return {"drift_detected": drift_detected, "report": str(report_path), "s3_key": s3_key}
