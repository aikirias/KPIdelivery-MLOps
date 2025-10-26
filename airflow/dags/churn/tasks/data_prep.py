"""Data preparation helpers for the churn DAG."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from churn import config


def _ensure_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def clean_sources(**context: Any) -> str:
    """Load & clean the raw CSV assets."""
    for path in (
        config.PAYMENTS_PATH,
        config.ACTIVE_PATH,
        config.DEMOGRAPHICS_PATH,
        config.FUNDS_PATH,
        config.MARKETPLACE_PATH,
    ):
        if not path.exists():
            raise FileNotFoundError(f"Missing required CSV: {path}")

    payments = pd.read_csv(config.PAYMENTS_PATH)
    payments["FECHA"] = _ensure_datetime(payments["FECHA"])
    payments["DESCUENTO"] = payments["DESCUENTO"].fillna(0.0)

    active = pd.read_csv(config.ACTIVE_PATH)
    active["last_login_mp_date_1"] = _ensure_datetime(active["last_login_mp_date_1"])

    demographics = pd.read_csv(config.DEMOGRAPHICS_PATH)
    funds = pd.read_csv(config.FUNDS_PATH)
    marketplace = pd.read_csv(config.MARKETPLACE_PATH)
    marketplace["RECENCY_ML"] = _ensure_datetime(marketplace["RECENCY_ML"])

    base = (
        demographics.merge(active, on="CUS_CUST_ID_BUY", how="left")
        .merge(funds, on="CUS_CUST_ID_BUY", how="left")
        .merge(marketplace, on="CUS_CUST_ID_BUY", how="left")
    )
    base.rename(
        columns={
            "PLATA_CUENTA_1": "saldo_mes_actual",
            "PLATA_CUENTA_2": "saldo_mes_previo",
            "SPENT_ML": "spent_ml",
            "FREQUENCY_ML": "frequency_ml",
        },
        inplace=True,
    )

    config.PAYMENTS_CLEAN_PATH.parent.mkdir(parents=True, exist_ok=True)
    payments.to_parquet(config.PAYMENTS_CLEAN_PATH, index=False)
    base.to_parquet(config.USER_BASE_PATH, index=False)
    return str(config.USER_BASE_PATH)


def build_feature_matrix(**context: Any) -> str:
    """Feature engineering (aggregations + label)."""
    logical_date: datetime = context["logical_date"]
    snapshot_date = logical_date.date()
    payments = pd.read_parquet(config.PAYMENTS_CLEAN_PATH)
    base = pd.read_parquet(config.USER_BASE_PATH)

    payments["FECHA"] = pd.to_datetime(payments["FECHA"])
    cutoff_60 = pd.Timestamp(snapshot_date) - pd.Timedelta(days=60)
    cutoff_30 = pd.Timestamp(snapshot_date) - pd.Timedelta(days=30)

    payments_last_60 = (
        payments[payments["FECHA"] >= cutoff_60]
        .groupby("CUS_CUST_ID_BUY")
        .size()
        .rename("payments_last_60d")
    )
    payments_sum = (
        payments.groupby("CUS_CUST_ID_BUY")["SPENT"].sum().rename("payments_sum")
    )
    avg_discount = (
        payments.groupby("CUS_CUST_ID_BUY")["DESCUENTO"].mean().rename("avg_discount")
    )
    label = (
        payments[payments["FECHA"] >= cutoff_30]
        .groupby("CUS_CUST_ID_BUY")
        .size()
        .rename("label_indicator")
    )

    features = (
        base.set_index("CUS_CUST_ID_BUY")
        .join([payments_last_60, payments_sum, avg_discount, label])
        .fillna(
            {
                "payments_last_60d": 0,
                "payments_sum": 0.0,
                "avg_discount": 0.0,
                "label_indicator": 0,
            }
        )
    )

    features["label"] = np.where(features["label_indicator"] > 0, 1, 0)
    if features["label"].nunique() < 2:
        threshold = features["payments_sum"].median()
        features["label"] = np.where(features["payments_sum"] > threshold, 1, 0)
    features["gender_is_female"] = (features["GENDER"].str.upper() == "F").astype(int)
    features["has_investment"] = features["INVERSION"].isin(["Investing", "Eligible"]).astype(int)
    recency_ts = pd.to_datetime(features["RECENCY_ML"], errors="coerce")
    features["recency_ml_days"] = (
        pd.Timestamp(snapshot_date) - recency_ts
    ).dt.days.fillna(999)

    features.rename(
        columns={
            "MAU_MP_1": "mau_mp_1",
            "MAU_ML_1": "mau_ml_1",
            "MAU_ML_2": "mau_ml_2",
        },
        inplace=True,
    )
    features["mau_mp_1"] = features["mau_mp_1"].fillna(0)
    features["mau_ml_1"] = features["mau_ml_1"].fillna(0)
    features["mau_ml_2"] = features["mau_ml_2"].fillna(0)
    features["saldo_mes_actual"] = features["saldo_mes_actual"].fillna(0.0)
    features["saldo_mes_previo"] = features["saldo_mes_previo"].fillna(0.0)
    features["spent_ml"] = features["spent_ml"].fillna(0.0)
    features["frequency_ml"] = features["frequency_ml"].fillna(0)

    ordered_cols = [
        "CUS_CUST_ID_BUY",
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
        "label",
    ]

    features = features.reset_index()[ordered_cols]
    numeric_cols = [col for col in ordered_cols if col not in ("CUS_CUST_ID_BUY", "label")]
    features[numeric_cols] = features[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    features["snapshot_date"] = snapshot_date.isoformat()

    config.FEATURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(config.FEATURES_PATH, index=False)

    history = pd.DataFrame()
    if config.REFERENCE_HISTORY_PATH.exists():
        history = pd.read_parquet(config.REFERENCE_HISTORY_PATH)
    history = pd.concat([history, features], ignore_index=True)
    recent_snapshots = (
        history["snapshot_date"].drop_duplicates().sort_values().tolist()
    )
    if len(recent_snapshots) > 8:
        keep = set(recent_snapshots[-8:])
        history = history[history["snapshot_date"].isin(keep)]
    history.to_parquet(config.REFERENCE_HISTORY_PATH, index=False)

    return str(config.FEATURES_PATH)
