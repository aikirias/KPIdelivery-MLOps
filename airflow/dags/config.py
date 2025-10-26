"""Central configuration for the crypto events DAG.

Values can be overridden via Airflow Variables (e.g. crypto_window_days).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

try:
    from airflow.models import Variable
except Exception:  # pragma: no cover - outside Airflow runtime
    Variable = None  # type: ignore


def get_var(key: str, default: Any) -> Any:
    """Fetch a Variable if available, otherwise fallback to default."""
    if Variable is None:
        return default
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        return default


POSTGRES_CONN_ID = get_var("crypto_postgres_conn_id", "crypto_postgres")
CHECKPOINT_NAME = get_var("crypto_checkpoint_name", "crypto_checkpoint")
GE_ROOT = Path(get_var("crypto_ge_root", "/opt/airflow/ge"))
RAW_DATE_REGEX = get_var(
    "crypto_raw_date_regex",
    r"^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/20[0-9]{2}$",
)
WINDOW_DAYS = int(get_var("crypto_window_days", 5))
MIN_ROWS_PER_DAY = int(get_var("crypto_min_rows_per_day", 4))
ALLOWED_SITES = tuple(get_var("crypto_allowed_sites", "ARGENTINA,BRASIL,MEXICO").split(","))
ALLOWED_CRYPTOS = tuple(get_var("crypto_allowed_cryptos", "BTC,ETH,USDC").split(","))
WINDOW_INTERVAL = f"{WINDOW_DAYS} days"
RANGE_INTERVAL = f"{max(WINDOW_DAYS - 1, 1)} day"


def raw_validation_payload() -> Dict[str, Any]:
    return {
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


def staging_validation_payload() -> Dict[str, Any]:
    return {
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
