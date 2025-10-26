"""Central configuration for the crypto events DAG with template helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Sequence

from jinja2 import Environment, FileSystemLoader

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
ALLOWED_SITES: Sequence[str] = tuple(
    site.strip() for site in get_var("crypto_allowed_sites", "ARGENTINA,BRASIL,MEXICO").split(",")
    if site.strip()
)
ALLOWED_CRYPTOS: Sequence[str] = tuple(
    crypto.strip() for crypto in get_var("crypto_allowed_cryptos", "BTC,ETH,USDC").split(",")
    if crypto.strip()
)
WINDOW_INTERVAL = f"{WINDOW_DAYS} days"
RANGE_INTERVAL = f"{max(WINDOW_DAYS - 1, 1)} day"

SQL_DIR = Path(__file__).parent / "sql"
SQL_ENV = Environment(loader=FileSystemLoader(str(SQL_DIR)), autoescape=False, trim_blocks=True, lstrip_blocks=True)


def render_sql(template_name: str, **params: Any) -> str:
    """Render a SQL template stored under the DAG's sql/ directory."""
    template = SQL_ENV.get_template(template_name)
    return template.render(**params)


def raw_validation_payload() -> Dict[str, Any]:
    return {
        "runtime_parameters": {
            "query": render_sql(
                "raw_validation.sql",
                window_interval=WINDOW_INTERVAL,
            )
        },
        "batch_identifiers": {"default_identifier_name": "raw_history_window"},
        "data_asset_name": "raw_history_last5",
    }


def staging_validation_payload() -> Dict[str, Any]:
    return {
        "runtime_parameters": {
            "query": render_sql(
                "staging_validation.sql",
                range_interval=RANGE_INTERVAL,
            )
        },
        "batch_identifiers": {"default_identifier_name": "staging_candidate_window"},
        "data_asset_name": "staging_candidate_last5",
    }
