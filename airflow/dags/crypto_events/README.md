# Crypto Events DAG

## Purpose
Daily incremental pipeline that extracts the last five days of raw crypto transactions, builds a staging candidate table, validates the data with Great Expectations, and conditionally merges into `prod.bt_crypto_events` with logical deactivation of missing rows.

## Task Topology
1. **`extract_generate_history`** *(PythonOperator)* – fills `raw.bt_crypto_transaction_history` with deterministic Faker data for the rolling window using the DAG's logical date as the seed.
2. **`transform_build_candidate`** *(SQLExecuteQueryOperator)* – runs `sql/build_events_candidate.sql` to rebuild `staging.bt_crypto_events_candidate`.
3. **`qa_quality_checks` TaskGroup* – executes two GE checkpoints (`suite_raw_history`, `suite_staging_candidate`) via `dq.run_ge_validation`; failures capture rejects into `dqm.bt_crypto_events_rejects`.
4. **`load_merge_to_prod`** *(SQLExecuteQueryOperator)* – applies `sql/merge_to_prod.sql` to upsert current rows and deactivate missing ones.
5. **`notify_success`** – lightweight completion log hook.

## Configuration
- Tunable through Airflow Variables prefixed with `crypto_` (e.g., `crypto_postgres_conn_id`, `crypto_window_days`, `crypto_allowed_sites`). Defaults live in `config.py`.
- SQL templates reside under `sql/` and rely on `params` passed by the operators.
- GE assets are stored under `/opt/airflow/ge` inside the container.

## Development Notes
- To modify SQL logic, edit the corresponding file in `sql/` and keep parameters aligned with the operator definitions in `dag.py`.
- Utility helpers live in `utils/`:
  - `extract.py` handles Faker seeding.
  - `dq.py` wraps GE execution and reject insertion logic.
- Use the repo-level `Makefile` targets (`make reset`, `make dag-run`, etc.) to spin up the full stack and manually trigger the DAG end-to-end.
