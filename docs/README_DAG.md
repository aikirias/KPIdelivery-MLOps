# crypto_events_dag

This document focuses on how the Airflow DAG works, how to operate it, and ways to validate its behavior end-to-end.

## Purpose
`crypto_events_dag` orchestrates a validate-before-load pipeline for MELI's crypto transactions. It extracts the last five days of history, stages clean candidates, runs Great Expectations suites, and performs an upsert + logical delete into `prod.BT_CRYPTO_EVENTS`. Any invalid rows are diverted to `dqm.BT_CRYPTO_EVENTS_REJECTS` with a reason code.

## Task Graph
1. **generate_fake_history_last_5_days** – Calls `python /opt/airflow/scripts/faker_seed.py` to ensure each of the last five dates has fresh rows (forward-only inserts).
2. **build_events_candidate** – Truncates `staging.BT_CRYPTO_EVENTS_CANDIDATE`, converts text dates with a regex guard, calculates `PURCHASE_VALUE`, and inserts only the rolling 5-day window.
3. **ge_validate_raw** – Runs `suite_raw_history` from the shared checkpoint. Missing columns, bad enums/date formats, or negative numbers fail the task.
4. **ge_validate_candidate** – Runs `suite_staging_candidate`. Verifies `DATE` typing, math integrity, positive values, and uniqueness.
5. **merge_to_prod** – Transactional upsert with `ON CONFLICT` plus logical deactivation for rows that vanished from staging during the same window.
6. **vacuum_analyze_prod** – Optional VACUUM ANALYZE for the prod table to keep stats fresh.
7. **notify_success** – Placeholder notifier (stdout log) signaling the daily run completed.

Any Great Expectations failure stops the DAG before touching prod. The helper functions `capture_raw_rejects` / `capture_staging_rejects` compute and store the offending rows (site/user/date/crypto plus reason) for auditability.

## Running the DAG
1. Start the stack (see main README) so Airflow, Postgres, and Redis are live.
2. Enable `crypto_events_dag` in the Airflow UI. It has a daily schedule but you can trigger ad hoc:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
     airflow dags trigger crypto_events_dag
   ```
3. Track progress in the UI or stream logs:
   ```bash
   docker compose -f infrastructure/docker-compose.yml logs -f airflow-scheduler
   docker compose -f infrastructure/docker-compose.yml logs -f airflow-worker
   ```

## Verifying Behavior
- **Schemas & seeds**: In pgAdmin or psql, inspect `raw.BT_CRYPTO_TRANSACTION_HISTORY` to confirm the seed rows plus recent Faker output.
- **Staging refresh**: After `build_events_candidate`, query `staging.BT_CRYPTO_EVENTS_CANDIDATE` to see only the last five days with parsed `DATE` values and computed `purchase_value`.
- **GE outcomes**:
  - Validation summaries are inserted into `dqm.DQ_RUNS` (JSON details column).
  - Row-level rejects land in `dqm.BT_CRYPTO_EVENTS_REJECTS`. Filter by `suite_name` to see which suite failed and why.
  - Validation artifacts + Data Docs publish to MinIO (`ge-artifacts` bucket). Build docs if needed:
    ```bash
    docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
      great_expectations --v3-api docs build
    ```
    Then browse `http://localhost:9001/browser/ge-artifacts/data_docs` in the MinIO console.
- **Prod merge**: Query `prod.BT_CRYPTO_EVENTS` to verify upserts and `is_active` toggling. For example, rows removed from staging in the last window should flip to `false`.

## Failure Recovery
If either validation fails, fix the offending raw data (or adjust Faker settings), optionally delete quarantine records for clarity, and re-trigger the DAG. Because the merge step never ran, prod remains unchanged until both suites pass.
