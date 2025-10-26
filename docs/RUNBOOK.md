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
1. Start the stack (see main README) so Airflow, Postgres, Redis, MinIO, etc. are live (`make init && make up`).
2. Enable `crypto_events_dag` in the Airflow UI. It has a daily schedule but you can trigger ad hoc:
   ```bash
   make dag-run
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

### Quick Troubleshooting

| Symptom | Checks | Resolution |
| --- | --- | --- |
| DAG run stuck on GE | Look at `dqm.BT_CRYPTO_EVENTS_REJECTS` for rule violations and inspect logs under `airflow/logs/dag_id=crypto_events_dag/...` | Fix raw data (or rerun Faker) and re-trigger DAG |
| No logs in UI | Host folder `airflow/logs` missing or read-only | `make clean` (ensures 777 perms) and `make up` |
| Prod table unchanged | GE task failed or DAG aborted before merge | Rerun once validations succeed; merge is transactional so prod remains safe |

## Failure Recovery
If either validation fails, fix the offending raw data (or adjust Faker settings), optionally delete quarantine records for clarity, and re-trigger the DAG (`make dag-run`). Because the merge step never ran, prod remains unchanged until both suites pass.

---

# churn_training_dag

Spark-based churn pipeline that reads `airflow/mlops/*.csv`, prepares features, trains a logistic regression model, and manages the MLflow registry lifecycle. The DAG is now scheduled via Airflow Datasets tied to the source CSVs, so it only runs when `churn_data_watchdog` detects a real data change.

## Task Graph
1. **clean_raw_sources** – Validates + normalizes the CSV inputs, writes canonical parquet snapshots for payments and the user base.
2. **feature_engineering** – Aggregates payments (counts, sums, discounts), joins app usage + demographics + funds/ML activity, derives binary features, computes the target label, and updates the drift reference history (last 8 weekly snapshots).
3. **train_spark_model** – Creates a Spark session against `spark://spark-master:7077`, runs a small grid search over `regParam` / `elasticNetParam` for logistic regression, evaluates each candidate on a holdout split, and logs metrics + params + explainability artifacts (SHAP plot + mean absolute values) to MLflow (`churn_retention` experiment). The task also records Evidently’s latest `drift_share` + `drift_detected` metrics so each run links model quality with feature stability. Artifacts land in MinIO bucket `mlflow-artifacts`.
4. **evaluate_candidate** – Registers the run as a new model version inside MLflow (`churn-model`), compares its ROC AUC with the current Production version, and stores `{promote, candidate_version, metrics}` in XCom.
5. **decide_promotion** – Branches to `promote_model` or `skip_promotion` depending on the evaluation result.
6. **promote_model / skip_promotion** – Transition the new version to Production (archiving old versions) or just log that it was skipped. Promotion now requires the candidate ROC AUC to beat the current Production model by at least `CHURN_PROMOTION_MIN_DELTA` (default 0.005) to avoid flip-flopping on noise.
7. **notify_success** – Final log hook.

## Key Files / Paths
- Raw CSVs: `airflow/mlops/*.csv` (mounted at `/opt/mlops` inside Airflow and Spark containers).
- Cleaned snapshots: `airflow/mlops/artifacts/payments_clean.parquet` + `user_base.parquet`.
- Feature matrix: `airflow/mlops/artifacts/churn_features.parquet`.
- Reference history (up to 8 snapshots): `airflow/mlops/artifacts/reference_history.parquet`.
- MLflow experiment: `churn_retention` (tracking URI `http://mlflow:5000`).
- Model registry: `churn-model` (Production stage consumed by the Streamlit app).

## Verifying Behavior
- After a run, browse `http://localhost:5000` to inspect the new run/metrics and confirm whether the candidate was promoted to Production.
- Spark UI (`http://localhost:8082`) shows the submitted application while `train_spark_model` is running.
- The Streamlit app (`http://localhost:8601`) loads the Production model once it exists; the UI displays a warning if only the heuristic fallback is available.

---

# churn_data_watchdog

Daily DAG that hashes every churn CSV and emits all Dataset outlets when any file changes. `churn_training_dag` lists those Datasets in its schedule, so it automatically retrains after data refreshes and stays idle otherwise. The state file lives in `airflow/mlops/artifacts/source_hashes.json`.

## Tasks
1. **detect_csv_changes** (`ShortCircuitOperator`) – verifies each CSV exists under `/opt/mlops`, computes MD5 hashes, compares them with the previous snapshot stored in `source_hashes.json`, and short-circuits if nothing changed.
2. **emit_dataset_update** (`EmptyOperator`) – has the five churn datasets (`file:///opt/mlops/*.csv`) in its `outlets`. When it runs, the scheduler records one event per dataset, immediately triggering `churn_training_dag`.

## Usage
- First run: delete `airflow/mlops/artifacts/source_hashes.json` (or leave it missing), then trigger the DAG. It will detect “changes” because there is no baseline and emit events.
- Ongoing: allow the daily schedule to run; if no files changed, it exits quickly and no training occurs. If you manually edit a CSV (e.g., via JupyterLab), rerun the watchdog to kick off retraining.
- Debugging: query `dataset_event` table in the Airflow metadata DB to verify events were created, or inspect the DAG run log (look for “Detectados cambios en los CSV…”).

---

# churn_drift_monitor_w

Weekly Evidently workflow that compares the latest scoring snapshot against the rolling 8-week reference.

## Task Graph
1. **build_current_snapshot** – Copies `churn_features.parquet`, injects light noise to simulate the current week, and writes `current_snapshot.parquet`.
2. **run_drift_report** – Executes Evidently's `DataDriftPreset`, saves an HTML report under `airflow/mlops/artifacts/drift_reports/`, and uploads it to MinIO bucket `mlflow-artifacts` (key `drift-reports/<date>.html`). Returns `{drift_detected, report_path, s3_key}` via XCom.
3. **log_drift_result** – Prints whether drift was detected and surfaces the S3 key in the task log for quick access.

## Notes
- Ensure `churn_training_dag` has run at least once so the drift reference history exists.
- The Evidently HTML can be downloaded from the MinIO console (`http://localhost:9001/browser/mlflow-artifacts/drift-reports`).
- Extend the DAG easily by adding notification hooks (email, Slack, etc.) reacting to the `drift_detected` flag.

---

## Operational Checklist

1. **Stack up** – `make reset` for a fresh environment or `make up` for reuse. Confirm all containers report `Up` via `docker compose … ps`.
2. **Watchdog** – Run/verify `churn_data_watchdog` so dataset events emit. Without events, `churn_training_dag` will stay idle.
3. **Churn training** – Follow the dataset-triggered run to completion; MLflow should show `roc_auc`, `recent_drift_share`, SHAP artifacts, and a new/updated `churn-model` version.
4. **Crypto pipeline** – `make dag-run` (or let schedule run) and validate rejects/staging/prod tables plus GE Data Docs in MinIO.
5. **Drift monitor** – Ensure `churn_drift_monitor_w` completes weekly (or trigger manually) so a fresh Evidently HTML lands in `mlflow-artifacts/drift-reports/`.
6. **Dashboards** – Visit MLflow, MinIO, Streamlit, and pgAdmin to demonstrate outputs and allow reviewers to self-serve.

## Useful References

- **Architecture + churn deep dive**: [`airflow/dags/churn/CHURN_PLAYBOOK.md`](../airflow/dags/churn/CHURN_PLAYBOOK.md)
- **Crypto DAG playbook**: [`airflow/dags/crypto_events/CRYPTO_PLAYBOOK.md`](../airflow/dags/crypto_events/CRYPTO_PLAYBOOK.md)
- **Top-level quickstart / credentials**: [`README.md`](../README.md#quickstart-happy-path)
- **Make targets**: `Makefile`
- **Airflow dataset metadata**: Postgres `dataset` and `dataset_event` tables (connect via `docker compose … exec postgres psql -U airflow -d crypto_db`)
