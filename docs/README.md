# MELI KPI Delivery - Crypto Events Pipeline

Reproducible Airflow + PostgreSQL + pgAdmin environment (Docker Compose) that ingests crypto transactions, validates them with Great Expectations, and publishes trusted events into `prod.BT_CRYPTO_EVENTS` using a validate-before-load pattern.

## Stack Overview
- **PostgreSQL 15**: hosts raw/staging/prod/dqm schemas + Airflow metadata. Bootstraps schemas/seeds automatically via `database/sql/init/*.sql` when the container is created.
- **pgAdmin 4**: browser UI for inspecting schemas (`http://localhost:8081`, user `admin@local.test`, pass `admin`).
- **Airflow 2.7 (CeleryExecutor)**: orchestrates the daily DAG `crypto_events_dag`, mounts `/opt/airflow/logs` from the repo, and ships remote logs to MinIO (S3-compatible).
- **Great Expectations 0.18**: Data Context lives under `/opt/airflow/ge`; stores validation results and Data Docs in MinIO (`ge-artifacts` bucket).
- **Redis 7**: Celery broker/backend.
- **MinIO**: provides S3 storage. Buckets `airflow-logs` (Airflow remote logging) and `ge-artifacts` (GE validations/data docs) are auto-created by the `minio-mc` sidecar.

## Prerequisites
- Docker & Docker Compose plugin.
- GNU Make optional (commands shown directly with Compose).

## First Run
From repo root you can rely on the provided `Makefile` (recommended):
```bash
make init      # airflow db init + admin user
make up        # start postgres, redis, minio, airflow, pgadmin
```
…or run the raw Compose commands if you prefer:
```bash
docker compose -f infrastructure/docker-compose.yml up airflow-init
docker compose -f infrastructure/docker-compose.yml up -d
```
Services:
- Airflow UI: `http://localhost:8080` (user `admin`, pass `admin`).
- MinIO Console: `http://localhost:9001` (user `minio`, pass `minio123`) to browse `airflow-logs` and `ge-artifacts`.
- pgAdmin UI: `http://localhost:8081` (user `admin@local.test`, pass `admin`). Add a server pointing to host `postgres`, port `5432`, db `crypto_db`, user/pass `airflow`.

> **Note:** Schemas, tables, and the initial raw history seed are loaded during the Postgres bootstrap step. The DAG assumes those objects already exist and only processes the rolling 5-day window going forward.

## DAG Execution Flow
1. **generate_fake_history_last_5_days** – `/opt/airflow/scripts/faker_seed.py` ensures each of the last five dates has at least `MIN_ROWS_PER_DAY` rows without touching historic data.
2. **build_events_candidate** – truncates `staging.BT_CRYPTO_EVENTS_CANDIDATE` and reloads the last 5 days, parsing text dates and calculating `PURCHASE_VALUE`.
3. **ge_validate_raw** – runs `suite_raw_history`; failures pipe bad records + reasons into `dqm.BT_CRYPTO_EVENTS_REJECTS` and abort the DAG.
4. **ge_validate_candidate** – runs `suite_staging_candidate` (datatype/math/uniqueness checks) over staging.
5. **merge_to_prod** – transactional upsert + logical deactivation for the rolling window, only if both validations passed.
6. **vacuum_analyze_prod** – optional housekeeping for `prod.BT_CRYPTO_EVENTS`.
7. **notify_success** – placeholder notifier (stdout log / future email hook).

The faker task plus the `WHERE` clauses ensure we only touch the rolling 5-day window. `prod.BT_CRYPTO_EVENTS.is_active` toggles to `false` whenever rows disappear from staging.

## Great Expectations
- Data Context: `airflow/ge/great_expectations.yml` (mounted to `/opt/airflow/ge`).
- Expectation suites:
  - `suite_raw_history` → schema + domain checks on `raw.BT_CRYPTO_TRANSACTION_HISTORY`.
  - `suite_staging_candidate` → datatype, math integrity, and uniqueness checks on staging.
- Checkpoint: `ge/checkpoints/crypto_checkpoint.yml`. Each Airflow validation task reuses the checkpoint with a targeted batch request.
- Validation artifacts + Data Docs: written to MinIO (`s3://ge-artifacts/validations` and `s3://ge-artifacts/data_docs`). Browse them from the MinIO console or any S3 client pointing to `http://localhost:9000`.

To regenerate Data Docs manually:
```bash
docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
  great_expectations --v3-api docs build
# Open http://localhost:9001/browser/ge-artifacts/data_docs/index.html via the MinIO console
```

## Fake Data Generator
`airflow/scripts/faker_seed.py` uses Faker + psycopg2 to guarantee a minimum number of rows for each day in the 5-day window (without mutating older data). Environment variables (already set in the DAG) control the Postgres connection and optional `MIN_ROWS_PER_DAY`.

## Useful Commands
- Tail scheduler logs: `docker compose -f infrastructure/docker-compose.yml logs -f airflow-scheduler`
- Trigger DAG manually: `make dag-run` (or `docker compose ... airflow dags trigger crypto_events_dag`)
- Inspect DAG run history: `make dag-status`
- Inspect schemas: `docker compose -f infrastructure/docker-compose.yml exec postgres psql -U airflow -d crypto_db -c "\dn"`
- Full reset (tear down, clear logs, re-up): `make reset`
- Inspect MinIO artifacts: browse `http://localhost:9001` and open the `airflow-logs` / `ge-artifacts` buckets.

## Tear Down
```bash
docker compose -f infrastructure/docker-compose.yml down -v
```
or simply run `make destroy`. This removes containers and volumes (including PostgreSQL data); rerun the init sequence to start fresh.

---

# Churn MLOps Extension (MLflow + Spark + Streamlit)

In addition to the crypto pipeline, the stack now ships an opinionated MLOps playground for a churn prediction use case. The goal is to cover data prep, training, model registry, inference UI, notebooks, and drift monitoring with Evidently.

## Extra Services

| Service | Purpose | How to access |
| --- | --- | --- |
| **MLflow 2.10** | Tracking + Model Registry backed by Postgres `mlflow_db`, artifacts in MinIO bucket `mlflow-artifacts`. | `http://localhost:5000` |
| **Spark master/worker** | Standalone cluster used by the training task (`spark://spark-master:7077`). | Spark UI `http://localhost:8082` |
| **JupyterLab** | Notebook environment with MinIO/MLflow credentials baked in for quick experiments. | `http://localhost:8888` (token `mlops`). Notebooks saved under `notebooks/`. |
| **Streamlit** | Simple UI in `app/main.py` to score users against the Production model (`models:/churn-model/Production`). | `http://localhost:8601` |

All containers share AWS-style credentials (`minio/minio123`) and the same MinIO endpoint (`http://minio:9000`), so artifacts/logs land in S3-compatible storage automatically.

## Data Layout

- Raw CSVs for the exercise live in `airflow/mlops/` (`PAYMENTS.csv`, `ACTIVE_USER.csv`, `DEMOGRAFICOS.csv`, `DINERO_CUENTA.csv`, `MARKETPLACE_DATA.csv`).
- DAG artifacts (cleaned datasets, feature matrix, drift reference snapshots, Evidently HTML reports) are written under `airflow/mlops/artifacts/` (ignored via `.gitignore`).
- The same directory is mounted read/write inside Airflow, Spark, MLflow, JupyterLab, and Streamlit at `/opt/mlops` so every stage sees identical files.

## `churn_training_dag` – ETL/ML Pipeline

Tasks:

1. **clean_raw_sources** – Validates the CSV inputs, converts date columns, and writes canonical parquet snapshots for payments + the user base.
2. **feature_engineering** – Builds the feature matrix by aggregating payments (counts, sums, discounts), joining usage + demographic attributes, deriving binary features, and computing the label (activity in the last 30 days). Also appends the snapshot to the drift reference history (max 8 weeks).
3. **train_spark_model** – Creates a Spark session against `spark://spark-master:7077`, trains a logistic regression classifier with Spark ML, splits train/test, and logs metrics + model artifacts to MLflow.
4. **evaluate_candidate** – Registers the new run in the MLflow Model Registry (`churn-model`), compares its ROC AUC with the current Production version, and stores the decision in XCom.
5. **decide_promotion** – BranchPythonOperator that routes to `promote_model` when the candidate beats Production (or when no Production exists) and otherwise to `skip_promotion`.
6. **promote_model / skip_promotion** – Transition the candidate to *Production* (archiving previous versions) or simply log that it was skipped.
7. **notify_success** – Friendly log to mark the workflow completion.

Artifacts:
- MLflow experiment `churn_retention` with metrics (`roc_auc`) and model versions.
- Local JSON summary under `airflow/mlops/artifacts/last_training.json` for debugging.
- Feature snapshots + reference history stored in the same artifacts folder.

## `churn_drift_monitor_w` – Evidently Drift Report

Weekly DAG that:

1. **build_current_snapshot** – Copies the latest feature matrix, adds light noise to simulate the most recent scoring week, and writes `current_snapshot.parquet`.
2. **run_drift_report** – Uses Evidently's `DataDriftPreset` to compare the current snapshot with the average of the last 8 weeks. Saves the HTML report locally and uploads it to MinIO at `s3://mlflow-artifacts/drift-reports/<date>.html`.
3. **log_drift_result** – Prints whether drift was detected. The task output also includes the MinIO key so operators can click through to the HTML report.

## Streamlit UI

- Code lives in `app/main.py` (mounted into the container). It loads the Production model from MLflow via `models:/churn-model/Production` and exposes a simple form to tweak the input features.
- If no Production model exists yet, it falls back to a deterministic heuristic and shows a warning banner.
- Hit `http://localhost:8601` after the stack is up. Credentials are inherited from the container env (no manual configuration needed).

## JupyterLab

- Built from `jupyter/scipy-notebook` plus the same MLflow/PySpark dependencies as Airflow. The default token is `mlops` (configurable in `infrastructure/jupyterlab/Dockerfile`).
- Automatically mounts `../notebooks` as the working directory and `/opt/mlops` to read the raw CSVs.
- Environment variables (`MLFLOW_TRACKING_URI`, `MLFLOW_S3_ENDPOINT_URL`, AWS creds) are injected so you can log experiments seamlessly.

## End-to-End Workflow

1. `make reset` 👈 tears down everything, recreates volumes/logs, bootstraps DB/schema, and starts the stack with the new services.
2. Enable & trigger `churn_training_dag` from Airflow. After it succeeds, check MLflow to see the experiment + registered model. The Streamlit UI should now render predictions from the Production version.
3. Trigger or schedule `churn_drift_monitor_w` to produce the Evidently report. Verify the HTML inside MinIO's `mlflow-artifacts/drift-reports/` path.
4. Use JupyterLab/notebooks as needed for exploratory work—the same data + credentials are available without leaving the containerized environment.

Everything (Airflow, Spark, MLflow, Streamlit, Jupyter) continues to be orchestrated via the single Compose file/Makefile, so existing commands (`make up`, `make down`, `make dag-run`, etc.) still apply.
