# Churn MLOps Package

This folder contains the Airflow DAGs, configs, and helper tasks that power the churn prediction pipeline. It implements the full ML lifecycle:

1. **Data preparation**: `clean_raw_sources` and `feature_engineering` load the CSVs under `/opt/mlops`, consolidate the user base, build aggregations/labels, and persist clean parquet snapshots. Artifacts live under `airflow/mlops/artifacts/`.
2. **Model training** (`train_spark_model`): spins up a Spark session (`spark://spark-master:7077`), runs a lightweight hyperparameter search over `regParam`/`elasticNetParam`, evaluates each candidate on a holdout split, and keeps the best run. It now also logs Evidently’s `drift_share`/`drift_detected` metrics alongside ROC AUC so each MLflow run captures both model quality and feature stability. Metrics, params, dataset snapshots, and SHAP explainability artifacts (PNG + mean absolute values) are logged to MLflow (`churn_retention` experiment) so Streamlit/Jupyter can surface them instantly. Models get registered under `churn-model`.
3. **Evaluation & registry**: `evaluate_candidate` compares the candidate ROC AUC vs. the current Production version. `decide_promotion` branches to `promote_model` (if better) or `skip_promotion`. Promotion updates the MLflow Model Registry so Streamlit/Jupyter consumers always hit the latest Production version.
4. **Drift monitoring** (`churn_drift_monitor_w`): weekly DAG that snapshots the latest features, runs Evidently’s data drift preset against the last 8 snapshots, uploads the HTML report to `s3://mlflow-artifacts/drift-reports/<date>.html`, and logs whether drift was detected.
5. **Dataset guard** (`churn_data_watchdog`): hashes each CSV under `/opt/mlops` daily; if any file changes it emits Airflow Datasets so `churn_training_dag` re-runs automatically. No change → no retraining.

## Key Files

- `config.py`: centralizes paths (`/opt/mlops`), MLflow vars, feature columns, and MinIO credentials.
- `dag.py`: training DAG definition (`churn_training_dag`), now scheduled via Dataset updates instead of a static cron.
- `drift_dag.py`: weekly drift DAG (`churn_drift_monitor_w`).
- `data_watchdog_dag.py`: emits Dataset updates when any CSV changes.
- `datasets.py`: Dataset definitions referenced by both the watchdog and training DAG.
- `tasks/data_prep.py`: all the cleaning + feature engineering code. Outputs `churn_features.parquet` and rolling reference history.
- `tasks/modeling.py`: Spark ML training, MLflow logging (metrics + drift info), evaluation, and registry promotion logic with a configurable promotion margin.
- `tasks/drift.py`: Evidently snapshot generation and report upload utilities.

## Running the pipeline

1. Ensure the stack is booted via `make reset && make up` (or just `make up` if services are already built). Spark, MLflow, MinIO, and Airflow must be running.
2. Trigger the training DAG from Airflow (or just let Dataset updates kick it off automatically):
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
     airflow dags trigger churn_training_dag
   ```
   Watch logs in the UI or via `docker compose ... logs -f airflow-worker`.
3. After a successful run, check MLflow (`http://localhost:5000`) for the new run + registered version. Streamlit (`http://localhost:8601`) will now consume the Production model and display both ROC AUC + drift metrics.
4. Trigger the drift DAG weekly (or manually) to generate the Evidently report:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
     airflow dags trigger churn_drift_monitor_w
   ```
   Reports live in MinIO bucket `mlflow-artifacts` at `drift-reports/<date>.html`.

## MLflow Metrics & Artifacts

| Artifact / Metric | Where to find it | Logged by | Why it matters |
| --- | --- | --- | --- |
| `roc_auc` metric | MLflow run → Metrics tab | `train_spark_model` | Primary model selection metric |
| `recent_drift_share`, `recent_drift_detected` metrics | MLflow run → Metrics | `train_spark_model` (via Evidently helper) | Correlate performance with feature stability |
| `dataset/churn_features.parquet` artifact | MLflow run → Artifacts | `train_spark_model` | Exact training dataset snapshot |
| `explainability/shap_summary.png` | MLflow run → Artifacts (download) | `train_spark_model` | Visual SHAP summary served in Streamlit |
| `explainability/mean_abs_shap.json` | MLflow run → Artifacts | `train_spark_model` | Tabular feature importance for Streamlit/Jupyter |
| Drift report HTML | MinIO `mlflow-artifacts/drift-reports/<date>.html` | `churn_drift_monitor_w` | Weekly data drift diagnosis |

Streamlit pulls the Production run’s SHAP artifacts automatically; MLflow keeps the full history for audits.

## Data Inputs

Raw CSVs are mounted under `airflow/mlops/` and baked into the containers at `/opt/mlops`. On each run:
- Payments are cleaned (`PAYMENTS.csv` → `payments_clean.parquet`).
- User base is constructed from `ACTIVE_USER.csv`, `DEMOGRAFICOS.csv`, `DINERO_CUENTA.csv`, `MARKETPLACE_DATA.csv`.
- Features + label (binary churn/no churn) are derived for every user; history is retained for drift.

## Extending

- Edit `config.FEATURE_COLUMNS` / engineering logic to add features.
- Swap the Spark model in `tasks/modeling.py` (e.g., GradientBoostedTrees or XGBoost via Spark ML) or adjust the evaluation metric/tagging logic.
- Change `config.PROMOTION_MIN_DELTA` (env var `CHURN_PROMOTION_MIN_DELTA`) to tighten/relax the promotion guard or replace it with a bootstrap test.
- Wire additional notification hooks (Slack, email) in `decide_promotion`/`promote_model` or the drift DAG.

## Troubleshooting & FAQs

| Question | Answer |
| --- | --- |
| **Why didn’t `churn_training_dag` run after I edited a CSV?** | The DAG is dataset-triggered. Ensure `churn_data_watchdog` executed after the edit. Remove `airflow/mlops/artifacts/source_hashes.json` or run `airflow dags trigger churn_data_watchdog` to force a new hash snapshot. Inspect the Airflow metadata DB (`dataset_event` table) to confirm events were emitted. |
| **Training run failed during Spark HPO** | Check `airflow/logs/dag_id=churn_training_dag/.../train_spark_model/` and the Spark UI (`http://localhost:8082`). Common causes: Spark worker not up, MinIO credentials missing, or not enough data variety. Fix and rerun the failed task from the Airflow UI. |
| **Promotion skipped even though metrics look similar** | We require the candidate to beat Production by at least `CHURN_PROMOTION_MIN_DELTA` (default 0.005). Adjust the env var or improve the model before expecting promotion. |
| **Streamlit still shows “Heurística local”** | No Production model exists yet. Trigger the churn DAG, ensure promotion succeeded, then refresh Streamlit. |
| **Where are the watchdog hashes stored?** | `airflow/mlops/artifacts/source_hashes.json`. Remove it to force the watchdog to treat the next run as “changed”. |

Everything runs entirely inside Docker: Spark master/worker, MLflow, Airflow (Celery executor), Streamlit UI, and JupyterLab. Use the `Makefile` targets to reset, rebuild, and trigger DAGs quickly during development.
