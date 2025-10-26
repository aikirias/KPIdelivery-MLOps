# KPIdelivery-MLOps

Repo organized by functional layers:

- `infrastructure/`: Docker Compose stack + one Dockerfile per service (Airflow, Postgres, Redis, MinIO, pgAdmin, MLflow, Spark master/worker, JupyterLab, Streamlit).
- `airflow/`: DAG packages (`crypto_events`, `churn`), scripts, and the Great Expectations context mounted inside the Airflow containers.
- `airflow/mlops/`: source CSVs for the churn exercise + derived artifacts (ignored via `.gitignore`).
- `database/`: SQL DDL/seed scripts consumed by Postgres bootstrap (crypto schemas + MLflow DB provisioning).
- `docs/`: runbooks (`docs/README.md` for the stack, `docs/RUNBOOK.md` for DAG-level behavior).
- `app/`: Streamlit UI that reads the latest Production model from MLflow for manual scoring.
- `notebooks/`: mounted into the JupyterLab container for interactive exploration.
- `Makefile`: convenience targets (`make init`, `make up`, `make down`, `make reset`, `make dag-run`, etc.) wrapping the Compose workflow.

## Quickstart (Happy Path)

1. **Prereqs** – Docker Desktop (or engine) + Compose plugin, `make`, and ~10 GB free disk. No additional Python setup is required; everything runs in containers.
2. **Clone & configure** – `git clone <repo>`; the repo already ships with `.env` (merged credentials for Airflow/MinIO/MLflow/Spark). Adjust ports/paths there if needed.
3. **Cold start** – From repo root run `make reset`. This performs a full teardown, reinitializes Airflow metadata, recreates Postgres + MinIO volumes, and boots every service. Wait until `docker compose -f infrastructure/docker-compose.yml ps` shows `Up` for all containers (webserver, scheduler, worker, Spark, MLflow, Streamlit, MinIO, pgAdmin, Redis, Postgres, JupyterLab).
4. **Log in** – Default credentials:
   - Airflow UI: `http://localhost:8080` (`admin` / `admin`)
   - pgAdmin: `http://localhost:8081` (`admin@local.test` / `admin`)
   - MinIO Console: `http://localhost:9001` (`minio` / `minio123`)
   - MLflow UI: `http://localhost:5000` (no auth)
   - Streamlit: `http://localhost:8601`
   - JupyterLab: `http://localhost:8888` (token `mlops`)
5. **First execution flow** (ten minutes end-to-end):
   1. Trigger the watchdog DAG so Airflow registers dataset events for the churn inputs:
      ```bash
      docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
        airflow dags trigger churn_data_watchdog
      ```
      When this detects any change (or simply because it is the first run/output file missing), it emits Dataset events and `churn_training_dag` launches automatically. Follow its progress from the UI (`DAGs > churn_training_dag > Graph`).
   2. Trigger the crypto pipeline once:
      ```bash
      make dag-run
      ```
      This runs `crypto_events_dag` manually for the latest 5‑day window using Faker-generated data.
   3. The weekly drift DAG (`churn_drift_monitor_w`) runs on schedule as soon as the scheduler sees it; you can force a run with the UI or `airflow dags trigger churn_drift_monitor_w`.
6. **Verify outputs**
   - MLflow (`http://localhost:5000`) should now show at least one run in experiment `churn_retention` with metrics `roc_auc`, `recent_drift_share`, `recent_drift_detected`, and artifacts under `dataset/` + `explainability/`.
   - MinIO bucket `mlflow-artifacts` contains drift reports (HTML) under `drift-reports/`.
   - Streamlit displays the latest Production model along with SHAP plots once a model is promoted.
   - Postgres schemas (`raw`, `staging`, `prod`, `dqm`) contain the crypto tables; `dqm.BT_CRYPTO_EVENTS_REJECTS` stores validation failures if any.

That’s it—subsequent cycles usually only require `make up` (to start containers) and triggering whichever DAG you want to demo. Use `make reset` whenever you need a pristine environment.

## Why two pipelines?

| Workflow | Purpose | Highlights |
| --- | --- | --- |
| `crypto_events_dag` | Validate-before-load pipeline for the crypto transaction history. | Great Expectations gating, quarantine table, transactional upsert into `prod.BT_CRYPTO_EVENTS`. |
| `churn_training_dag` + `churn_drift_monitor_w` | End-to-end churn MLOps demo: data prep, Spark ML training, MLflow model registry integration, Streamlit UI, weekly drift report with Evidently. | Spark master/worker, MLflow tracking/registry on MinIO, JupyterLab for experiments, Streamlit app for manual scoring, Evidently report uploaded to `s3://mlflow-artifacts/drift-reports/`. |

The new services are exposed locally for quick access:

| Service | URL / Notes |
| --- | --- |
| Airflow UI | `http://localhost:8080` (`admin` / `admin`). |
| pgAdmin | `http://localhost:8081` (`admin@local.test` / `admin`). |
| MinIO Console | `http://localhost:9001` (`minio` / `minio123`). Buckets: `airflow-logs`, `ge-artifacts`, `mlflow-artifacts`. |
| MLflow Tracking UI | `http://localhost:5000`. Uses Postgres `mlflow_db` + MinIO artifacts. |
| JupyterLab | `http://localhost:8888` (token `mlops`). Mounts `notebooks/` and shares credentials to MLflow/MinIO. |
| Spark Master UI | `http://localhost:8082`. Cluster reachable via `spark://spark-master:7077`. |
| Streamlit | `http://localhost:8601`. UI for ad-hoc churn scoring using the Production model. |

Follow `docs/README.md` for full stack instructions and `docs/RUNBOOK.md` for DAG specifics (crypto + churn). The Makefile targets continue to be the easiest way to start/reset the entire environment.
