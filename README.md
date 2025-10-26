# KPIdelivery-MLOps

Repo organized by functional layers:

- `infrastructure/`: Docker Compose stack + one Dockerfile per service (Airflow, Postgres, Redis, MinIO, pgAdmin, MLflow, Spark master/worker, JupyterLab, Streamlit).
- `airflow/`: DAG packages (`crypto_events`, `churn`), scripts, and the Great Expectations context mounted inside the Airflow containers.
- `airflow/mlops/`: source CSVs for the churn exercise + derived artifacts (ignored via `.gitignore`).
- `database/`: SQL DDL/seed scripts consumed by Postgres bootstrap (crypto schemas + MLflow DB provisioning).
- `docs/`: runbooks (`docs/README.md` for the stack, `docs/README_DAG.md` for DAG-level behavior).
- `app/`: Streamlit UI that reads the latest Production model from MLflow for manual scoring.
- `notebooks/`: mounted into the JupyterLab container for interactive exploration.
- `Makefile`: convenience targets (`make init`, `make up`, `make down`, `make reset`, `make dag-run`, etc.) wrapping the Compose workflow.

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

Follow `docs/README.md` for full stack instructions and `docs/README_DAG.md` for DAG specifics (crypto + churn). The Makefile targets continue to be the easiest way to start/reset the entire environment.
