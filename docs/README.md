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
Run commands from repo root pointing to the Compose file under `infrastructure/`:
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
- Trigger DAG manually: `docker compose -f infrastructure/docker-compose.yml exec airflow-webserver airflow dags trigger crypto_events_dag`
- Inspect schemas: `docker compose -f infrastructure/docker-compose.yml exec postgres psql -U airflow -d crypto_db -c "\dn"`
- Inspect remote logs / Data Docs: browse `http://localhost:9001` (MinIO console) and open the corresponding bucket/object.

## Tear Down
```bash
docker compose -f infrastructure/docker-compose.yml down -v
```
This removes containers and volumes (including PostgreSQL data); rerun the init sequence to start fresh.
