# KPIdelivery-MLOps

Repo organized by functional layers:
- `infrastructure/`: Docker Compose stack, Airflow image recipe, env vars.
- `airflow/`: DAGs, scripts, and Great Expectations context mounted into Airflow containers.
- `database/`: SQL DDL/seed scripts consumed by Postgres bootstrap and tasks.
- `docs/`: detailed runbooks (see `docs/README.md` and `docs/README_DAG.md`).
- `Makefile`: convenience targets (`make init`, `make up`, `make down`, `make reset`, `make dag-run`, etc.) wrapping the Compose workflow found in the docs.

To run the project follow `docs/README.md`. For DAG-specific behavior see `docs/README_DAG.md`.
