# KPIdelivery-MLOps

Repositorio organizado por capas funcionales:

- `infrastructure/`: stack Docker Compose con un Dockerfile por servicio (Airflow, Postgres, Redis, MinIO, pgAdmin, MLflow, Spark master/worker, JupyterLab, Streamlit).
- `airflow/`: paquetes de DAGs (`crypto_events`, `churn`), scripts y el contexto de Great Expectations montado dentro de los contenedores de Airflow.
- `airflow/mlops/`: CSV de origen para el ejercicio de churn y los artefactos derivados (ignorados vía `.gitignore`).
- `database/`: scripts SQL (DDL + seeds) que Postgres ejecuta al inicializar (esquemas de crypto + base de MLflow).
- `docs/`: documentación adicional del stack (guías específicas que acompañan los README principales cuando haga falta). Los detalles operativos de cada DAG viven en sus propios README (`airflow/dags/churn/README.md`, `airflow/dags/crypto_events/README.md`).
- `app/`: interfaz Streamlit que consume el último modelo en etapa *Production* desde MLflow para scoring manual.
- `notebooks/`: montados en el contenedor de JupyterLab para experimentación interactiva.
- `Makefile`: atajos (`make init`, `make up`, `make down`, `make reset`, `make dag-run`, etc.) sobre los comandos de Compose.

## Stack tecnológico y por qué

| Componente | Rol | ¿Por qué? |
| --- | --- | --- |
| PostgreSQL 15 | Base operativa (schemas raw/staging/prod/dqm) + metadata de Airflow + BD de MLflow | Motor relacional estable, soporta `ON CONFLICT` para los upserts y se inicializa fácilmente con scripts en `database/sql/init`. |
| Airflow 2.7 (CeleryExecutor) | Orquestador de los DAGs de crypto y churn | Necesitamos dependencias complejas (Spark, GE, MLflow). Airflow facilita el *scheduling* y la observabilidad vía UI. |
| Redis 7 | Broker/resultado de Celery | Permite escalar los workers de Airflow sin añadir dependencias extra. |
| MinIO | Storage S3-compatible para logs de Airflow, artefactos GE y MLflow | Simplifica la integración con GE y MLflow sin depender de un servicio externo de AWS. |
| Great Expectations | Validaciones DQ antes del merge | Permite implementar el patrón *validate-before-load* y guardar reportes/artefactos trazables. |
| Spark 3.5 (master/worker) | Entrenamiento distribuido para churn | Necesario para correr PySpark/MLlib y permitir HPO ligero con datasets medianos. |
| MLflow 2.10 | Tracking + Model Registry | Centraliza métricas, artefactos (dataset, SHAP) y versionado de modelos para consumo por Streamlit. |
| Streamlit | UI de scoring y explainability | Ofrece una capa rápida para probar el modelo Production y mostrar SHAP sin desarrollar una app full-stack. |
| JupyterLab | Entorno interactivo | Usado principalmente para testing. |

## Guía rápida (Happy Path)

1. **Prerequisitos** – Docker + Docker Compose, `make` y ~10 GB libres. No hace falta instalar dependencias, todo corre en contenedores.
2. **Clonar y configurar** – `git clone <repo>`. El repositorio ya incluye `.env` (credenciales compartidas para Airflow/MinIO/MLflow/Spark). Ajustá puertos/rutas ahí si lo necesitás.
3. **Arranque en frío** – Desde la raíz ejecutá `make reset`. Esto derriba cualquier stack previo, reinicializa la metadata de Airflow, recrea los volúmenes de Postgres/MinIO y levanta todos los servicios. Esperá a que `docker compose -f infrastructure/docker-compose.yml ps` muestre `Up` para webserver, scheduler, worker, Spark, MLflow, Streamlit, MinIO, pgAdmin, Redis, Postgres y JupyterLab.
4. **Accesos por defecto**
   - Airflow UI: `http://localhost:8080` (`admin` / `admin`)
   - pgAdmin: `http://localhost:8081` (`admin@local.test` / `admin`)
   - Consola MinIO: `http://localhost:9001` (`minio` / `minio123`)
   - MLflow UI: `http://localhost:5000` (sin autenticación)
   - Streamlit: `http://localhost:8601`
   - JupyterLab: `http://localhost:8888` (token `mlops`)
5. **Siguiente paso** – con los servicios arriba, seguí los README de `airflow/dags/churn` y `airflow/dags/crypto_events` para ejecutar y validar cada flujo usando los targets del `Makefile` (`make dag-run`, `make churn-watchdog`, `make mlops-run`, etc.). Ahí se detallan los pasos, checks y criterios de éxito.

Cuando ejecutes los DAGs (según los README específicos) deberías validar:
- MLflow (`http://localhost:5000`) con nuevos runs en `churn_retention`, métricos (`roc_auc`, `recent_drift_share`, etc.) y artefactos (`dataset/`, `explainability/`).
- Reportes de drift en MinIO (`mlflow-artifacts/drift-reports/`).
- Streamlit (`http://localhost:8601`) consumiendo el modelo en Production y mostrando SHAP.
- Tablas `raw/staging/prod/dqm` en Postgres con los datos de crypto; `dqm.BT_CRYPTO_EVENTS_REJECTS` registra cualquier validación fallida.

Luego del setup inicial, usualmente alcanza con `make up` para levantar servicios y disparar el DAG que quieras demostrar. Ejecutá `make reset` cuando necesites volver a un estado limpio.

## Los dos flujos:

| Workflow | Propósito | Highlights |
| --- | --- | --- |
| `crypto_events_dag` | Pipeline *validate-before-load* para el historial de transacciones cripto. | Great Expectations como *gate*, tabla de cuarentena, *upsert* transaccional sobre `prod.BT_CRYPTO_EVENTS`. |
| `churn_training_dag` + `churn_drift_monitor_w` | Demostración MLOps end-to-end de churn: limpieza, Spark ML, registro en MLflow, UI Streamlit y monitoreo de drift. | Spark master/worker, MLflow sobre MinIO, JupyterLab para notebooks, Streamlit para scoring manual, reportes Evidently en `s3://mlflow-artifacts/drift-reports/`. |

Servicios expuestos localmente:

| Servicio | URL / Nota |
| --- | --- |
| Airflow UI | `http://localhost:8080` (`admin` / `admin`) |
| pgAdmin | `http://localhost:8081` (`admin@local.test` / `admin`) |
| MinIO Console | `http://localhost:9001` (`minio` / `minio123`) – buckets: `airflow-logs`, `ge-artifacts`, `mlflow-artifacts` |
| MLflow Tracking UI | `http://localhost:5000` – usa Postgres `mlflow_db` + artefactos en MinIO |
| JupyterLab | `http://localhost:8888` (token `mlops`) – monta `notebooks/` y exporta credenciales a MLflow/MinIO |
| Spark Master UI | `http://localhost:8082` – clúster accesible en `spark://spark-master:7077` |
| Streamlit | `http://localhost:8601` – UI para scoring ad-hoc con el modelo Production |

Para conocer los flujos en detalle, consultá los README específicos de cada DAG (`airflow/dags/churn/README.md` y `airflow/dags/crypto_events/README.md`). El `Makefile` sigue siendo la forma más rápida de iniciar, reiniciar o disparar pipelines.
