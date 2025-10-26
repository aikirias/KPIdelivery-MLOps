# KPIdelivery-MLOps

Repositorio organizado por capas funcionales:

- `infrastructure/`: stack Docker Compose con un Dockerfile por servicio (Airflow, Postgres, Redis, MinIO, pgAdmin, MLflow, Spark master/worker, JupyterLab, Streamlit).
- `airflow/`: paquetes de DAGs (`crypto_events`, `churn`), scripts y el contexto de Great Expectations montado dentro de los contenedores de Airflow.
- `airflow/mlops/`: CSV de origen para el ejercicio de churn y los artefactos derivados (ignorados vía `.gitignore`).
- `database/`: scripts SQL (DDL + seeds) que Postgres ejecuta al inicializar (esquemas de crypto + base de MLflow).
- `docs/`: runbooks (`docs/README.md` describe todo el stack y `docs/RUNBOOK.md` detalla el funcionamiento de los DAGs). Cada DAG tiene además su propio playbook (`airflow/dags/churn/CHURN_PLAYBOOK.md`, `airflow/dags/crypto_events/CRYPTO_PLAYBOOK.md`).
- `app/`: interfaz Streamlit que consume el último modelo en etapa *Production* desde MLflow para scoring manual.
- `notebooks/`: montados en el contenedor de JupyterLab para experimentación interactiva.
- `Makefile`: atajos (`make init`, `make up`, `make down`, `make reset`, `make dag-run`, etc.) sobre los comandos de Compose.

## Guía rápida (Happy Path)

1. **Prerequisitos** – Docker Desktop (o motor equivalente) + plugin de Compose, `make` y ~10 GB libres. No hace falta instalar dependencias de Python: todo corre en contenedores.
2. **Clonar y configurar** – `git clone <repo>`. El repositorio ya incluye `.env` (credenciales compartidas para Airflow/MinIO/MLflow/Spark). Ajustá puertos/rutas ahí si lo necesitás.
3. **Arranque en frío** – Desde la raíz ejecutá `make reset`. Esto derriba cualquier stack previo, reinicializa la metadata de Airflow, recrea los volúmenes de Postgres/MinIO y levanta todos los servicios. Esperá a que `docker compose -f infrastructure/docker-compose.yml ps` muestre `Up` para webserver, scheduler, worker, Spark, MLflow, Streamlit, MinIO, pgAdmin, Redis, Postgres y JupyterLab.
4. **Accesos por defecto**
   - Airflow UI: `http://localhost:8080` (`admin` / `admin`)
   - pgAdmin: `http://localhost:8081` (`admin@local.test` / `admin`)
   - Consola MinIO: `http://localhost:9001` (`minio` / `minio123`)
   - MLflow UI: `http://localhost:5000` (sin autenticación)
   - Streamlit: `http://localhost:8601`
   - JupyterLab: `http://localhost:8888` (token `mlops`)
5. **Primer flujo de ejecución** (≈10 minutos):
   1. Lanzá el DAG watchdog para que Airflow registre eventos de Dataset sobre los CSV de churn:
      ```bash
      docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
        airflow dags trigger churn_data_watchdog
      ```
      Al detectar cambios (o simplemente porque es la primera corrida), emite eventos y `churn_training_dag` se dispara automáticamente. Seguilo desde la UI (`DAGs > churn_training_dag > Graph`).
   2. Ejecutá la tubería de crypto una vez:
      ```bash
      make dag-run
      ```
      Corre `crypto_events_dag` manualmente para la ventana móvil de 5 días usando datos generados con Faker.
   3. El DAG semanal de *drift* (`churn_drift_monitor_w`) queda programado; podés forzar una corrida desde la UI o con `airflow dags trigger churn_drift_monitor_w`.
6. **Verificaciones**
   - MLflow (`http://localhost:5000`) debería mostrar al menos un run en el experimento `churn_retention` con los métricos `roc_auc`, `recent_drift_share`, `recent_drift_detected` y artefactos bajo `dataset/` y `explainability/`.
   - El bucket `mlflow-artifacts` en MinIO contiene los reportes de drift (HTML) dentro de `drift-reports/`.
   - Streamlit pasa a consumir el modelo en Production y exhibe los gráficos/tabla de SHAP apenas exista una promoción exitosa.
   - Las schemas `raw`, `staging`, `prod`, `dqm` en Postgres muestran las tablas de crypto; `dqm.BT_CRYPTO_EVENTS_REJECTS` guarda los registros rechazados por validaciones.

Luego del setup inicial, usualmente alcanza con `make up` para levantar servicios y disparar el DAG que quieras demostrar. Ejecutá `make reset` cuando necesites volver a un estado limpio.

## ¿Por qué dos pipelines?

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

Consultá `docs/README.md` para instrucciones completas del stack y `docs/RUNBOOK.md` para detalles de los DAGs (crypto + churn). El `Makefile` sigue siendo la forma más rápida de iniciar, reiniciar o disparar pipelines.
