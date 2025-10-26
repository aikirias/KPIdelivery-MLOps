# Paquete Churn MLOps

Este directorio contiene los DAGs de Airflow, configuraciones y utilidades que implementan el pipeline de churn. Cubre el ciclo MLOps completo:

1. **Preparación de datos**: `clean_raw_sources` y `feature_engineering` leen los CSV de `/opt/mlops`, consolidan la base de usuarios, generan agregaciones/labels y persist en parquet dentro de `airflow/mlops/artifacts/`.
2. **Entrenamiento de modelo** (`train_spark_model`): crea una sesión Spark (`spark://spark-master:7077`), ejecuta una grilla liviana de `regParam`/`elasticNetParam`, evalúa sobre un *holdout* y conserva el mejor resultado. Además loguea en MLflow los métricos de Evidently (`drift_share` / `drift_detected`) junto con el `roc_auc`, el snapshot del dataset y los artefactos SHAP (PNG + mean_abs). El modelo se registra en `churn-model`.
3. **Evaluación y registro**: `evaluate_candidate` compara el AUC del candidato contra la versión Production y `decide_promotion` enruta a `promote_model` o `skip_promotion`. Así Streamlit/Jupyter siempre consumen la versión más reciente.
4. **Monitoreo de drift** (`churn_drift_monitor_w`): DAG semanal que toma el último snapshot, corre Evidently contra las últimas 8 semanas, sube el reporte HTML a `s3://mlflow-artifacts/drift-reports/<fecha>.html` e informa si se detectó drift.
5. **Watchdog de datasets** (`churn_data_watchdog`): calcula hashes diarios de los CSV; si detecta cambios emite eventos de Dataset que disparan `churn_training_dag`. Sin cambios ⇒ no se reentrena.

## Archivos clave

- `config.py`: rutas, variables de MLflow, columnas de features y credenciales de MinIO.
- `dag.py`: definición del DAG de entrenamiento (ahora dispara por Datasets).
- `drift_dag.py`: DAG semanal de Evidently.
- `data_watchdog_dag.py`: emite eventos cuando cambian los CSV.
- `datasets.py`: definición de los cinco Datasets file://.
- `tasks/data_prep.py`: limpieza y feature engineering (produce `churn_features.parquet` + historial).
- `tasks/modeling.py`: entrenamiento Spark, logging en MLflow, lógica de evaluación/promoción.
- `tasks/drift.py`: generación y subida de reportes de Evidently.

## Cómo ejecutar el pipeline

1. Asegurate de tener el stack corriendo (`make reset && make up` la primera vez, luego alcanza con `make up`). Spark, MLflow, MinIO y Airflow deben estar “Up”.
2. Gatillá el DAG (también podés dejar que los Datasets lo disparen):
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
     airflow dags trigger churn_training_dag
   ```
   Seguís logs por UI o con `docker compose ... logs -f airflow-worker`.
3. Tras una corrida exitosa, revisá MLflow (`http://localhost:5000`): deberías ver el nuevo run + una versión registrada. Streamlit (`http://localhost:8601`) mostrará el modelo Production junto con métricas de drift.
4. Ejecutá el DAG de drift semanal:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
     airflow dags trigger churn_drift_monitor_w
   ```
   El HTML queda en el bucket `mlflow-artifacts/drift-reports/<fecha>.html`.

## Métricas y artefactos en MLflow

| Artefacto / Métrica | Ubicación | Quién lo registra | Para qué sirve |
| --- | --- | --- | --- |
| `roc_auc` | Run de MLflow → pestaña Metrics | `train_spark_model` | Métrica principal de selección |
| `recent_drift_share`, `recent_drift_detected` | Metrics | `train_spark_model` (usa Evidently) | Relaciona desempeño con estabilidad de datos |
| `dataset/churn_features.parquet` | Artifacts | `train_spark_model` | Snapshot exacto del dataset usado |
| `explainability/shap_summary.png` | Artifacts | `train_spark_model` | Visualización SHAP mostrada en Streamlit |
| `explainability/mean_abs_shap.json` | Artifacts | `train_spark_model` | Tabla de importancia promedio para dashboards |
| Reporte de drift | MinIO `mlflow-artifacts/drift-reports/<fecha>.html` | `churn_drift_monitor_w` | Diagnóstico semanal de distribución |

Streamlit descarga automáticamente los artefactos SHAP del modelo en Production; MLflow preserva el historial para auditorías.

## Datos de entrada

- Los CSV viven en `airflow/mlops/` (montados en `/opt/mlops`). Cada corrida:
  - Limpia `PAYMENTS.csv` → `payments_clean.parquet`.
  - Construye la base de usuarios combinando `ACTIVE_USER.csv`, `DEMOGRAFICOS.csv`, `DINERO_CUENTA.csv`, `MARKETPLACE_DATA.csv`.
  - Genera *features* + etiqueta binaria y actualiza el historial de referencia usado para drift.

## Extensiones posibles

- Ajustar `config.FEATURE_COLUMNS` y la lógica en `data_prep`.
- Cambiar el modelo Spark (p. ej. árboles) en `tasks/modeling.py` o modificar la métrica.
- Modificar el margen de promoción con `CHURN_PROMOTION_MIN_DELTA`.
- Agregar notificaciones (Slack, email) en los DAGs de entrenamiento o drift.

## Troubleshooting / FAQ

| Pregunta | Respuesta |
| --- | --- |
| **¿Por qué no corrió `churn_training_dag` tras editar un CSV?** | Es un DAG disparado por Datasets. Corré `churn_data_watchdog` luego del cambio o borrá `airflow/mlops/artifacts/source_hashes.json` para forzar un nuevo evento. Verificá la tabla `dataset_event`. |
| **Falló `train_spark_model` durante el HPO** | Revisá `airflow/logs/dag_id=churn_training_dag/.../train_spark_model/` y el Spark UI (`http://localhost:8082`). Usualmente Spark no está arriba o faltan credenciales de MinIO. Corregí y reintenta desde la UI. |
| **El candidato no se promociona pese a tener AUC parecido** | Exigimos que supere al Production por al menos `CHURN_PROMOTION_MIN_DELTA` (0.005). Ajustá el umbral o mejorá el modelo. |
| **Streamlit sigue mostrando “Heurística local”** | Aún no existe modelo en Production. Corré el DAG, asegurate de que `promote_model` termine OK y refrescá la app. |
| **¿Dónde se guardan los hashes del watchdog?** | En `airflow/mlops/artifacts/source_hashes.json`. Eliminá el archivo para que la próxima corrida considere que hubo cambios. |

Todo el flujo corre dentro de Docker (Spark, MLflow, Airflow, Streamlit, Jupyter). Usá los targets del `Makefile` para resetear, levantar o disparar pipelines sin recordar comandos largos de Compose.
