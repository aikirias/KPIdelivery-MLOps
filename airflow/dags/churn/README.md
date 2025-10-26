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

## Cómo probar el flujo completo

1. **Preparar el entorno** – `make reset && make up` (primer uso) o `make up` si los contenedores ya existen. Comprobá con `docker compose ... ps` que Airflow, Spark, MLflow, MinIO y Redis estén “Up”.
2. **Emitir eventos de Dataset** – Ejecutá `make churn-watchdog` (o `make mlops-run` si querés encadenar todo). Verificá en la UI de Airflow que la corrida `churn_data_watchdog` finalice en verde. *Tip:* En la primera corrida después de un `make reset`, borrá `airflow/mlops/artifacts/source_hashes.json` para que el watchdog detecte cambios desde cero. Para confirmar que se generaron eventos podés consultar:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \
     psql -U airflow -d crypto_db -c "SELECT * FROM dataset_event ORDER BY timestamp DESC LIMIT 5;"
   ```
   Si no aparecen filas nuevas, revisá `airflow/mlops/artifacts/source_hashes.json` (borrarlo fuerza una nueva emisión) y los logs de la tarea `detect_csv_changes`.
3. **Ejecutar `churn_training_dag`** – Debería dispararse automáticamente tras el paso anterior. Podés forzarlo con `make churn-train`. Seguimiento recomendado:
   - UI de Airflow → pestaña *Graph* del DAG para ver el avance de cada tarea (`clean_raw_sources`, `feature_engineering`, `train_spark_model`, etc.).
   - Logs locales `airflow/logs/dag_id=churn_training_dag/...` o `docker compose ... logs -f airflow-worker` para diagnosticar fallas (Spark, GE, DB).
   - Al finalizar, inspeccioná MLflow (`http://localhost:5000`): el experimento `churn_retention` debe mostrar el run con `roc_auc`, `recent_drift_share` y los artefactos `dataset/` y `explainability/`. En la sección *Models* verificá que `churn-model` tenga una versión nueva/promocionada.
   - Abrí Streamlit (`http://localhost:8601`) y confirmá que ya no muestre la heurística local sino el modelo Production + gráficos SHAP.
4. **Generar reporte de drift** – Ejecutá `make churn-drift` (o esperá al schedule semanal). La tarea `run_drift_report` produce un HTML en MinIO (`mlflow-artifacts/drift-reports/<fecha>.html`). Validá también el log de la tarea `log_drift_result` (debería indicar si `drift_detected=true/false`).
5. **Validar housekeeping** – Revisá que `airflow/mlops/artifacts/reference_history.parquet` haya incorporado el snapshot más reciente, que `dataset_event` acumule sólo los eventos esperados y que `airflow/logs` no contenga errores repetitivos.

### Cómo detectar errores rápido
- **DAG no dispara tras `make churn-watchdog`**: confirmar que el watchdog terminó en *success* y que el query `SELECT * FROM dataset_event …` muestra nuevos registros; si no, borrar `source_hashes.json` y reintentar.
- **Fallas en `train_spark_model`**: revisar logs de la tarea (`airflow/logs/.../train_spark_model`) y el Spark UI (`http://localhost:8082`). Los mensajes más comunes apuntan a credenciales de MinIO mal seteadas o a memoria insuficiente.
- **Promoción omitida**: la tarea `skip_promotion` imprime la métrica candidata vs. Production y el margen requerido (`CHURN_PROMOTION_MIN_DELTA`). Ajustá la variable o mejorá el modelo.
- **Streamlit sigue en heurística**: confirmar en MLflow > Models que haya una versión en Production y que la transición se haya completado (`promote_model` en verde).

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
