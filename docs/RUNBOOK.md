# crypto_events_dag

Este documento resume cómo funciona el DAG de Airflow, cómo operarlo y qué validar en cada corrida de punta a punta.

## Propósito
`crypto_events_dag` implementa un pipeline *validate-before-load* para las transacciones cripto de MELI. Extrae el historial de los últimos 5 días, genera candidatos limpios en *staging*, ejecuta suites de Great Expectations y realiza un *upsert* con baja lógica en `prod.BT_CRYPTO_EVENTS`. Todo registro inválido se deriva a `dqm.BT_CRYPTO_EVENTS_REJECTS` junto con el motivo.

## Grafo de tareas
1. **generate_fake_history_last_5_days** – Ejecuta `python /opt/airflow/scripts/faker_seed.py` para garantizar filas frescas (sólo inserciones hacia adelante) en la ventana móvil.
2. **build_events_candidate** – Trunca `staging.BT_CRYPTO_EVENTS_CANDIDATE`, convierte fechas, calcula `PURCHASE_VALUE` y carga únicamente el rango de 5 días.
3. **ge_validate_raw** – Corre la suite `suite_raw_history`. Falla si faltan columnas, enums inválidos, fechas mal formateadas o valores negativos.
4. **ge_validate_candidate** – Ejecuta `suite_staging_candidate`, verificando tipos `DATE`, consistencia matemática y duplicados.
5. **merge_to_prod** – *Upsert* transaccional con `ON CONFLICT` + desactivación lógica de claves que desaparecen del staging en el mismo rango.
6. **vacuum_analyze_prod** – Paso opcional para mantener estadísticas actualizadas.
7. **notify_success** – Log final indicando que la corrida cerró correctamente.

Si alguna validación de GE falla, el DAG se detiene antes de tocar producción. Los helpers `capture_raw_rejects` y `capture_staging_rejects` guardan sitio/usuario/fecha/cripto + regla fallida para auditoría.

## Cómo ejecutarlo
1. Levantá el stack con `make init && make up` (ver README principal).
2. Habilitá el DAG desde la UI o lanzalo manualmente:
   ```bash
   make dag-run
   ```
3. Monitoreá desde la UI o con logs:
   ```bash
   docker compose -f infrastructure/docker-compose.yml logs -f airflow-scheduler
   docker compose -f infrastructure/docker-compose.yml logs -f airflow-worker
   ```

## Validaciones recomendadas
- **Seeds**: en pgAdmin/psql revisá `raw.BT_CRYPTO_TRANSACTION_HISTORY` para confirmar las filas iniciales + Faker.
- **Staging**: tras `build_events_candidate` inspeccioná `staging.BT_CRYPTO_EVENTS_CANDIDATE`; deben existir sólo fechas del rango móvil y `purchase_value` calculado.
- **Great Expectations**:
  - Resúmenes en `dqm.DQ_RUNS`.
  - Rechazos fila a fila en `dqm.BT_CRYPTO_EVENTS_REJECTS` (filtrá por `suite_name`).
  - Data Docs disponibles en MinIO (`ge-artifacts`); reconstruilos con:
    ```bash
    docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
      great_expectations --v3-api docs build
    ```
    Luego navega a `http://localhost:9001/browser/ge-artifacts/data_docs`.
- **Prod**: consulta `prod.BT_CRYPTO_EVENTS` para validar *upserts* y toggles de `is_active`.

### Troubleshooting rápido

| Síntoma | Qué revisar | Acción |
| --- | --- | --- |
| La corrida queda detenida en GE | `dqm.BT_CRYPTO_EVENTS_REJECTS` + logs de la tarea | Corregí los datos (o regenerá vía Faker) y relanzá el DAG |
| No se ven logs en la UI | ¿Existe `airflow/logs` en el host con permisos 777? | `make clean` + `make up` |
| Producción no cambia | GE falló o se abortó antes de `merge_to_prod` | Re-ejecutá tras validar; el merge es transaccional |

## Recuperación
Si alguna regla falla, corregí la fuente (o eliminá los registros en cuarentena) y corré `make dag-run` nuevamente. Como el merge nunca se ejecutó, `prod` permanece intacto.

---

# churn_training_dag

Pipeline de churn basado en Spark que consume `airflow/mlops/*.csv`, genera *features*, entrena una regresión logística y gestiona el ciclo de vida del modelo en MLflow. Está programado por Datasets de Airflow: sólo corre cuando `churn_data_watchdog` detecta cambios reales en los CSV.

## Grafo de tareas
1. **clean_raw_sources** – Valida/normaliza los CSV y guarda snapshots parquet (pagos + base de usuarios).
2. **feature_engineering** – Calcula agregados, une fuentes auxiliares, crea etiqueta binaria y actualiza el historial de referencia (últimas 8 semanas).
3. **train_spark_model** – Levanta Spark (`spark://spark-master:7077`), ejecuta una grilla chica de `regParam`/`elasticNetParam`, evalúa AUC y registra parámetros + métricas + artefactos (dataset, SHAP PNG, mean_abs_shap) en MLflow. También loguea `recent_drift_share` / `recent_drift_detected` reutilizando Evidently.
4. **evaluate_candidate** – Registra el run como nueva versión en `churn-model`, compara AUC vs. la versión Production vigente y guarda la decisión.
5. **decide_promotion** – Rama hacia `promote_model` o `skip_promotion`.
6. **promote_model / skip_promotion** – Promueve al stage Production (archivando versiones previas) o deja registro del rechazo. Se requiere que el candidato supere al actual por `CHURN_PROMOTION_MIN_DELTA` (0.005 por defecto).
7. **notify_success** – Log final informativo.

## Artefactos clave
- CSV origen: `airflow/mlops/*.csv` (montados en `/opt/mlops`).
- Parquets intermedios: `airflow/mlops/artifacts/payments_clean.parquet`, `user_base.parquet`, `churn_features.parquet`.
- Historial de referencia: `airflow/mlops/artifacts/reference_history.parquet`.
- MLflow: experimento `churn_retention`, modelo `churn-model`.

### Qué revisar
- MLflow (`http://localhost:5000`) debe mostrar los nuevos runs con métricas `roc_auc`, `recent_drift_share`, `recent_drift_detected` y artefactos de explainability.
- Spark UI (`http://localhost:8082`) evidencia las corridas activas mientras `train_spark_model` está ejecutando.
- Streamlit (`http://localhost:8601`) carga el modelo Production y muestra SHAP una vez que existe una versión promovida.

---

# churn_data_watchdog

DAG diario que calcula hashes de todos los CSV de churn y emite eventos de Dataset cuando detecta cambios. `churn_training_dag` depende de esos Datasets, por lo que se reentrena sólo cuando hay novedades. El estado se guarda en `airflow/mlops/artifacts/source_hashes.json`.

## Tareas
1. **detect_csv_changes** (`ShortCircuitOperator`) – verifica existencia de archivos, calcula MD5 y compara contra el snapshot previo; si no hay cambios, corta la ejecución.
2. **emit_dataset_update** (`EmptyOperator`) – expone los cinco Datasets (`file:///opt/mlops/*.csv`). Cuando corre, el scheduler crea un evento por dataset y dispara la DAG de entrenamiento.

## Uso
- **Primera vez**: eliminá `source_hashes.json` y gatillá el DAG; se interpretará como “hubo cambios” y se emitirá el evento.
- **Día a día**: dejá que corra según schedule; si no hay cambios, termina en segundos. Editaste un CSV manualmente? volvete a ejecutar el watchdog.
- **Debug**: consultá la tabla `dataset_event` (DB de Airflow) para confirmar que se generaron eventos o revisá el log (“Detectados cambios…”).

---

# churn_drift_monitor_w

Pipeline semanal con Evidently que compara el snapshot actual contra la media de las últimas 8 semanas.

## Grafo de tareas
1. **build_current_snapshot** – Copia `churn_features.parquet`, aplica ruido leve para simular la semana actual y guarda `current_snapshot.parquet`.
2. **run_drift_report** – Ejecuta `DataDriftPreset`, guarda el HTML en `airflow/mlops/artifacts/drift_reports/` y lo sube a MinIO (`mlflow-artifacts/drift-reports/<fecha>.html`). Devuelve `{drift_detected, report_path, s3_key}`.
3. **log_drift_result** – Informa si hubo drift y deja el path del reporte en logs.

## Notas
- Ejecutá `churn_training_dag` al menos una vez antes, así existe historial de referencia.
- Los reportes se descargan desde `http://localhost:9001/browser/mlflow-artifacts/drift-reports`.
- Fácil de extender con alertas (Slack/email) usando el flag `drift_detected`.

---

## Checklist Operativo

1. **Levantar servicios** – `make reset` (entorno nuevo) o `make up` (entorno ya construido). Verificá `docker compose … ps`.
2. **Watchdog** – Garantizá que `churn_data_watchdog` corra para emitir eventos de Dataset.
3. **Entrenamiento churn** – Confirmá que el DAG dataset-triggered llegue hasta `promote_model`/`skip_promotion`. Revisá MLflow para validar métricas/artefactos.
4. **Pipeline crypto** – `make dag-run` o dejá el schedule. Revisá staging, prod y la tabla de rechazos + Data Docs.
5. **Monitoreo de drift** – Ejecutá o esperá a `churn_drift_monitor_w`; verificá que se genere el HTML en MinIO.
6. **UIs** – Airflow para estado, MLflow para modelos, MinIO para artefactos, Streamlit para scoring, pgAdmin para consultas rápidas.

## Referencias útiles

- **Arquitectura + detalles de churn**: [`airflow/dags/churn/CHURN_PLAYBOOK.md`](../airflow/dags/churn/CHURN_PLAYBOOK.md)
- **Playbook de crypto**: [`airflow/dags/crypto_events/CRYPTO_PLAYBOOK.md`](../airflow/dags/crypto_events/CRYPTO_PLAYBOOK.md)
- **Quickstart / credenciales**: [`README.md`](../README.md#guía-rápida-happy-path)
- **Targets de Makefile**: `Makefile`
- **Metadata de Datasets**: tablas `dataset` y `dataset_event` en la DB de Airflow (`docker compose … exec postgres psql -U airflow -d crypto_db`)
