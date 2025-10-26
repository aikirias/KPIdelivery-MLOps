# Crypto Events DAG

## Propósito
Pipeline incremental diario que extrae los últimos 5 días de transacciones cripto, construye la tabla candidata en *staging*, valida con Great Expectations y mergea condicionalmente en `prod.bt_crypto_events`, desactivando filas ausentes.

## Topología de tareas
1. **`extract_generate_history`** *(PythonOperator)* – rellena `raw.bt_crypto_transaction_history` con datos determinísticos de Faker usando la `logical_date` como *seed*.
2. **`transform_build_candidate`** *(SQLExecuteQueryOperator)* – ejecuta `sql/build_events_candidate.sql` para regenerar `staging.bt_crypto_events_candidate`.
3. **TaskGroup `qa_quality_checks`** – corre los checkpoints `suite_raw_history` y `suite_staging_candidate` mediante `dq.run_ge_validation`; cualquier fallo inserta rechazados en `dqm.bt_crypto_events_rejects`.
4. **`load_merge_to_prod`** *(SQLExecuteQueryOperator)* – aplica `sql/merge_to_prod.sql` para hacer *upsert* de las filas vigentes y marcar como inactivas las que faltan.
5. **`notify_success`** – log de cierre.

## Configuración
- Ajustable vía Variables de Airflow con prefijo `crypto_` (ej.: `crypto_postgres_conn_id`, `crypto_window_days`, `crypto_allowed_sites`). Valores por defecto en `config.py`.
- Las plantillas SQL viven en `sql/` y usan parámetros vía `params`.
- Los assets de GE residen en `/opt/airflow/ge`.

## Notas de desarrollo
- Si cambiás la lógica SQL, modificá el archivo correspondiente en `sql/` y asegurate de mantener los mismos parámetros en los operadores definidos en `dag.py`.
- Helpers:
  - `utils/extract.py` contiene la lógica de Faker.
  - `utils/dq.py` ejecuta GE y maneja los inserts en `dqm`.
- Usá los targets del `Makefile` (`make reset`, `make dag-run`, etc.) para levantar el stack y disparar la DAG end-to-end.

## Respuestas solicitadas
1. **Tabla adicional:** Sí, se utiliza `staging.bt_crypto_events_candidate` como buffer validado y `dqm.bt_crypto_events_rejects` como cuarentena. Así se protege `prod.bt_crypto_events`.
2. **Proceso ETL (4+ pasos):**
   - **Extract:** Leer `raw.bt_crypto_transaction_history` para los últimos 5 días (Faker sólo agrega lo que falte).
   - **Transform:** Truncar y cargar `staging.bt_crypto_events_candidate`, parseando fechas y calculando `purchase_value`.
   - **Validate:** Ejecutar ambas suites de GE; en caso de error, registrar rechazos en `dqm` y abortar.
   - **Load:** Correr `merge_to_prod.sql`, que hace *upsert* y luego desactiva (`is_active=false`) las claves del rango que ya no existen.
   - **Post-load:** Mantener métricas DQ y notificar.
3. **Refresh rolling de 5 días:** Cada corrida reconstruye `staging` sólo con el rango `hoy-4 … hoy`. El merge inserta nuevas filas, actualiza cambios vía `ON CONFLICT … DO UPDATE` y realiza un `UPDATE` extra para marcar `is_active=false` cuando falta en staging. Todo ocurre únicamente si las dos validaciones de GE aprueban, asegurando integridad antes de tocar producción.
