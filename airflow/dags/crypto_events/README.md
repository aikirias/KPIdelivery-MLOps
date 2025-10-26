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

## Cómo probar el flujo y detectar errores

1. **Levantar el stack** – `make reset && make up` (primera vez) o `make up` si los contenedores existen.
2. **Ejecutar la DAG** – Lanzá `make dag-run`. Esto dispara `crypto_events_dag` inmediatamente; monitorealo desde la UI (vista *Graph*) para verificar que las tareas `extract_generate_history`, `transform_build_candidate`, `qa_quality_checks` y `load_merge_to_prod` se completen en verde.
3. **Validar Great Expectations** – Si `qa_quality_checks` falla, inspeccioná `dqm.bt_crypto_events_rejects`:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \\
     psql -U airflow -d crypto_db -c "SELECT * FROM dqm.bt_crypto_events_rejects ORDER BY rejected_at DESC LIMIT 20;"
   ```
   El campo `reason` indica la regla rota. También podés reconstruir Data Docs con
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \\
     great_expectations --v3-api docs build
   ```
   y navegar a `http://localhost:9001/browser/ge-artifacts/data_docs/index.html`.
4. **Revisar staging y prod** – Tras una corrida exitosa:
   - `staging.bt_crypto_events_candidate` debe contener sólo fechas entre `CURRENT_DATE-4` y `CURRENT_DATE`.
   - `prod.bt_crypto_events` debe reflejar los upserts y las bajas lógicas (`is_active=false` para claves ausentes). Comprobalo con:
     ```bash
     docker compose -f infrastructure/docker-compose.yml exec postgres \\
       psql -U airflow -d crypto_db -c "SELECT site_id,user_id,purchase_date,crypto_type,is_active FROM prod.bt_crypto_events ORDER BY purchase_date DESC LIMIT 20;"
     ```
5. **Logs y notificaciones** – Si `load_merge_to_prod` falla por SQL o locks, revisá `airflow/logs/dag_id=crypto_events_dag/.../load_merge_to_prod/` o `docker compose ... logs -f airflow-worker`. `notify_success` deja un mensaje en stdout (quiera loguear en el futuro).

### Tips de diagnóstico rápido
- **GE falla constantemente**: confirmá que los CSV seeds no fueron modificados; si Faker genera datos fuera de rango, revisá `airflow/scripts/faker_seed.py` o vuelve a ejecutar `make reset` para restaurar las semillas iniciales.
- **Staging vacío**: asegurate de que `transform_build_candidate` corrió (mirá los logs SQL que imprime la tarea) y que `crypto_window_days` no quedó en cero.
- **Prod no cambia**: GE probablemente abortó antes del merge; revisá el historial de runs en la UI y `dqm.bt_crypto_events_rejects`.
- **Datos docs no actualizados**: ejecutá el comando de `great_expectations --v3-api docs build` y refrescá la consola de MinIO.

## Cómo probar el flujo completo

1. **Levantar servicios** – `make reset && make up` la primera vez (luego alcanza con `make up`). Confirmá en la UI de Airflow que `crypto_events_dag` esté habilitado.
2. **Disparar la DAG** – Ejecutá `make dag-run`. Esto corre inmediatamente las tareas `extract_generate_history`, `transform_build_candidate`, `qa_quality_checks`, `load_merge_to_prod` y `notify_success`.
3. **Monitoreo en vivo** – Desde Airflow (`DAGs > crypto_events_dag > Graph`) asegurate de que cada tarea cambie a verde. Si alguna falla:
   - Revisá los logs (`docker compose ... logs -f airflow-worker` o `airflow/logs/dag_id=crypto_events_dag/...`).
   - Consultá `dqm.bt_crypto_events_rejects` para ver las filas rechazadas y su `reason`.
4. **Validar staging** – Tras el paso `transform_build_candidate`, corré:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \\
     psql -U airflow -d crypto_db -c \"SELECT MIN(purchase_date),MAX(purchase_date),COUNT(*) FROM staging.bt_crypto_events_candidate;\"
   ```
   El rango debe cubrir exactamente `CURRENT_DATE-4` a `CURRENT_DATE`.
5. **Validar prod** – Luego de `load_merge_to_prod`, inspeccioná `prod.bt_crypto_events` para comprobar que los registros se insertaron/actualizaron y que `is_active` sólo quede en `true` para el rango móvil:
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \\
     psql -U airflow -d crypto_db -c \"SELECT site_id,user_id,purchase_date,crypto_type,is_active FROM prod.bt_crypto_events ORDER BY purchase_date DESC LIMIT 10;\"
   ```
6. **Revisar DQ/Docs** – Si las validaciones fallan se registran en `dqm.bt_crypto_events_rejects`; para ver el detalle visual reconstruí los Data Docs con `great_expectations --v3-api docs build` (comando en la sección anterior) y abrí `http://localhost:9001/browser/ge-artifacts/data_docs/index.html`.

### Cómo detectar errores rápidamente
- **Fallas en GE**: inspeccioná `dqm.bt_crypto_events_rejects`; el campo `reason` indica la regla. Ajustá los datos de origen o Faker y relanzá el DAG.
- **Staging vacío**: verificá los logs de `transform_build_candidate` (puede haber filtros de fecha erróneos o la ventana `crypto_window_days` en 0).
- **Producción no cambia**: significa que al menos una validación falló y `load_merge_to_prod` se saltó. Revisión de GE + reintento.
- **Data Docs desactualizados**: ejecutá el comando de GE mencionado para regenerarlos y revisalos en MinIO.

## Respuestas solicitadas
1. **Tabla adicional:** Sí, se utiliza `staging.bt_crypto_events_candidate` como buffer validado y `dqm.bt_crypto_events_rejects` como cuarentena. Así se protege `prod.bt_crypto_events`.
2. **Proceso ETL (4+ pasos):**
   - **Extract:** Leer `raw.bt_crypto_transaction_history` para los últimos 5 días (Faker sólo agrega lo que falte).
   - **Transform:** Truncar y cargar `staging.bt_crypto_events_candidate`, parseando fechas y calculando `purchase_value`.
   - **Validate:** Ejecutar ambas suites de GE; en caso de error, registrar rechazos en `dqm` y abortar.
   - **Load:** Correr `merge_to_prod.sql`, que hace *upsert* y luego desactiva (`is_active=false`) las claves del rango que ya no existen.
   - **Post-load:** Mantener métricas DQ y notificar.
3. **Refresh rolling de 5 días:** Cada corrida reconstruye `staging` sólo con el rango `hoy-4 … hoy`. El merge inserta nuevas filas, actualiza cambios vía `ON CONFLICT … DO UPDATE` y realiza un `UPDATE` extra para marcar `is_active=false` cuando falta en staging. Todo ocurre únicamente si las dos validaciones de GE aprueban, asegurando integridad antes de tocar producción.
