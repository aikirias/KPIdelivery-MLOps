# Crypto Events DAG

### Propósito general
Pipeline incremental diario que procesa el rango móvil de cinco días de `raw.bt_crypto_transaction_history`, genera candidatos limpios en *staging*, valida cada lote con Great Expectations y aplica un *upsert* seguro en `prod.bt_crypto_events`, desactivando (`is_active=false`) las claves que desaparecen.

---

## 1. Flujo completo (qué hace cada tarea)

| Paso | Operador | Descripción |
| --- | --- | --- |
| `extract_generate_history` | `PythonOperator` | Ejecuta `scripts/faker_seed.py` para garantizar un mínimo de filas por día en la ventana `hoy-4 … hoy` sin mutar historial previo (usa `logical_date` como *seed* determinístico). |
| `transform_build_candidate` | `SQLExecuteQueryOperator` | Corre `sql/build_events_candidate.sql`: trunca `staging.bt_crypto_events_candidate`, convierte `purchase_date` (STRING) a `DATE`, calcula `purchase_value = price * units` y persiste sólo el rango móvil. |
| `qa_quality_checks` | TaskGroup (2× GE) | `dq.run_ge_validation` ejecuta `suite_raw_history` y `suite_staging_candidate`. Los registros que violan reglas se insertan en `dqm.bt_crypto_events_rejects` con su `reason`; cualquier fallo aborta la DAG antes del merge. |
| `load_merge_to_prod` | `SQLExecuteQueryOperator` | Aplica `sql/merge_to_prod.sql`: `INSERT ... ON CONFLICT DO UPDATE` sobre la PK `(site_id,user_id,purchase_date,crypto_type)` y luego `UPDATE` para marcar `is_active=false` cuando una clave del rango ya no existe en staging. |
| `notify_success` | `PythonOperator` | Placeholder para notificaciones (hoy sólo loguea en stdout). |

---

## 2. Configuración y componentes

- **Variables Airflow (`crypto_*`)**: controlan el `conn_id` de Postgres, la ventana (`crypto_window_days`, por defecto 5) y listas blancas como `crypto_allowed_sites`. Revisa `config.py` para los valores por defecto.
- **SQL templates**: en `sql/`. Cada `SQLExecuteQueryOperator` pasa sus parámetros vía `params`; mantenelos sincronizados si editás la lógica.
- **Great Expectations**: el Data Context vive en `/opt/airflow/ge`. Las suites están en `ge/expectations/` y el checkpoint en `ge/checkpoints/crypto_checkpoint.yml`. Los artefactos y Data Docs se publican en MinIO (`s3://ge-artifacts/...`).
- **Helpers Python**: `utils/extract.py` (Faker) y `utils/dq.py` (wrapper GE + inserción en `dqm`).

---

## 3. Ejecución paso a paso (cómo probarlo)

1. **Levantar servicios**
   ```bash
   make reset && make up   # primera vez
   # o simplemente make up si ya existe el stack
   ```
   Confirmá en la UI de Airflow que `crypto_events_dag` esté *unpaused*.

2. **Disparar el DAG**
   ```bash
   make dag-run
   ```
   Desde la UI (Graph view) verificá que cada tarea pase a verde. Para logs en vivo:
   ```bash
   docker compose -f infrastructure/docker-compose.yml logs -f airflow-worker
   ```

3. **Validar Great Expectations**
   - Si `qa_quality_checks` falla, inspeccioná `dqm.bt_crypto_events_rejects`:
     ```bash
     docker compose -f infrastructure/docker-compose.yml exec postgres \
       psql -U airflow -d crypto_db -c \
       "SELECT rejected_at,site_id,user_id,purchase_date_raw,crypto_type,reason \
        FROM dqm.bt_crypto_events_rejects ORDER BY rejected_at DESC LIMIT 20;"
     ```
   - Para visualizar Data Docs:
     ```bash
     docker compose -f infrastructure/docker-compose.yml exec airflow-webserver \
       great_expectations --v3-api docs build
     ```
     Luego abrí `http://localhost:9001/browser/ge-artifacts/data_docs`.

4. **Verificar staging**
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \
     psql -U airflow -d crypto_db -c \
     "SELECT MIN(purchase_date), MAX(purchase_date), COUNT(*) \
        FROM staging.bt_crypto_events_candidate;"
   ```
   El rango debe cubrir exactamente `CURRENT_DATE-4` a `CURRENT_DATE`.

5. **Verificar producción**
   ```bash
   docker compose -f infrastructure/docker-compose.yml exec postgres \
     psql -U airflow -d crypto_db -c \
     "SELECT site_id,user_id,purchase_date,crypto_type,is_active \
        FROM prod.bt_crypto_events ORDER BY purchase_date DESC LIMIT 10;"
   ```
   Confirmá que:
   - Las nuevas filas aparezcan con `is_active = true`.
   - Claves antiguas fuera del rango móvil estén en `false`.

6. **Revisar logs finales**
   - `load_merge_to_prod` imprime el número de filas afectadas en el log (`airflow/logs/.../load_merge_to_prod`).
   - `notify_success` deja un mensaje de completitud; útil para enganchar alertas futuras.

---

## 4. Diagnóstico rápido

| Síntoma | Qué revisar | Acción sugerida |
| --- | --- | --- |
| GE falla siempre | `dqm.bt_crypto_events_rejects` + Data Docs | Ajustá los datos de origen; si Faker generó valores fuera de rango, corré `make reset` o edita `scripts/faker_seed.py`. |
| `staging` vacío | Logs de `transform_build_candidate` | Confirmá filtros de fecha y que `crypto_window_days` > 0. |
| `prod` no cambia | GE abortó antes del merge | Revisa los logs de GE, corrige, vuelve a ejecutar `make dag-run`. |
| Data Docs no actualizados | Comando `great_expectations --v3-api docs build` | Reconstruí y refrescá el navegador en MinIO. |

---

## 5. Preguntas del enunciado

1. **¿Es necesaria una tabla adicional para cumplir las condiciones?**  
   Sí. `staging.bt_crypto_events_candidate` actúa como buffer validado y `dqm.bt_crypto_events_rejects` almacena los registros rechazados con su motivo, lo que evita contaminar `prod.bt_crypto_events`.

2. **Proceso ETL de al menos 4 pasos para la carga:**
   - **Extract:** leer `raw.bt_crypto_transaction_history` filtrando los últimos 5 días (Faker sólo agrega lo que falte).
   - **Transform:** truncar + poblar `staging.bt_crypto_events_candidate`, convertir fechas y calcular `purchase_value`.
   - **Validate:** ejecutar las suites de Great Expectations sobre `raw` y `staging`; si falla, registrar en `dqm` y detener el DAG.
   - **Load:** ejecutar `merge_to_prod.sql` para hacer *upsert* y luego `UPDATE` para marcar `is_active=false` cuando ya no exista en staging.
   - **Post-load:** mantener métricas de calidad y notificar el resultado.

3. **Refresh rolling de 5 días:**  
   Cada ejecución reconstruye `staging` únicamente con la ventana `hoy-4 … hoy`. El merge inserta nuevas filas y actualiza cambios via `ON CONFLICT DO UPDATE`. Luego un `UPDATE` adicional marca `is_active=false` en `prod.bt_crypto_events` cuando la clave del rango móvil ya no aparece en staging. Todo esto ocurre sólo si ambas validaciones de GE se aprueban, garantizando integridad antes de tocar producción.
