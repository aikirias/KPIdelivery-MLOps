COMPOSE_FILE = infrastructure/docker-compose.yml
COMPOSE      = docker compose -f $(COMPOSE_FILE)
LOG_DIR      = airflow/logs

.PHONY: init up down destroy reset ps logs dag-run dag-status clean

init:
	$(COMPOSE) up airflow-init

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

destroy:
	$(COMPOSE) down -v
	chmod -R u+w $(LOG_DIR) airflow/logs_old 2>/dev/null || true
	rm -rf $(LOG_DIR) airflow/logs_old
	mkdir -p $(LOG_DIR)

reset: destroy init up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

dag-run:
	$(COMPOSE) exec airflow-webserver airflow dags trigger crypto_events_dag

dag-status:
	$(COMPOSE) exec airflow-webserver airflow dags list-runs -d crypto_events_dag

clean:
	chmod -R u+w $(LOG_DIR) airflow/logs_old 2>/dev/null || true
	rm -rf $(LOG_DIR) airflow/logs_old
	mkdir -p $(LOG_DIR)
