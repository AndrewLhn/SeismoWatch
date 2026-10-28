COMPOSE := docker compose
BACKFILL_DS ?= 2024-01-01

.PHONY: help up down ps logs validate lint test e2e backfill dbt-build dbt-compile

help: ## List every target
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "%-14s %s\n", $$1, $$2}'

up: ## Start the full local stack
	$(COMPOSE) up -d --build

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Tail service logs
	$(COMPOSE) logs -f

validate: ## Static checks: compose config, byte-compile, lint
	$(COMPOSE) config --quiet
	python3 -m compileall -q dags tests infra
	$(MAKE) lint

lint: ## Ruff over all Python sources
	ruff check dags tests infra

test: ## Unit tests with coverage (needs requirements-dev.txt on the host)
	python3 -m coverage run -m unittest discover -s tests -v
	python3 -m coverage report --include="dags/*" --skip-empty

e2e: ## Full stack run against a historical USGS window, with assertions
	bash scripts/e2e.sh

backfill: ## Run the pipeline once for a date: make backfill BACKFILL_DS=2024-01-02
	$(COMPOSE) exec -T -e SEISMIC_BACKFILL_DS=$(BACKFILL_DS) airflow-standalone \
		airflow dags test seismic_event_pipeline

dbt-build: ## dbt build inside the stack's isolated dbt venv
	$(COMPOSE) exec -T airflow-standalone bash -c \
		'cd /opt/airflow/dbt && /home/airflow/dbt_env/bin/dbt build --profiles-dir . --target-path /tmp/dbt/target --log-path /tmp/dbt/logs'

dbt-compile: ## Render dbt SQL without executing it
	$(COMPOSE) exec -T airflow-standalone bash -c \
		'cd /opt/airflow/dbt && /home/airflow/dbt_env/bin/dbt compile --profiles-dir . --target-path /tmp/dbt/target --log-path /tmp/dbt/logs'
