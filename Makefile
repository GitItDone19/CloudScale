.PHONY: help up down ps logs test test-unit test-integration data-gen lint clean

help:
	@echo "CloudScale - Developer CLI Shortcuts"
	@echo "======================================"
	@echo "make up               - Start the Docker Compose stack (Airflow + Spark + Postgres)"
	@echo "make down             - Stop all running containers"
	@echo "make ps               - Display container status"
	@echo "make logs             - Stream logs from all containers"
	@echo "make data-gen         - Generate synthetic dirty data for testing"
	@echo "make test             - Run all unit and integration tests"
	@echo "make test-unit        - Run unit tests only"
	@echo "make test-integration - Run DAG integrity tests"
	@echo "make lint             - Run code formatters and linters"
	@echo "make clean            - Clean temporary cache and pyc files"

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

data-gen:
	python -m data_generator.generate_legacy_data --count 1000

test:
	python -m pytest tests/

test-unit:
	python -m pytest tests/unit/

test-integration:
	python -m pytest tests/integration/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
