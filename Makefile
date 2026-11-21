.PHONY: up down train test lint scenario load-test

up:
	docker compose up -d --build

scenario:
	docker compose -f docker-compose.yml -f docker-compose.scenario.yml up -d --build
	@echo "Dashboard: http://localhost:8501"
	@echo "API docs:  http://localhost:8000/docs"

down:
	docker compose down

train:
	python -m ml.train

# Needs the stack running (`make up`). Two runs: steady load for latency, unthrottled for peak throughput.
load-test:
	python -m scripts.load_test --events 3000 --rate 100 --report reports/load_test_steady.md
	python -m scripts.load_test --events 10000 --rate 0 --report reports/load_test_peak.md

test:
	python -m pytest --cov=common --cov=producer --cov=consumer --cov=ml --cov=api --cov=monitoring --cov=scripts --cov-report=term-missing

lint:
	ruff check .
	ruff format --check .
	mypy
