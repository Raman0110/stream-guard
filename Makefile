.PHONY: up down train test lint scenario demo demo-reset load-test

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

# Guided demo: normal traffic -> anomalies -> gradual drift (drift alert shows on the dashboard).
demo:
	python -m scripts.demo

# Back to normal producer settings after the demo.
demo-reset:
	docker compose up -d --force-recreate producer

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
