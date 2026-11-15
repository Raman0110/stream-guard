# Targets for later milestones (train, load-test, demo) are added as they are built.
.PHONY: up down test lint

up:
	docker compose up -d --build

down:
	docker compose down

test:
	python -m pytest --cov=common --cov=producer --cov=consumer --cov-report=term-missing

lint:
	ruff check .
	ruff format --check .
	mypy
