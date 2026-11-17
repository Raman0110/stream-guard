.PHONY: up down train test lint demo

up:
	docker compose up -d --build

demo:
	docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
	@echo "Dashboard: http://localhost:8501"
	@echo "API docs:  http://localhost:8000/docs"

down:
	docker compose down

train:
	python -m ml.train

test:
	python -m pytest --cov=common --cov=producer --cov=consumer --cov=ml --cov=api --cov=monitoring --cov-report=term-missing

lint:
	ruff check .
	ruff format --check .
	mypy
