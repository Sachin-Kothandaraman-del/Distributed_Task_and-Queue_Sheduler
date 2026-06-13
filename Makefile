.PHONY: install dev test up down logs api worker scheduler all load fmt

install:
	pip install -r requirements.txt && pip install -e .

dev:
	pip install -r requirements-dev.txt && pip install -e .

test:
	pytest -q

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

api:
	python -m dtq api

worker:
	python -m dtq worker

scheduler:
	python -m dtq scheduler

all:
	python -m dtq all

load:
	python -m examples.loadtest --count 100000 --concurrency 16
