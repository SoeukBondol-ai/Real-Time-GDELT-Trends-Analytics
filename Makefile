SHELL := /bin/bash

.PHONY: help copy-env up down restart logs ps topics backend frontend producer spark clean

help:
	@echo "Available commands:"
	@echo "  make copy-env     Copy .env.example to .env"
	@echo "  make up           Start the full stack"
	@echo "  make down         Stop the full stack"
	@echo "  make restart      Restart the full stack"
	@echo "  make logs         Follow all logs"
	@echo "  make ps           Show service status"
	@echo "  make topics       List Kafka topics"
	@echo "  make backend      Follow backend logs"
	@echo "  make frontend     Follow frontend logs"
	@echo "  make producer     Follow producer logs"
	@echo "  make spark        Follow Spark streaming logs"
	@echo "  make clean        Stop and remove volumes"

copy-env:
	@test -f .env || cp .env.example .env
	@echo ".env is ready"

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d --build

logs:
	docker compose logs -f

ps:
	docker compose ps

topics:
	docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

backend:
	docker compose logs -f backend

frontend:
	docker compose logs -f frontend

producer:
	docker compose logs -f producer

spark:
	docker compose logs -f spark-trends

clean:
	docker compose down -v
	rm -rf logs data spark/checkpoints spark/warehouse
