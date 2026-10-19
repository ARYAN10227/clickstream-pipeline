# clickpipe: single command surface. Run `make` or `make help` to list targets.

RATE ?= 20000
DURATION ?= 300
N ?= 1000000
DUP ?= 0.05
BAD ?= 0.01
LATE ?= 0.01

.DEFAULT_GOAL := help

.PHONY: help test test-int test-e2e cov lint fmt \
	up down logs ps topics ch smoke bench replay crash-test

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run unit tests
	uv run pytest tests/unit -q

test-int: ## Run integration tests (needs Docker)
	uv run pytest tests/integration -q -m integration

test-e2e: ## Run end-to-end tests (needs the full stack)
	uv run pytest tests/e2e -q -m e2e

cov: ## Run unit + integration tests with coverage (fails under 80%)
	uv run pytest tests/unit tests/integration --cov=clickpipe --cov-report=term-missing --cov-fail-under=80

lint: ## Check lint and formatting with ruff
	uv run ruff check . && uv run ruff format --check .

fmt: ## Format code and apply ruff autofixes
	uv run ruff format . && uv run ruff check --fix .

up: ## Start the local stack (Redpanda, ClickHouse, Prometheus, Grafana)
	@echo "not implemented yet (step 5)"; exit 1

down: ## Stop the local stack
	@echo "not implemented yet (step 5)"; exit 1

logs: ## Tail logs from the local stack
	@echo "not implemented yet (step 5)"; exit 1

ps: ## Show local stack container status
	@echo "not implemented yet (step 5)"; exit 1

topics: ## Create/list Redpanda topics (clicks, clicks-dlq)
	@echo "not implemented yet (step 5)"; exit 1

ch: ## Open a ClickHouse client shell
	@echo "not implemented yet (step 6)"; exit 1

smoke: ## Send a few events end to end and check they land in ClickHouse
	@echo "not implemented yet (step 8)"; exit 1

bench: ## Run throughput benchmark (RATE, DURATION)
	@echo "not implemented yet (step 38)"; exit 1

replay: ## Replay N events with duplicates/bad/late records (N, DUP, BAD, LATE)
	@echo "not implemented yet (step 26)"; exit 1

crash-test: ## kill -9 the consumer mid-run and check for zero loss
	@echo "not implemented yet (step 27)"; exit 1
