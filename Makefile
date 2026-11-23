GO  ?= go
DSN ?= postgres://durable:durable@localhost:5432/durable_tasks?sslmode=disable

.PHONY: up down reset psql build migrate test test-short lint fmt crash-test bench

## up: start Postgres in the background and wait until it is healthy
up:
	docker compose up -d --wait postgres

## down: stop containers (keeps the data volume)
down:
	docker compose down

## reset: stop containers and delete the data volume
reset:
	docker compose down -v

## psql: open a psql shell inside the Postgres container
psql:
	docker compose exec postgres psql -U durable -d durable_tasks

build:
	$(GO) build -o bin/durable-tasks ./cmd/durable-tasks

## migrate: apply migrations to the compose database
migrate: build
	./bin/durable-tasks migrate --dsn '$(DSN)'

## crash-test: start Postgres, build, and run the crash test (about a minute)
crash-test: up build
	./bin/durable-tasks nemesis --dsn '$(DSN)'

## bench: measure throughput and per-task overhead
bench: up build
	./bin/durable-tasks bench --dsn '$(DSN)'

## test: all tests, including integration tests (needs Docker), with the race detector
test:
	$(GO) test -race -count=1 ./...

## test-short: unit tests only (no Docker)
test-short:
	$(GO) test -short -race ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	$(GO) vet ./...
	@command -v golangci-lint >/dev/null && golangci-lint run || echo "golangci-lint not installed; skipped (CI runs it)"

fmt:
	gofmt -w .
