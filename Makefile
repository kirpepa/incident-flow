.DEFAULT_GOAL := help

GOTOOLCHAIN ?= go1.25.13
export GOTOOLCHAIN

.PHONY: help fmt lint test test-race integration cover build vuln openapi verify compose-up compose-down migrate seed demo

help:
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*?## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format Go sources
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

lint: ## Run static analysis
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

test: ## Run unit tests
	go test ./...

test-race: ## Run tests with the race detector
	go test -race ./...

integration: ## Run tests against real PostgreSQL, NATS and MCP
	go test -count=1 -tags=integration -v ./tests/integration

cover: ## Write an HTML coverage report
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

build: ## Build every binary
	go build ./cmd/...

vuln: ## Check reachable Go vulnerabilities
	go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...

openapi: ## Lint the OpenAPI contract
	npx --yes @redocly/cli@2.51.2 lint api/openapi.yaml

verify: ## Run the same fast quality gates as CI
	test -z "$$(gofmt -l .)"
	$(MAKE) lint test-race build vuln openapi
	docker compose config --quiet

compose-up: ## Start local infrastructure and applications
	docker compose up --build -d

compose-down: ## Stop local stack and remove containers
	docker compose down

migrate: ## Apply database migrations
	go run ./cmd/migrate

seed: ## Seed the local demo tenant and escalation policy
	go run ./cmd/seed

demo: ## Run the deterministic alert-storm demo
	./scripts/demo.sh
