.PHONY: up down build test test-integration vet migrate migrate-down lint

up:
	docker-compose up -d --build

down:
	docker-compose down -v

build:
	go build -o notifications ./cmd/notifications

test:
	go test ./...

# Mirrors the integration job in .github/workflows/ci.yml. Boots
# Postgres / Kafka / Redis via testcontainers-go, so requires a
# running Docker daemon. The package list and 14m timeout match CI
# verbatim — bump both sites together when adding a testcontainer
# package or recalibrating the budget.
test-integration:
	TEST_INTEGRATION=1 go test -timeout 14m \
		./internal/itest/... \
		./internal/store/... \
		./internal/dispatcher/... \
		./internal/relay/... \
		./internal/worker/... \
		./internal/reaper/... \
		./internal/ratelimit/... \
		./internal/kafkaadmin/... \
		./internal/metrics/... \
		./internal/observability/... \
		./internal/metricsserver/... \
		./internal/health/...

vet:
	go vet ./...

migrate:
	go run ./cmd/notifications migrate up

migrate-down:
	go run ./cmd/notifications migrate down

# Pin to the same golangci-lint version CI runs (.github/workflows/ci.yml,
# .golangci.yml) so local lint results match CI. Bump all three in lockstep.
LINT_IMAGE ?= golangci/golangci-lint:v2.12.2

lint:
	docker run --rm \
		-v $(CURDIR):/app \
		-w /app \
		$(LINT_IMAGE) \
		golangci-lint run --timeout=3m ./...
