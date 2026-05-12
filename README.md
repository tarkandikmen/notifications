# notifications

Event-driven notification system: SMS, email, and push delivery with rate limiting, retries, idempotency, dead-letter routing, and end-to-end tracing. One Go binary, five run modes, Postgres + Kafka + Redis.

## Quickstart

Requirements: Docker and Docker Compose. Go 1.25+ is only needed for local `go test` and `make build`; `make lint` runs inside a pinned Docker image and needs no host Go toolchain.

1. **Set a real `WEBHOOK_URL`.** The committed `.env` ships with a placeholder. Replace `REPLACE-WITH-YOUR-UUID` with a UUID from [webhook.site](https://webhook.site/) so you can watch deliveries land:

   ```bash
   sed -i.bak 's|webhook.site/REPLACE-WITH-YOUR-UUID|webhook.site/<your-uuid>|' .env
   ```

   Every binary refuses to start while the placeholder is in place — fail-fast guard from `internal/config/config.go`.

2. **Bring the stack up.** This builds the binary image and starts every dep + every binary:

   ```bash
   make up
   # equivalent to: docker-compose up -d --build
   ```

   Within ~90 s, the service set is live: `db`, `redis`, `kafka`, `jaeger`, the one-shot `migrate` and `kafka-bootstrap` jobs (which exit cleanly), then `api`, `dispatcher`, `relay`, `reaper`, `worker-sms`, `worker-email`, `worker-push`.

3. **Check health.** Every binary exposes `/healthz` on its metrics port (defaults to `:9090` inside the container; see [Observability](#observability) for the host-port table). The api binary additionally exposes `/healthz` on `:8080`:

   ```bash
   curl -fsS http://localhost:8080/healthz
   # → {"status":"ok"}
   ```

   When a dependency fails its probe, `/healthz` returns 503 with `{"status":"unhealthy","components":{"<name>":"<error>"}}` listing only the failing components. Component names: `postgres`, `redis`, `kafka`. Probes run in parallel inside a 2 s budget per request.

4. **Send a notification.**

   ```bash
   curl -fsS -X POST http://localhost:8080/v1/notifications \
     -H 'Content-Type: application/json' \
     -d '{
       "channel": "sms",
       "recipient": "+905551234567",
       "content": "hello",
       "idempotency_key": "00000000-0000-4000-8000-000000000001"
     }'
   # → {"id":"<uuid>"}
   ```

   The dispatcher claims the row within `~100 ms`, the relay pushes it to Kafka, the worker calls webhook.site, and the row reaches `DELIVERED`. Watch the lifecycle:

   ```bash
   curl -fsS http://localhost:8080/v1/notifications/<id> | jq
   ```

5. **Tear down.**

   ```bash
   make down
   # equivalent to: docker-compose down -v
   ```

## Subcommands

The single `notifications` binary multiplexes every run mode via cobra subcommands. `docker-compose` runs each one in its own container; you can also run them locally against a started stack.

| Subcommand | Purpose |
|---|---|
| `notifications api` | Serve the HTTP API on `HTTP_ADDR` (default `:8080`) and the metrics + healthz on `METRICS_ADDR` (default `:9090`). |
| `notifications dispatcher` | Claim eligible `notifications` rows and emit `send.<channel>` outbox rows. Lag-aware: pauses when consumer-group lag is high. |
| `notifications worker --channel=<sms\|email\|push>` | Consume `send.<channel>`, rate-limit via Redis token bucket, call the provider, write `delivery_attempts` + emit `events.notification`. |
| `notifications relay` | Drain unpublished `outbox` rows to Kafka. Producer-side dual-write avoidance for every other binary. |
| `notifications reaper` | Reset stuck `DISPATCHED` rows back to `PENDING` (or terminal-fail when `max_attempts` is exhausted). Lag-aware. |
| `notifications migrate up` / `notifications migrate down` | Apply / revert database migrations using `golang-migrate`. |
| `notifications kafka-bootstrap` | One-shot: create the topic set (`send.<channel>`, `events.notification`, `send.<channel>.dlq`) idempotently and exit. |

The `--help` of every subcommand carries the per-subcommand flag set.

## Configuration

Configuration is read from environment variables; the binary calls `godotenv.Load()` at startup, so a committed `.env` at the repo root supplies defaults (and pre-existing OS env vars override file values).

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | — | Postgres DSN, e.g. `postgres://user:pass@host:5432/notifications?sslmode=disable`. |
| `REDIS_URL` | yes | — | Redis URL, e.g. `redis://redis:6379`. |
| `KAFKA_BROKERS` | yes | — | Comma-separated bootstrap brokers, e.g. `kafka:9092`. |
| `WEBHOOK_URL` | yes | — | The provider endpoint workers call. The committed placeholder `https://webhook.site/REPLACE-WITH-YOUR-UUID` is rejected at startup. |
| `HTTP_ADDR` | no | `:8080` | api binary's main HTTP listener. |
| `METRICS_ADDR` | no | `:9090` | Per-binary `/metrics` + `/healthz` listener. Every binary exposes this. |
| `LOG_LEVEL` | no | `info` | One of `debug`, `info`, `warn`, `error`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | (empty → stdout exporter) | OTLP/gRPC endpoint, e.g. `jaeger:4317`. |

Missing required variables (or a placeholder `WEBHOOK_URL`) cause the binary to exit non-zero before any dep is touched. See `internal/config/config.go` for the validation rules.

## How data moves

```
   HTTP
    │
    ▼
  [api] ──────────────────► Postgres (notifications)
                                │
                                │  poll for eligible rows
  [dispatcher] ◄────────────────┤  (lag-aware: pause if consumer lag is high)
        │                       │  claim atomically (FOR UPDATE SKIP LOCKED)
        │                       │  insert outbox row in same tx
        ▼                       │
     Postgres ──► [relay] ──► Kafka(send.<channel>)
                                       │
                                       ▼
                                  [worker] ──► provider (webhook.site)
                                       │       (rate-limited via Redis token bucket)
                                       │       writes delivery_attempts
                                       │       writes outbox row(events.notification)
                                       ▼
                                   Postgres ──► [relay] ──► Kafka(events.notification)

  [reaper] runs continuously: resets stuck DISPATCHED rows back to PENDING,
           but skips resets while consumer lag is high.
```

The transactional outbox is the single Postgres↔Kafka bridge: every producer (api, dispatcher, worker, reaper) writes to **only Postgres** in any one transaction, and the relay alone publishes outbox rows to Kafka. Worker idempotency is three layers (state guard, `ON CONFLICT DO NOTHING` on the unique `(notification_id, attempt)` constraint, attempt-guarded UPDATE).

## Observability

`docker-compose` exposes one metrics + healthz port per binary on the host. All ports inside the container are `:9090`; the host-port mapping disambiguates:

| Service | Host port | Endpoints |
|---|---|---|
| `api` | `8080` | `/v1/notifications/*`, `/healthz`, `/metrics` |
| `api` (metrics) | `9090` | `/healthz`, `/metrics` (same registry as :8080) |
| `dispatcher` | `9091` | `/healthz`, `/metrics` |
| `worker-sms` | `9082` | `/healthz`, `/metrics` |
| `worker-email` | `9093` | `/healthz`, `/metrics` |
| `worker-push` | `9094` | `/healthz`, `/metrics` |
| `relay` | `9095` | `/healthz`, `/metrics` |
| `reaper` | `9096` | `/healthz`, `/metrics` |
| `jaeger` | `16686` | Web UI for distributed traces |
| `jaeger` | `4317` | OTLP/gRPC ingest (every binary exports here when `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4317`) |

Open the Jaeger UI at <http://localhost:16686> and inspect traces by service. The `api` service surfaces inbound HTTP request traces in their own root trace (the `notifications` table has no trace‑headers column to bridge to the dispatcher). The `dispatcher`/`worker` services surface a separate joined trace per row: `dispatcher.tick → dispatcher.row → worker.handleRecord → provider call`, with W3C Trace Context propagated end‑to‑end via the outbox `headers` column and Kafka record headers. Structured logs include `trace_id` + `span_id` automatically when emitted under an active span context.

## Common Makefile targets

| Target | What it does |
|---|---|
| `make up` | `docker-compose up -d --build` — bring the stack up. |
| `make down` | `docker-compose down -v` — tear it down, drop volumes. |
| `make build` | `go build -o notifications ./cmd/notifications` — local binary. |
| `make test` | `go test ./...` — unit tests (integration tests are gated by `TEST_INTEGRATION=1`). |
| `make test-integration` | Runs the full testcontainer-backed suite (`TEST_INTEGRATION=1 go test -timeout 14m` over `internal/itest`, `store`, `dispatcher`, `relay`, `worker`, `reaper`, `ratelimit`, `kafkaadmin`, `metrics`, `observability`, `metricsserver`, `health`). Mirrors `.github/workflows/ci.yml`; requires a running Docker daemon. |
| `make vet` | `go vet ./...`. |
| `make lint` | Runs `golangci-lint run --timeout=3m ./...` inside the pinned `golangci/golangci-lint:v2.12.2` image (no host install needed). Linters: `govet`, `errcheck`, `staticcheck`, `ineffassign`, `unused`, `bodyclose`. Config in `.golangci.yml`; version matches `.github/workflows/ci.yml`. Override the tag with `make lint LINT_IMAGE=golangci/golangci-lint:<tag>`. |
| `make migrate` | Apply every pending migration against `DATABASE_URL`. |
| `make migrate-down` | Revert every applied migration. |

Integration tests require Docker and use `testcontainers-go` to spin up real Postgres / Kafka / Redis. The single-command path matches CI:

```bash
make test-integration
```

The OpenAPI 3.1 description of the HTTP API lives in `docs/openapi.yaml` and is linted by Spectral in CI.
