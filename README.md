# Reliable HTTP Notification Service

A production-oriented Go service that durably accepts outbound HTTP notifications and delivers them to pre-registered suppliers. PostgreSQL is the system of record; RabbitMQ is the asynchronous transport. The target workload is roughly 100 accepted notifications per second.

The detailed design and decision record is in [the architecture specification](docs/superpowers/specs/2026-09-23-notification-service-design.md). AI-assisted work is disclosed in [docs/ai-usage.md](docs/ai-usage.md).

## Architecture

```text
caller -> API -> PostgreSQL (task + outbox in one transaction)
                         |
                         v
                  outbox publisher -> RabbitMQ -> delivery worker -> HTTPS supplier
                         ^                              |
                         |                              v
                   retry scheduler <------------- task result/lease
```

One binary exposes `api`, `publisher`, `worker`, `scheduler`, and development-only `all` roles. The API returns `202 Accepted` only after committing the task and its initial Outbox event. The publisher uses short `SKIP LOCKED` claims and RabbitMQ publisher confirms. Workers use expiring leases and compare-and-swap updates; the scheduler recovers due retries and expired leases.

## Delivery semantics

Delivery is **at least once**, not exactly once. A crash after a supplier accepts a request but before PostgreSQL records success can result in a repeated HTTP call. The service sends the caller's idempotency key to the supplier as `Idempotency-Key`; suppliers should deduplicate it when possible.

DB/MQ consistency uses a Transactional Outbox:

1. API transaction atomically writes `notification_tasks` and `outbox_events`.
2. Publisher commits its database claim before network I/O.
3. A confirmed publish is marked with a claim-token CAS update.
4. A crash or lost confirm can republish, while worker state transitions remain idempotent.

No database transaction remains open during RabbitMQ or supplier network calls.

## Quick start

Requirements: Docker with Compose v2. The Compose credentials and token values are local-development placeholders and must not be reused in production.

```bash
docker compose -f deployments/compose.yaml up -d --build
curl -fsS http://localhost:8080/readyz
```

The included destination points to `https://inventory.example.com` and is illustrative; replace it and supply its referenced secret before expecting successful delivery. Stop and remove local state with:

```bash
docker compose -f deployments/compose.yaml down -v
```

For direct process execution, set the role-specific variables and run `go run ./cmd/notifier -role api`. Common variables are:

| Variable | Required by | Purpose |
|---|---|---|
| `DATABASE_URL` | all roles | PostgreSQL DSN |
| `RABBITMQ_URL` | publisher, worker, all | AMQP URL |
| `DESTINATIONS_FILE` | API, worker, all | validated destination YAML |
| `CALLER_TOKENS` | API, all | comma-separated `token:caller-id` entries |
| `ADMIN_TOKENS` | API, all | comma-separated `token:operator-id` entries |
| `HTTP_ADDR` | API, all | listen address, default `:8080` |
| `METRICS_ADDR` | publisher, worker, scheduler | internal health/metrics address, default `:9090` |

Destination secrets are environment-variable references in YAML. Resolved values are never stored in PostgreSQL.

## API

Create a notification:

```bash
curl -i http://localhost:8080/v1/notifications \
  -H 'Authorization: Bearer caller-development-token' \
  -H 'Idempotency-Key: order-10001' \
  -H 'Content-Type: application/json' \
  --data '{"destination_id":"inventory-system","payload":{"order_id":"10001"}}'
```

Query it with `GET /v1/notifications/{notification_id}` and the same caller token. Replay a terminal dead task with `POST /v1/admin/notifications/{notification_id}/replay` and an admin token.

Important responses are `202` accepted, `400` malformed input, `401` authentication failure, `409` idempotency-key conflict, `413` body too large, and `422` unknown/invalid destination. Repeating a key with canonically equivalent JSON returns the original task.

## Retry and failure policy

- HTTP `2xx` is delivered; `408`, `425`, `429`, and `5xx` are retryable; other `4xx` responses are permanent.
- Network and timeout failures are retryable.
- Retry delays, maximum attempts, lifetime, timeout, and concurrency limit come from the immutable destination snapshot.
- Configured retry delays receive bounded random jitter of ±20%; an explicit supplier `Retry-After` value is honored without jitter.
- `concurrency_limit` caps simultaneous supplier calls for one destination within one worker process. Multiple worker replicas each enforce their own limit; it is not a distributed global quota.
- Due retries and expired leases create a new logical Outbox generation atomically.
- Exhausted and permanent failures become `dead`; replay is an explicit authenticated operation.
- Terminal records are retained for 30 days by the retention worker.

## Security and limits

Requests are capped at 256 KiB. Destinations are pre-registered, production URLs require HTTPS, redirects are constrained, and resolved IPs are checked against unsafe network ranges to reduce SSRF and DNS-rebinding risk. Logs and query responses omit payloads, authorization values, secrets, and full supplier response bodies.

Bearer tokens are intentionally simple for this assignment. Production evolution should use a secret manager and workload identity, rate limits at the edge, encrypted connections to PostgreSQL/RabbitMQ, structured audit export, and destination-level circuit breaking. Generic exactly-once delivery remains out of scope.

## Observability

API and `all` expose `/livez`, `/readyz`, and `/metrics` on `HTTP_ADDR`. Split publisher, worker, and scheduler processes expose the same operational endpoints on `METRICS_ADDR` inside their containers. Implemented counters cover intake outcomes, Outbox publish confirmations/failures, committed delivery outcomes, dead transitions, retry generations, and recovered delivery leases. Identifiers, URLs, payloads, and credentials are never metric labels.

## Verification

```bash
make fmt
make vet
make lint
make test
make test-race
TEST_DATABASE_URL='postgres://notifier:notifier@127.0.0.1:5432/notifier?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://notifier:notifier@127.0.0.1:5672/' \
  go test -tags=integration -race -timeout=90s ./...
docker build -f deployments/Dockerfile .
```

Integration tests require reachable PostgreSQL and RabbitMQ instances. Unit tests using `httptest` also require permission to bind loopback ports; highly restricted sandboxes may reject that operation even when the code is correct.

The integration suite includes a successful API-to-supplier path plus real PostgreSQL/RabbitMQ recovery cases for `503` retry scheduling, duplicate broker delivery, and expired worker leases. Component tests separately cover HTTP timeout, redirects, response truncation, `Retry-After`, publisher-confirm loss, stale CAS ownership, and authentication boundaries.

## Repository layout

- `cmd/notifier`: process entry point and role selection
- `internal/app`: dependency wiring and lifecycle
- `internal/store/postgres`: transactional repository and leases
- `internal/messaging/rabbitmq`: topology, confirms, and manual acknowledgements
- `internal/delivery`, `internal/outbox`, `internal/retry`: application services
- `internal/transport/httpapi`: authenticated HTTP API
- `migrations`: PostgreSQL schema
- `deployments`: image and local Compose stack

The modular-monolith shape keeps deployment flexible without premature service boundaries. At higher scale, roles can be scaled independently; online destination management, multi-region ownership, and stronger supplier-specific deduplication can be added without changing PostgreSQL's authoritative role.
