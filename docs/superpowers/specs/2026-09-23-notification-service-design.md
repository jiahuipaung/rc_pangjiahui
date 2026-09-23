# Reliable HTTP Notification Service Design

## 1. Purpose

This project implements an internal service that accepts outbound HTTP notification requests from trusted business systems and delivers them to pre-registered external suppliers as reliably as practical.

The first version is an MVP intended to demonstrate explicit system boundaries, production-oriented Go engineering, failure handling, and justified trade-offs. The target load is approximately 100 accepted notifications per second. Notification records are retained for 30 days, and an individual request body is limited to 256 KiB.

The service returns after durable acceptance. It does not wait for or return the external supplier's response.

## 2. Goals

- Durably accept authenticated notification requests.
- Deliver external HTTP requests with at-least-once semantics.
- Decouple API availability from supplier availability through RabbitMQ.
- Resolve PostgreSQL and RabbitMQ consistency with a Transactional Outbox.
- Retry transient failures with bounded exponential backoff and jitter.
- Expose task status without exposing supplier credentials or payload data.
- Run locally and in CI with a reproducible Docker Compose environment.
- Support multiple publisher and worker instances without duplicate state transitions.

## 3. Non-goals

- Exactly-once delivery to arbitrary third-party HTTP APIs.
- Business-level payload transformation or a mapping/template DSL.
- Returning supplier responses synchronously to callers.
- Global or per-destination ordering across notifications.
- Online destination administration, RBAC, billing, or a management UI.
- Business compensation after a supplier has accepted a request.
- Infinite retry or automatic replay of permanently failed tasks.
- Persisting full supplier response bodies.
- Kubernetes manifests, distributed tracing infrastructure, or a workflow engine.

These concerns either require cooperation from external suppliers, form independent products, or add operational complexity that is not justified by the MVP load.

## 4. Assumptions and Constraints

- Go 1.27 is the language baseline. The latest available Go 1.27 patch release is used in CI and container images.
- PostgreSQL is the system of record for notification and Outbox state.
- RabbitMQ is a durable transport and load buffer, not the source of truth.
- Production deployments use HTTPS destinations. Plain HTTP requires an explicit development-only override.
- Callers provide a JSON payload already matching the supplier's expected body schema.
- Destinations are declared in YAML. Secret values are referenced by environment-variable name and are never committed.
- A single deployment can run all roles in one process. Production-like deployments may run API, publisher, worker, and scheduler roles independently from the same binary.

## 5. Architecture

```text
Business systems
      |
      | POST /v1/notifications
      v
Notification API
      |
      | one PostgreSQL transaction
      +--------------------------+
      | notification_tasks       |
      | outbox_events            |
      +--------------------------+
                   |
                   v
           Outbox Publisher
                   |
                   | persistent publish + publisher confirm
                   v
               RabbitMQ
                   |
                   | manual consumer acknowledgement
                   v
            Delivery Worker
                   |
                   +------> External HTTPS supplier
                   |
                   +------> PostgreSQL task result

Retry Scheduler -- scans due/expired tasks --> new Outbox events
```

### 5.1 Notification API

The API authenticates the internal caller, validates the request, resolves the configured destination, and constructs an immutable delivery snapshot. It creates the task and its initial Outbox event in one PostgreSQL transaction. A committed transaction returns `202 Accepted`; no RabbitMQ or supplier call occurs on the request path.

### 5.2 Outbox Publisher

The publisher claims unpublished events in short PostgreSQL transactions using `FOR UPDATE SKIP LOCKED`, a random `claim_token`, and a bounded `claim_until`. It commits before performing network I/O, publishes persistent messages, waits for RabbitMQ Publisher Confirms, and marks confirmed events published with a compare-and-swap update on the claim token.

A crash or lost confirm may publish an event more than once. This is intentional under at-least-once semantics; consumers must tolerate duplicate messages.

### 5.3 Delivery Worker

RabbitMQ messages contain only an event identifier and notification identifier. A worker claims an eligible task using a database compare-and-swap transition to `delivering` and assigns a `lease_token` and `lease_until`. It commits the claim before making the HTTP request, then applies the result only if it still owns the lease.

The worker acknowledges the RabbitMQ message only after the database outcome has committed. Redelivery of a message for a terminal or currently owned task is safe. A crash after the supplier accepts the request but before the database update can cause a repeated supplier call; this is the unavoidable duplicate window for a generic HTTP destination.

### 5.4 Retry Scheduler

The scheduler scans due `retry_wait` tasks and expired `delivering` leases in bounded batches. Multiple instances divide work using `FOR UPDATE SKIP LOCKED`. The state transition and creation of the next Outbox event occur in one PostgreSQL transaction. RabbitMQ is not used as a multi-hour delay store.

### 5.5 PostgreSQL and RabbitMQ Roles

PostgreSQL stores authoritative task state, immutable request snapshots, attempt counters, scheduling timestamps, error summaries, and Outbox events. RabbitMQ provides asynchronous transport, manual acknowledgements, consumer scaling, and traffic buffering.

No distributed transaction is attempted between them. Atomic local writes plus an idempotent Outbox publisher and duplicate-tolerant worker provide eventual consistency.

## 6. External API

### 6.1 Create a Notification

```http
POST /v1/notifications
Authorization: Bearer <internal-service-token>
Idempotency-Key: order-20260923-10001
Content-Type: application/json
```

```json
{
  "destination_id": "inventory-system",
  "payload": {
    "order_id": "10001",
    "sku": "ABC-001",
    "quantity": 2
  }
}
```

Successful durable acceptance returns:

```http
HTTP/1.1 202 Accepted
```

```json
{
  "notification_id": "01K...",
  "status": "pending"
}
```

Rules:

- `Idempotency-Key` is required and is unique together with `caller_id`.
- `destination_id` must exist in the startup configuration.
- The configured method, URL, and headers cannot be overridden by a request.
- The JSON request body is limited to 256 KiB.
- Repeating the same key and canonical request content returns the existing task.
- Reusing the key with different request content returns `409 Conflict`.
- Malformed requests return `400`, authentication failures return `401`, unknown destinations return `422`, and oversized bodies return `413`.

### 6.2 Query a Notification

```http
GET /v1/notifications/{notification_id}
Authorization: Bearer <internal-service-token>
```

The response includes status, attempt count, timestamps, the next scheduled attempt, the last HTTP status, and a sanitized error code/message. It does not return payloads, supplier credentials, or resolved secret headers. A caller may only query its own notifications.

### 6.3 Replay a Dead Notification

```http
POST /v1/admin/notifications/{notification_id}/replay
Authorization: Bearer <admin-token>
```

Only `dead` tasks may be replayed. Replay resets scheduling state, creates an Outbox event in the same transaction, and emits an audit log. It does not reset the historical attempt count. The endpoint is deliberately narrow and does not provide arbitrary state mutation.

## 7. Destination Configuration

A destination declaration contains:

- stable destination identifier;
- fixed HTTP method and HTTPS URL;
- content type and static non-secret headers;
- secret header names mapped to environment-variable references;
- request timeout;
- maximum attempts and task lifetime;
- optional per-destination concurrency limit.

Startup validation rejects duplicate identifiers, unsupported methods, malformed URLs, missing secret environment variables, invalid retry bounds, and unsafe network targets. A request stores a snapshot of the destination identifier, method, URL, non-secret headers, secret references, timeout, and retry policy. Secret values themselves are resolved at delivery time and never stored in PostgreSQL.

Redirects are disabled unless the destination explicitly allows a same-host redirect. Cross-host redirects are rejected. Production configuration rejects loopback, link-local, private, multicast, unspecified, and cloud metadata destinations unless an explicit deployment-level allowlist permits the exact network range. DNS results are revalidated when a connection is established to limit DNS-rebinding risk.

## 8. Data Model

### 8.1 `notification_tasks`

Key fields:

- `id`: sortable application-generated identifier;
- `caller_id`, `idempotency_key`, and `request_hash`;
- `destination_id`;
- snapshotted `method`, `url`, non-secret headers, secret references, body, timeout, and retry policy;
- `status`;
- `attempt_count` and `next_attempt_at`;
- `lease_token` and `lease_until`;
- `last_http_status`, `last_error_code`, and sanitized `last_error_message`;
- `created_at`, `updated_at`, `delivered_at`, and `dead_at`.

The unique constraint `(caller_id, idempotency_key)` enforces logical request idempotency. The request hash detects key reuse with different content.

### 8.2 `outbox_events`

Key fields:

- `id` and `aggregate_id` (notification identifier);
- `event_type` and compact message payload;
- `created_at` and `published_at`;
- `claim_token` and `claim_until`;
- `publish_attempts` and sanitized last publish error.

An event-generation key uniquely identifies each logical dispatch generation so concurrent scheduler instances cannot create multiple logical retry events. Physical RabbitMQ publication may still be duplicated.

## 9. State Machine

```text
pending ---- claim ----> delivering ---- 2xx ----> delivered
                            |
                            | retryable failure
                            v
                        retry_wait
                            |
                            | next_attempt_at reached
                            +------------> delivering
                            |
                            | attempts/lifetime exhausted
                            v
                           dead

delivering -- expired lease --> retry_wait or dead
dead -- authenticated replay --> pending
```

Only explicit transitions are permitted. Every worker result update includes `WHERE lease_token = $token AND status = 'delivering'`; a worker that has lost its lease cannot overwrite a newer result.

## 10. Delivery and Failure Policy

Defaults:

- one HTTP attempt timeout: 5 seconds;
- maximum attempts: 8;
- retry delays: 5 seconds, 30 seconds, 2 minutes, 10 minutes, 30 minutes, 2 hours, and 8 hours;
- bounded random jitter on every computed delay;
- maximum task lifetime: 24 hours.

Result classification:

- `2xx`: delivered;
- network failure, timeout, `408`, `425`, `429`, and `5xx`: retryable;
- all other `4xx`: permanent failure;
- malformed or unsupported responses: retryable only when they represent transport failure, otherwise permanent.

A valid `Retry-After` from `429` or `503` takes precedence when it remains within the task lifetime. Attempts or lifetime exhaustion moves the task to `dead`. Retries are never performed by sleeping inside a worker goroutine.

At-least-once delivery is the explicit external contract. An idempotency key is forwarded in a configured header for suppliers that support deduplication, but supplier-side idempotency is not assumed.

## 11. Go Module Design

```text
cmd/notifier/                 process entry point and role selection
internal/
  notification/              domain model, state machine, failure classes
  destination/               YAML loading, validation, secret references
  intake/                    acceptance and idempotency use case
  delivery/                  leasing, HTTP delivery, result application
  outbox/                    claiming and publishing Outbox events
  retry/                     due retry and expired lease recovery
  store/postgres/            PostgreSQL repositories
  transport/httpapi/         routes, authentication, validation, responses
  messaging/rabbitmq/        RabbitMQ producer and consumer adapters
  observability/             logging, metrics, health and readiness
migrations/                   forward and rollback SQL migrations
configs/                      safe example destination configuration
deployments/                  Dockerfile and Docker Compose configuration
docs/                         architecture, implementation and AI-use notes
```

The domain and application packages define behavior and the narrow interfaces they consume. PostgreSQL, RabbitMQ, and HTTP packages implement adapters. Interfaces live near their consumers. The design does not create abstractions that have no testing or substitution value.

One binary supports `api`, `publisher`, `worker`, `scheduler`, and `all` roles. Graceful shutdown stops intake, cancels polling, waits for in-flight work up to a configured deadline, and then closes dependencies.

## 12. Technology Choices

- Go 1.27 and the standard library where practical;
- `net/http` with `chi` for small, explicit routing;
- `pgx/v5` for PostgreSQL transactions and pooling;
- `amqp091-go` for RabbitMQ;
- `slog` for structured logs;
- Prometheus Go client for metrics;
- YAML only for destination declarations;
- Docker Compose for local PostgreSQL and RabbitMQ;
- GitHub Actions for formatting, tests, race detection, vetting, static analysis, and integration tests.

An ORM, dependency-injection framework, and dynamic configuration service are intentionally excluded.

## 13. Observability and Operations

Structured logs use stable error codes and include `notification_id`, `destination_id`, `caller_id`, attempt number, event identifier, elapsed time, and a short lease-token fingerprint. Logs never include request bodies, authorization values, secret headers, complete URLs with query strings, or complete supplier response bodies.

Metrics cover:

- accepted and rejected requests and API latency;
- tasks by state;
- unpublished Outbox count and oldest event age;
- publish confirms, failures, and claim recoveries;
- delivery attempts, latency, and classified outcomes;
- retry scheduling, dead tasks, and expired lease recovery;
- RabbitMQ redeliveries.

High-cardinality identifiers are not metric labels. Destination identifiers may be labels only when their configured count is operationally bounded.

`/livez` reports process liveness. `/readyz` checks only dependencies required by the selected role: PostgreSQL for all roles, RabbitMQ for publishers/workers, and validated destination configuration for APIs/workers. `/metrics` is exposed separately and should be protected at the network layer.

Thirty-day retention is enforced by a bounded cleanup job that removes terminal tasks and their published Outbox rows in small batches. Active, retryable, or unpublished records are never deleted by retention cleanup.

## 14. Testing Strategy

- Unit tests cover state transitions, failure classification, request hashing, backoff/jitter bounds, configuration validation, and secret redaction.
- HTTP handler tests cover authentication, ownership, malformed JSON, unknown fields, size limits, idempotent replay, idempotency conflict, and response codes.
- Delivery tests use `httptest.Server` for success, permanent failure, retryable failure, timeout, redirect, truncated response, and `Retry-After` cases.
- PostgreSQL integration tests verify unique constraints, Outbox atomicity, `SKIP LOCKED` claim separation, compare-and-swap ownership, expired lease recovery, and concurrent scheduler behavior.
- RabbitMQ integration tests verify persistent publishing, Publisher Confirms, manual acknowledgements, duplicate messages, reconnect behavior, and redelivery after consumer interruption.
- An end-to-end test submits an API request and waits for a controlled supplier server to observe delivery.
- CI runs formatting checks, `go vet ./...`, the selected static analyzer, unit/integration tests, and `go test -race ./...` where compatible with the test grouping.

Time, identifier generation, jitter, HTTP transport, repositories, and message adapters are controllable in tests. Tests use eventual assertions with explicit deadlines rather than arbitrary long sleeps.

## 15. Security Controls

- Separate caller and administrator bearer tokens, compared in constant time.
- Caller identity is derived from server configuration, never accepted from a request header supplied by the caller.
- Strict JSON decoding rejects unknown fields and trailing documents.
- Request, header, timeout, response-read, and connection limits prevent unbounded resource use.
- Destination configuration and resolved IP addresses enforce the SSRF policy.
- TLS certificate verification remains enabled.
- Secrets are environment references, never serialized task values.
- Errors returned to callers do not expose database, broker, network, or secret details.
- Container images run as a non-root user with a minimal runtime image.

## 16. Evolution

### 16.1 Higher Throughput

Scale publisher and worker roles independently, add destination-level concurrency controls, partition or archive task tables, and tune batch claiming. If sustained throughput reaches a level where RabbitMQ or PostgreSQL becomes the measured bottleneck, evaluate broker sharding or Kafka based on observed traffic and ordering requirements.

### 16.2 Destination Governance

Move YAML declarations to an audited, versioned control plane with approvals. Move secret resolution to Vault or a cloud secret manager. Add destination pause, quota, canary, and controlled replay capabilities.

### 16.3 Data-plane Separation

At substantially higher scale, separate the configuration/control plane from the delivery data plane. Retain PostgreSQL as task-state authority while using a partitioned event log for high-volume transport only if measurements justify it.

### 16.4 More Complex Integrations

Add versioned, bounded signing or schema adapters for demonstrated supplier requirements. Do not introduce an unrestricted expression language. Preserve the invariant that credentials and destination authority remain server controlled.

## 17. Decisions and Rejected Alternatives

### 17.1 PostgreSQL-only Queue

The initial AI recommendation was a PostgreSQL task queue because it minimizes components and is sufficient for approximately 100 notifications per second. It was not selected because this exercise benefits from demonstrating the common database/message-broker consistency problem and its Transactional Outbox solution. PostgreSQL remains authoritative, and RabbitMQ adds buffering and independently scalable consumers.

### 17.2 Direct RabbitMQ Publication

Publishing directly from the request handler creates a failure window between database commit and broker publication. Reversing the order creates the opposite window. The Transactional Outbox avoids both without distributed transactions.

### 17.3 Two-phase Commit

Distributed/XA transactions would increase operational and implementation complexity, have limited ecosystem support, and still would not provide exactly-once effects at an arbitrary HTTP supplier. They are rejected.

### 17.4 Kafka, Microservices, Kubernetes, and Workflow Engines

These are excessive for the stated load and fixed workflow. A modular monolith, RabbitMQ, PostgreSQL, and Compose provide the required behavior with a smaller operational surface.

### 17.5 Dynamic Payload Templates and Online Administration

Both would create separate security, versioning, audit, and user-experience problems. Callers provide supplier-shaped payloads, while destination authority remains in deployment-controlled configuration for the MVP.

## 18. Acceptance Criteria

The MVP is complete when:

1. An authenticated caller can durably create and query a notification.
2. Task creation and its initial Outbox event are atomic.
3. A confirmed Outbox event is delivered through RabbitMQ to a worker.
4. A controlled HTTPS supplier receives the configured method, headers, idempotency key, and JSON body.
5. Transient failures retry according to policy without holding a worker open.
6. Permanent or exhausted failures enter `dead` and can be replayed only through the authenticated admin endpoint.
7. Duplicate API requests, Outbox publications, and RabbitMQ deliveries do not create duplicate logical tasks or invalid state transitions.
8. Worker and publisher crashes recover through leases without leaving tasks permanently stuck.
9. Logs, API responses, and persisted records do not expose supplier secret values.
10. Unit, integration, end-to-end, race, vet, and static-analysis checks pass in the documented development workflow.

