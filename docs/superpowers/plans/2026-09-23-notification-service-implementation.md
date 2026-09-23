# Reliable HTTP Notification Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-oriented Go service that durably accepts supplier notifications, publishes them through a Transactional Outbox and RabbitMQ, and delivers them to pre-registered HTTP endpoints with at-least-once semantics.

**Architecture:** A modular monolith exposes API, Outbox publisher, delivery worker, and retry scheduler roles from one binary. PostgreSQL is authoritative for task and Outbox state; RabbitMQ is an at-least-once transport. Short database claims, expiring leases, compare-and-swap updates, and idempotent state transitions make duplicate messages and process crashes recoverable.

**Tech Stack:** Go 1.27, `net/http`, `chi`, `pgx/v5`, `amqp091-go`, PostgreSQL, RabbitMQ, `slog`, Prometheus, YAML, Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-23-notification-service-design.md`

## Global Constraints

- Target approximately 100 accepted notifications per second.
- Limit each JSON request body to 256 KiB and retain terminal records for 30 days.
- Use Go 1.27 and pin the latest available Go 1.27 patch in CI and container images.
- Return `202 Accepted` only after the notification and initial Outbox event commit atomically.
- PostgreSQL is authoritative; RabbitMQ messages contain only event and notification identifiers.
- External HTTP delivery is at least once and may be duplicated after crashes.
- Production destinations are HTTPS and are selected only by pre-registered `destination_id`.
- Never persist or log resolved supplier secrets, request bodies, authorization values, or full response bodies.
- Do not hold database locks or transactions during RabbitMQ or supplier network I/O.
- Use deterministic clocks, identifier generators, and jitter sources in tests.
- Every task uses test-first changes and ends with an independently reviewable commit.

## Review Focus

- A repeated idempotency key with semantically identical JSON but different object-key order returns the original task; different content returns `409`.
- A publisher that loses its RabbitMQ confirm may republish, but the worker never performs an invalid duplicate state transition.
- A worker that finishes after its lease expired cannot overwrite a result recorded by the new lease owner.
- Redirects and DNS resolution cannot move an approved destination to a disallowed network address.
- Shutdown stops new claims, drains bounded in-flight work, and leaves uncompleted work recoverable after lease expiry.

## File Map

```text
go.mod                                  module definition and dependency versions
go.sum                                  dependency checksums
Makefile                                reproducible developer commands
.gitignore                              local artifact exclusions
.golangci.yml                           static-analysis configuration
.github/workflows/ci.yml                unit, race, integration and image checks
cmd/notifier/main.go                    process lifecycle and role selection
internal/app/app.go                     dependency construction and role orchestration
internal/config/config.go               process configuration
internal/destination/config.go          destination YAML schema and loading
internal/destination/validate.go        URL/network/retry validation
internal/destination/resolver.go        secret resolution and safe dial policy
internal/notification/model.go          task, event and status types
internal/notification/state.go          state transitions and failure classes
internal/notification/retry.go          deterministic retry calculation
internal/notification/hash.go           canonical request hashing
internal/intake/service.go               durable acceptance use case
internal/outbox/service.go               claim/publish/confirm loop
internal/delivery/service.go             task claim and result persistence
internal/delivery/http_sender.go         hardened supplier HTTP client
internal/retry/service.go                retry and expired-lease scheduling
internal/store/postgres/*.go             pgx repositories and transactions
internal/messaging/rabbitmq/*.go         broker topology, publisher and consumer
internal/transport/httpapi/*.go          handlers, middleware and JSON errors
internal/observability/*.go              logs, metrics and health handlers
internal/testsupport/*.go                fake clock, IDs, jitter and integration helpers
migrations/000001_initial.up.sql          initial PostgreSQL schema
migrations/000001_initial.down.sql        schema rollback
configs/destinations.example.yaml        safe destination example
deployments/Dockerfile                   non-root multi-stage image
deployments/compose.yaml                 API, publisher, worker, scheduler and dependencies
docs/ai-usage.md                          required AI-use disclosure
README.md                                 operation, API and trade-off guide
```

---

### Task 1: Repository Harness, Configuration, and Domain Primitives

**Files:**
- Create: `go.mod`
- Create: `.gitignore`
- Create: `Makefile`
- Create: `.golangci.yml`
- Create: `cmd/notifier/main.go`
- Create: `internal/config/config.go`
- Create: `internal/destination/config.go`
- Create: `internal/destination/validate.go`
- Create: `internal/destination/resolver.go`
- Create: `internal/destination/config_test.go`
- Create: `internal/notification/model.go`
- Create: `internal/notification/state.go`
- Create: `internal/notification/state_test.go`
- Create: `internal/notification/retry.go`
- Create: `internal/notification/retry_test.go`
- Create: `internal/notification/hash.go`
- Create: `internal/notification/hash_test.go`
- Create: `internal/testsupport/clock.go`
- Create: `internal/testsupport/random.go`
- Create: `configs/destinations.example.yaml`

**Interfaces:**
- Produces: `destination.Load(path string, lookupEnv func(string) (string, bool)) (destination.Registry, error)`.
- Produces: `Registry.Get(id string) (destination.Destination, bool)` and immutable destination snapshots.
- Produces: `notification.ClassifyResult(status int, err error) notification.Outcome`.
- Produces: `notification.NextAttempt(policy notification.RetryPolicy, attempt int, retryAfter *time.Time, now time.Time, jitter notification.Jitter) (time.Time, bool)`.
- Produces: `notification.RequestHash(destinationID string, payload json.RawMessage) ([32]byte, error)`.

- [ ] **Step 1: Initialize the Go module and failing domain/config tests**

Use module path `github.com/jiahuipaung/rc_pangjiahui`. Tests must pin these behaviors:

```go
func TestRequestHashCanonicalizesJSONObjectOrder(t *testing.T) {
    a, err := notification.RequestHash("inventory", json.RawMessage(`{"b":2,"a":1}`))
    require.NoError(t, err)
    b, err := notification.RequestHash("inventory", json.RawMessage(`{"a":1,"b":2}`))
    require.NoError(t, err)
    require.Equal(t, a, b)
}

func TestClassifyResult(t *testing.T) {
    require.Equal(t, notification.Delivered, notification.ClassifyResult(204, nil))
    require.Equal(t, notification.Retryable, notification.ClassifyResult(503, nil))
    require.Equal(t, notification.Permanent, notification.ClassifyResult(422, nil))
    require.Equal(t, notification.Retryable, notification.ClassifyResult(0, context.DeadlineExceeded))
}

func TestLoadRejectsUnsafeProductionDestination(t *testing.T) {
    _, err := destination.Load(fixturePath("loopback.yaml"), lookupFixtureEnv)
    require.ErrorContains(t, err, "disallowed destination network")
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `go test ./internal/notification ./internal/destination`

Expected: FAIL because the packages and exported functions do not exist.

- [ ] **Step 3: Implement minimal immutable domain and configuration types**

Use explicit types rather than stringly typed transitions:

```go
type Status string

const (
    StatusPending    Status = "pending"
    StatusDelivering Status = "delivering"
    StatusRetryWait  Status = "retry_wait"
    StatusDelivered  Status = "delivered"
    StatusDead       Status = "dead"
)

type RetryPolicy struct {
    MaxAttempts int
    Lifetime    time.Duration
    Delays      []time.Duration
}

type Destination struct {
    ID               string
    Method           string
    URL              *url.URL
    StaticHeaders    http.Header
    SecretHeaders    map[string]string // header name -> environment variable name
    Timeout          time.Duration
    Retry            notification.RetryPolicy
    ConcurrencyLimit int
}
```

Canonical request hashing must decode with `json.Decoder.UseNumber`, reject trailing JSON, re-encode deterministically with `encoding/json`, and include `destination_id` in the digest. Configuration loading must reject duplicate IDs, missing env references, unsupported methods, URL userinfo/fragments, unsafe production networks, invalid limits, and secret values embedded directly in YAML.

- [ ] **Step 4: Add reproducible developer commands and a compilable entry point**

`Makefile` targets:

```make
.PHONY: fmt test test-race vet lint integration up down
fmt:
	@test -z "$$(gofmt -l .)"
test:
	go test ./...
test-race:
	go test -race ./...
vet:
	go vet ./...
lint:
	golangci-lint run
integration:
	go test -tags=integration ./internal/integration/...
up:
	docker compose -f deployments/compose.yaml up -d postgres rabbitmq
down:
	docker compose -f deployments/compose.yaml down -v
```

`cmd/notifier/main.go` parses a validated role enum (`api`, `publisher`, `worker`, `scheduler`, `all`) and exits with a non-zero code for invalid configuration. Dependency construction remains in `internal/app`, added in later tasks.

- [ ] **Step 5: Run domain quality checks**

Run: `gofmt -w cmd internal && go test ./internal/notification ./internal/destination && go vet ./...`

Expected: PASS.

- [ ] **Step 6: Commit the repository foundation**

```bash
git add go.mod go.sum .gitignore Makefile .golangci.yml cmd internal configs
git commit -m "chore: initialize Go notification service"
```

---

### Task 2: PostgreSQL Schema and Transactional Repositories

**Files:**
- Create: `migrations/000001_initial.up.sql`
- Create: `migrations/000001_initial.down.sql`
- Create: `internal/store/postgres/store.go`
- Create: `internal/store/postgres/tx.go`
- Create: `internal/store/postgres/intake.go`
- Create: `internal/store/postgres/outbox.go`
- Create: `internal/store/postgres/delivery.go`
- Create: `internal/store/postgres/retry.go`
- Create: `internal/store/postgres/query.go`
- Create: `internal/store/postgres/store_integration_test.go`
- Create: `internal/testsupport/postgres.go`

**Interfaces:**
- Produces: `Store.WithTx(ctx context.Context, fn func(Tx) error) error`.
- Produces: transactional `CreateNotification(ctx context.Context, task notification.Task) error`, `InsertOutbox(ctx context.Context, event notification.OutboxEvent) error`, `ScheduleRetry(ctx context.Context, id, expectedLease string, next time.Time, result notification.AttemptResult) (bool, error)`, and `ReplayDead(ctx context.Context, id string, now time.Time) (bool, error)` methods.
- Produces: `ClaimOutbox(ctx context.Context, now time.Time, limit int, token string, until time.Time) ([]notification.OutboxEvent, error)`, `MarkPublished(ctx context.Context, eventID, token string, at time.Time) (bool, error)`, `ClaimDelivery(ctx context.Context, notificationID, leaseToken string, leaseUntil time.Time) (notification.Claim, error)`, and `ApplyDeliveryResult(ctx context.Context, notificationID, leaseToken string, result notification.AttemptResult) (bool, error)`.

- [ ] **Step 1: Write PostgreSQL integration tests before schema code**

Mark tests with `//go:build integration`. Cover atomic rollback, uniqueness, concurrent claims, stale tokens, and generation uniqueness:

```go
func TestCreateTaskAndOutboxRollBackTogether(t *testing.T) {
    store := newStore(t)
    err := store.WithTx(t.Context(), func(tx postgres.Tx) error {
        require.NoError(t, tx.CreateNotification(t.Context(), fixtureTask()))
        require.NoError(t, tx.InsertOutbox(t.Context(), fixtureEvent()))
        return errors.New("rollback")
    })
    require.Error(t, err)
    require.Equal(t, 0, countRows(t, store, "notification_tasks"))
    require.Equal(t, 0, countRows(t, store, "outbox_events"))
}

func TestDeliveryResultRejectsStaleLease(t *testing.T) {
    store := seededDeliveringTask(t)
    applied, err := store.ApplyDeliveryResult(t.Context(), id, "old-token", deliveredResult())
    require.NoError(t, err)
    require.False(t, applied)
}
```

- [ ] **Step 2: Run integration tests and verify schema-related failure**

Run: `go test -tags=integration ./internal/store/postgres -run 'Test(CreateTask|DeliveryResult)'`

Expected: FAIL because migrations and repository methods do not exist.

- [ ] **Step 3: Implement the migration with constraints and claim indexes**

The migration must define enums through checked text columns to keep rollback simple. Required constraints and indexes include:

```sql
UNIQUE (caller_id, idempotency_key)
UNIQUE (aggregate_id, generation)
CHECK (status IN ('pending','delivering','retry_wait','delivered','dead'))
CHECK (attempt_count >= 0)

CREATE INDEX notification_retry_due_idx
ON notification_tasks (next_attempt_at, id)
WHERE status = 'retry_wait';

CREATE INDEX notification_lease_expired_idx
ON notification_tasks (lease_until, id)
WHERE status = 'delivering';

CREATE INDEX outbox_unpublished_idx
ON outbox_events (created_at, id)
WHERE published_at IS NULL;
```

Use `JSONB` for payload/non-secret header snapshots, `BYTEA` for request hashes, `TIMESTAMPTZ` for time, and explicit maximum-length checks for caller IDs, idempotency keys, URLs, and error summaries.

- [ ] **Step 4: Implement short-transaction claims and token-guarded updates**

Publisher claim shape:

```sql
WITH candidates AS (
  SELECT id FROM outbox_events
  WHERE published_at IS NULL
    AND (claim_until IS NULL OR claim_until < $1)
  ORDER BY created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT $2
)
UPDATE outbox_events AS e
SET claim_token = $3, claim_until = $4, publish_attempts = publish_attempts + 1
FROM candidates
WHERE e.id = candidates.id
RETURNING e.id, e.aggregate_id, e.event_type, e.payload;
```

All completion methods return `(applied bool, err error)` so a stale owner is distinguishable from a database failure.

- [ ] **Step 5: Run all repository integration tests including concurrent claim races**

Run: `go test -tags=integration -race ./internal/store/postgres`

Expected: PASS with two concurrent claimers receiving disjoint rows and stale claim/lease updates returning `applied=false`.

- [ ] **Step 6: Commit the persistence boundary**

```bash
git add migrations internal/store internal/testsupport/postgres.go
git commit -m "feat: add transactional notification store"
```

---

### Task 3: Intake Service and Authenticated HTTP API

**Files:**
- Create: `internal/intake/service.go`
- Create: `internal/intake/service_test.go`
- Create: `internal/transport/httpapi/router.go`
- Create: `internal/transport/httpapi/auth.go`
- Create: `internal/transport/httpapi/json.go`
- Create: `internal/transport/httpapi/create.go`
- Create: `internal/transport/httpapi/create_test.go`
- Create: `internal/transport/httpapi/errors.go`

**Interfaces:**
- Consumes: destination registry, clock/ID generator, and transactional intake repository from Tasks 1-2.
- Produces: `intake.Service.Create(ctx, caller, idempotencyKey, destinationID string, payload json.RawMessage) (notification.Task, bool, error)`; the bool reports an idempotent replay.
- Produces: authenticated `POST /v1/notifications`.

- [ ] **Step 1: Write failing use-case and handler tests**

Tests must include same-key/same-content replay, canonical key order, conflicting content, missing/oversized idempotency key, unknown destination, malformed JSON, unknown fields, trailing JSON, body over 256 KiB, missing/wrong token, and database rollback.

```go
func TestCreateSameKeyDifferentBodyReturnsConflict(t *testing.T) {
    repo := newFakeIntakeRepo(existingTask(`{"order_id":"1"}`))
    _, _, err := newService(repo).Create(t.Context(), "orders", "key-1", "crm", json.RawMessage(`{"order_id":"2"}`))
    require.ErrorIs(t, err, intake.ErrIdempotencyConflict)
}

func TestCreateRejectsTrailingJSON(t *testing.T) {
    req := httptest.NewRequest(http.MethodPost, "/v1/notifications", strings.NewReader(`{"destination_id":"crm","payload":{}} {}`))
    rr := httptest.NewRecorder()
    newRouter(t).ServeHTTP(rr, authenticated(req))
    require.Equal(t, http.StatusBadRequest, rr.Code)
}
```

- [ ] **Step 2: Verify the focused tests fail**

Run: `go test ./internal/intake ./internal/transport/httpapi -run 'TestCreate'`

Expected: FAIL because the service and routes do not exist.

- [ ] **Step 3: Implement atomic intake and idempotency conflict handling**

The service resolves the destination, constructs its snapshot, hashes canonical request content, and calls one repository transaction that either inserts task plus generation-zero Outbox event or loads the unique-key conflict. Equal hashes return the existing task; unequal hashes return `ErrIdempotencyConflict`.

Do not recover uniqueness by matching error strings. Map PostgreSQL constraint names to typed repository errors.

- [ ] **Step 4: Implement strict HTTP parsing, authentication and error envelopes**

Use a bounded reader and this stable envelope:

```go
type errorResponse struct {
    Error struct {
        Code    string `json:"code"`
        Message string `json:"message"`
    } `json:"error"`
}
```

Bearer tokens map to configured caller identities and are compared with `subtle.ConstantTimeCompare`. The handler sets `Content-Type: application/json`, returns `202` for both initial acceptance and exact replay, and never exposes internal error text.

- [ ] **Step 5: Run API tests and race checks**

Run: `go test -race ./internal/intake ./internal/transport/httpapi`

Expected: PASS.

- [ ] **Step 6: Commit durable API intake**

```bash
git add internal/intake internal/transport/httpapi
git commit -m "feat: accept notifications with idempotency"
```

---

### Task 4: RabbitMQ Topology and Transactional Outbox Publisher

**Files:**
- Create: `internal/messaging/rabbitmq/topology.go`
- Create: `internal/messaging/rabbitmq/publisher.go`
- Create: `internal/messaging/rabbitmq/consumer.go`
- Create: `internal/messaging/rabbitmq/integration_test.go`
- Create: `internal/outbox/service.go`
- Create: `internal/outbox/service_test.go`
- Create: `internal/testsupport/rabbitmq.go`

**Interfaces:**
- Consumes: Outbox claim/confirm repository and clock.
- Produces: `outbox.Publisher.Publish(ctx context.Context, msg outbox.Message) error` where success means broker confirmation.
- Produces: `outbox.Service.RunOnce(ctx context.Context) (int, error)` for deterministic tests and `Run(ctx)` for polling.
- Produces: durable RabbitMQ exchange/queue topology and manual-ack consumer deliveries.

- [ ] **Step 1: Write publisher service tests with confirm-loss and stale-claim cases**

```go
func TestRunOnceDoesNotMarkPublishedWhenConfirmFails(t *testing.T) {
    repo := fakeOutboxRepoWith(oneEvent())
    pub := &fakePublisher{err: outbox.ErrConfirmLost}
    _, err := outbox.NewService(repo, pub, fixedClock()).RunOnce(t.Context())
    require.ErrorIs(t, err, outbox.ErrConfirmLost)
    require.Empty(t, repo.markedPublished)
}

func TestRunOnceTreatsStaleConfirmAsBenign(t *testing.T) {
    repo := fakeOutboxRepoWithStaleClaim(oneEvent())
    count, err := outbox.NewService(repo, successfulPublisher(), fixedClock()).RunOnce(t.Context())
    require.NoError(t, err)
    require.Equal(t, 0, count)
}
```

- [ ] **Step 2: Verify publisher tests fail**

Run: `go test ./internal/outbox`

Expected: FAIL because the publisher loop is absent.

- [ ] **Step 3: Implement topology and confirmation-aware publication**

Declare one durable direct exchange, one durable quorum queue, a stable routing key, persistent messages, JSON content type, message ID equal to `event_id`, and correlation ID equal to `notification_id`. Mandatory publication and returned-message handling are required. A publish is successful only after a positive confirmation; channel closure, return, timeout, or negative confirmation is failure.

- [ ] **Step 4: Implement bounded claim/publish/mark batches**

Each claimed event is independently published and marked using its claim token. Context cancellation stops new work. Failed events retain or expire their claims for retry; the loop applies bounded backoff on infrastructure errors and does not busy-spin when no rows exist.

- [ ] **Step 5: Run RabbitMQ integration tests**

Tests must prove durable topology creation is idempotent, a persistent message is confirmed, duplicate message IDs can be consumed safely, unacked messages are redelivered after consumer connection close, and malformed messages are rejected without infinite hot-looping.

Run: `go test -tags=integration -race ./internal/messaging/rabbitmq ./internal/outbox`

Expected: PASS.

- [ ] **Step 6: Commit the Outbox transport**

```bash
git add internal/messaging internal/outbox internal/testsupport/rabbitmq.go
git commit -m "feat: publish transactional outbox events"
```

---

### Task 5: Hardened HTTP Delivery Worker

**Files:**
- Create: `internal/delivery/http_sender.go`
- Create: `internal/delivery/http_sender_test.go`
- Create: `internal/delivery/service.go`
- Create: `internal/delivery/service_test.go`
- Modify: `internal/messaging/rabbitmq/consumer.go`
- Modify: `internal/destination/resolver.go`

**Interfaces:**
- Consumes: task claim/result repository, destination secret resolver, RabbitMQ delivery, clock/IDs, and `delivery.Sender`.
- Produces: `Sender.Send(ctx context.Context, snapshot notification.DeliverySnapshot) delivery.Result`.
- Produces: `Service.Handle(ctx context.Context, message messaging.Delivery) messaging.Disposition` returning ACK, requeue, or reject.

- [ ] **Step 1: Write failing sender tests for status, timeout, redirects and network policy**

```go
func TestSenderRejectsCrossHostRedirect(t *testing.T) {
    target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
    defer target.Close()
    source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
    }))
    defer source.Close()

    result := testSender(t).Send(t.Context(), snapshotFor(source.URL))
    require.Equal(t, notification.Permanent, result.Outcome)
    require.Equal(t, "redirect_not_allowed", result.ErrorCode)
}

func TestSenderLimitsResponseBody(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(http.StatusBadGateway)
        _, _ = w.Write(bytes.Repeat([]byte("x"), 64<<10))
    }))
    defer server.Close()

    sender := testSenderWithDiagnosticLimit(t, 1024)
    result := sender.Send(t.Context(), snapshotFor(server.URL))
    require.Equal(t, notification.Retryable, result.Outcome)
    require.LessOrEqual(t, len(result.ErrorMessage), 1024)
    require.Equal(t, "http_5xx", result.ErrorCode)
}
```

Also test `2xx`, `408`, `425`, `429`, other `4xx`, `5xx`, timeout, connection reset, malformed `Retry-After`, same-host redirect policy, resolved private IP rejection, header secret resolution, and idempotency header forwarding.

- [ ] **Step 2: Write worker crash-window and lease ownership tests**

Pin duplicate messages, terminal tasks, active lease conflicts, stale result writes, database failure before HTTP, database failure after HTTP, ACK failure, and malformed broker payload.

```go
func TestHandleDoesNotSendWhenClaimNotAcquired(t *testing.T) {
    repo := fakeDeliveryRepo{claim: notification.Claim{Acquired: false}}
    sender := &spySender{}
    disposition := newWorker(repo, sender).Handle(t.Context(), validDelivery())
    require.Equal(t, messaging.Ack, disposition)
    require.Zero(t, sender.calls)
}
```

- [ ] **Step 3: Verify delivery tests fail**

Run: `go test ./internal/delivery ./internal/messaging/rabbitmq`

Expected: FAIL because the sender and worker service are absent.

- [ ] **Step 4: Implement the hardened transport outside database transactions**

Build a per-request timeout context. Use a custom `http.Transport` with TLS verification, bounded connection pools, response header timeout, disabled compression if diagnostic size cannot be bounded, and a `DialContext` that resolves and validates every IP before dialing. Revalidate redirects through `CheckRedirect` and strip sensitive headers before any allowed redirect.

Read at most the diagnostic response limit, sanitize control characters, and return stable error codes rather than raw network text.

- [ ] **Step 5: Implement lease-driven worker disposition**

The handler validates the compact broker message, claims the task in a short transaction, performs HTTP outside the transaction, and applies the outcome with the lease token. ACK terminal/duplicate messages. Requeue only when authoritative database state could not be read or committed. Reject malformed poison messages without requeue and emit a metric/log.

- [ ] **Step 6: Run delivery tests with the race detector**

Run: `go test -race ./internal/delivery ./internal/messaging/rabbitmq`

Expected: PASS, including concurrent handling of duplicate messages.

- [ ] **Step 7: Commit reliable delivery**

```bash
git add internal/delivery internal/messaging/rabbitmq/consumer.go internal/destination/resolver.go
git commit -m "feat: deliver notifications with leases"
```

---

### Task 6: Retry Scheduling and Expired Lease Recovery

**Files:**
- Create: `internal/retry/service.go`
- Create: `internal/retry/service_test.go`
- Modify: `internal/store/postgres/retry.go`
- Modify: `internal/notification/retry.go`

**Interfaces:**
- Consumes: scheduler repository and deterministic clock.
- Produces: `retry.Service.RunOnce(ctx context.Context) (retry.Result, error)` and `Run(ctx)`.
- Produces: atomic due-task/expired-lease transitions plus one unique Outbox generation per transition.

- [ ] **Step 1: Write failing scheduler tests**

Cover due versus future tasks, max-attempt exhaustion, lifetime exhaustion, valid `Retry-After`, jitter boundaries, two concurrent scheduler instances, and expired lease recovery.

```go
func TestExpiredLeaseCreatesOneRetryGeneration(t *testing.T) {
    repo := fakeSchedulerRepoWithExpiredLease()
    svc := retry.NewService(repo, fixedClock())
    _, err := svc.RunOnce(t.Context())
    require.NoError(t, err)
    _, err = svc.RunOnce(t.Context())
    require.NoError(t, err)
    require.Len(t, repo.outboxEvents, 1)
    require.Equal(t, "delivery_lease_expired", repo.task.LastErrorCode)
}
```

- [ ] **Step 2: Verify scheduler tests fail**

Run: `go test ./internal/retry ./internal/notification -run 'Test(Expired|Next|Retry)'`

Expected: FAIL because the scheduler service is absent or behavior is incomplete.

- [ ] **Step 3: Implement atomic scheduling batches**

One repository transaction claims a bounded set, changes each task to `pending` or `dead`, increments its dispatch generation only when scheduling, and inserts the generation-matched Outbox event. A uniqueness conflict means another scheduler already created the event and is treated as a benign race.

The scheduler records lease expiry separately from supplier failures. It stops claiming promptly on context cancellation and sleeps through an injectable ticker only in `Run`, never in `RunOnce`.

- [ ] **Step 4: Run unit and PostgreSQL concurrency tests**

Run: `go test -race ./internal/retry ./internal/notification && go test -tags=integration -race ./internal/store/postgres -run 'Test.*(Retry|Lease|Scheduler)'`

Expected: PASS.

- [ ] **Step 5: Commit bounded retry scheduling**

```bash
git add internal/retry internal/notification/retry.go internal/store/postgres/retry.go
git commit -m "feat: schedule retries and recover leases"
```

---

### Task 7: Status Query, Ownership, and Audited Dead-task Replay

**Files:**
- Create: `internal/notification/query.go`
- Create: `internal/notification/query_test.go`
- Create: `internal/transport/httpapi/query.go`
- Create: `internal/transport/httpapi/query_test.go`
- Create: `internal/transport/httpapi/replay.go`
- Create: `internal/transport/httpapi/replay_test.go`
- Modify: `internal/transport/httpapi/router.go`
- Modify: `internal/transport/httpapi/auth.go`
- Modify: `internal/store/postgres/query.go`

**Interfaces:**
- Consumes: query/replay repository and caller/admin authentication.
- Produces: `GET /v1/notifications/{notification_id}` and `POST /v1/admin/notifications/{notification_id}/replay`.
- Produces: `QueryService.Get(ctx, callerID, notificationID string) (notification.View, error)` and `ReplayService.Replay(ctx, adminID, notificationID string) error`.

- [ ] **Step 1: Write failing endpoint tests**

Test owner access, cross-caller indistinguishable `404`, malformed ID, missing task, redacted view, admin token separation, non-dead replay conflict, repeated replay conflict, and transaction rollback.

```go
func TestQueryDoesNotReturnPayloadOrSecrets(t *testing.T) {
    rr := performOwnedQuery(t)
    require.Equal(t, http.StatusOK, rr.Code)
    require.NotContains(t, rr.Body.String(), "api-key")
    require.NotContains(t, rr.Body.String(), "order_id")
}

func TestReplayNonDeadTaskReturnsConflict(t *testing.T) {
    rr := performAdminReplay(t, notification.StatusRetryWait)
    require.Equal(t, http.StatusConflict, rr.Code)
}
```

- [ ] **Step 2: Verify query/replay tests fail**

Run: `go test ./internal/notification ./internal/transport/httpapi -run 'Test(Query|Replay)'`

Expected: FAIL because routes and services do not exist.

- [ ] **Step 3: Implement caller-owned query view**

Return only ID, destination ID, status, attempt count, next attempt, last HTTP status, stable error code, sanitized bounded error message, and lifecycle timestamps. Scope SQL by both notification ID and caller ID.

- [ ] **Step 4: Implement atomic dead-task replay and audit log**

Replay changes `dead` to `pending`, clears lease/dead scheduling fields, increments dispatch generation, and inserts the Outbox event in one transaction. Preserve historical `attempt_count`. Emit an audit log with admin identity and notification ID, never payload or secret values.

- [ ] **Step 5: Run handler and repository tests**

Run: `go test -race ./internal/notification ./internal/transport/httpapi && go test -tags=integration ./internal/store/postgres -run 'Test.*Replay'`

Expected: PASS.

- [ ] **Step 6: Commit operational status APIs**

```bash
git add internal/notification/query* internal/transport/httpapi internal/store/postgres/query.go
git commit -m "feat: query and replay notification tasks"
```

---

### Task 8: Application Wiring, Health, Metrics, Shutdown, and Retention

**Files:**
- Create: `internal/app/app.go`
- Create: `internal/app/app_test.go`
- Modify: `cmd/notifier/main.go`
- Create: `internal/observability/logging.go`
- Create: `internal/observability/metrics.go`
- Create: `internal/observability/health.go`
- Create: `internal/observability/health_test.go`
- Create: `internal/retention/service.go`
- Create: `internal/retention/service_test.go`
- Create: `internal/store/postgres/retention.go`

**Interfaces:**
- Consumes: all role services and dependency health checks.
- Produces: `app.Run(ctx context.Context, cfg config.Config) error`.
- Produces: `/livez`, role-aware `/readyz`, `/metrics`, structured redacted logs, and bounded retention cleanup.

- [ ] **Step 1: Write failing role readiness and shutdown tests**

```go
func TestWorkerReadinessRequiresPostgresRabbitAndDestinations(t *testing.T) {
    checks := fakeChecks{postgres: nil, rabbit: errors.New("down"), destinations: nil}
    rr := requestReady(t, "worker", checks)
    require.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestShutdownStopsClaimsBeforeWaitingForInflight(t *testing.T) {
    roles := newBlockingRoleSet()
    cancelAndRun(t, roles)
    require.Less(t, roles.stopClaimsAt, roles.inflightDoneAt)
}
```

Test secret/body redaction with adversarial strings, high-cardinality label exclusion, readiness per role, graceful timeout, and cleanup skipping active/unpublished records.

- [ ] **Step 2: Verify orchestration tests fail**

Run: `go test ./internal/app ./internal/observability ./internal/retention`

Expected: FAIL because application wiring and operational endpoints are absent.

- [ ] **Step 3: Implement role-specific dependency construction**

`api` starts intake/query/replay HTTP routes; `publisher` starts Outbox publication; `worker` starts RabbitMQ consumers; `scheduler` starts retry plus retention loops; `all` starts every role. Build only dependencies needed by the selected role and close them in reverse order.

Use `signal.NotifyContext` for SIGINT/SIGTERM. On cancellation, stop accepting HTTP and broker deliveries, stop pollers, wait for in-flight operations up to the configured shutdown timeout, then close broker and database connections. Any interrupted claimed work remains recoverable by leases.

- [ ] **Step 4: Implement metrics, redacted logging and bounded retention**

Metrics use fixed outcome/status labels and bounded destination labels. Logs pass through a redacting handler for configured sensitive keys. Retention deletes terminal tasks older than 30 days only when no unpublished Outbox rows reference them, in small repeatable batches.

- [ ] **Step 5: Run operational and full unit checks**

Run: `go test -race ./... && go vet ./... && make fmt`

Expected: PASS and no formatting output.

- [ ] **Step 6: Commit production runtime wiring**

```bash
git add cmd internal/app internal/observability internal/retention internal/store/postgres/retention.go
git commit -m "feat: wire service roles and operations"
```

---

### Task 9: Containers and End-to-end Reliability Tests

**Files:**
- Create: `deployments/Dockerfile`
- Create: `deployments/compose.yaml`
- Create: `internal/integration/e2e_test.go`
- Create: `internal/integration/failure_recovery_test.go`
- Create: `internal/testsupport/supplier.go`
- Modify: `configs/destinations.example.yaml`

**Interfaces:**
- Consumes: completed application binary and public HTTP API.
- Produces: reproducible local stack and black-box proof of durable delivery/recovery.

- [ ] **Step 1: Add failing end-to-end tests**

Tests must cover:

1. API `202` through Outbox, RabbitMQ and worker to a controlled supplier.
2. Supplier `503`, scheduler retry, then supplier `204`.
3. Duplicate broker delivery resulting in one terminal transition.
4. Publisher interruption before mark-published followed by successful recovery.
5. Worker interruption after claim followed by lease-expiry recovery.
6. Graceful restart with no permanently stuck task.

Use polling assertions with explicit deadlines and state diagnostics; do not use fixed multi-second sleeps.

- [ ] **Step 2: Verify tests fail before deployment artifacts exist**

Run: `go test -tags=integration ./internal/integration -run TestEndToEnd`

Expected: FAIL because the stack and harness are incomplete.

- [ ] **Step 3: Implement the multi-stage non-root image and Compose stack**

The image builds with the pinned Go 1.27 patch and copies only the binary, CA certificates, and migrations into a minimal non-root runtime. Compose defines PostgreSQL and RabbitMQ health checks, a one-shot migration service, separate API/publisher/worker/scheduler services, restart policies, and internal networking. The example supplier is test-only.

Do not embed credentials in the image. Compose uses development-only values documented as unsuitable for production.

- [ ] **Step 4: Run end-to-end recovery tests**

Run: `docker compose -f deployments/compose.yaml up -d --build && go test -tags=integration -race ./internal/integration && docker compose -f deployments/compose.yaml down -v`

Expected: PASS; final cleanup exits successfully even if the test command fails, implemented by the test script/CI trap rather than manual intervention.

- [ ] **Step 5: Scan the built image and configuration for accidental secrets**

Run: `git grep -nE '(api[_-]?key|token|password)[[:space:]]*[:=][[:space:]]*[^$<{]' -- ':!docs/superpowers/**' ':!**/*_test.go'`

Expected: no real secret values; documented local development placeholders are clearly named and scoped.

- [ ] **Step 6: Commit reproducible deployment and E2E coverage**

```bash
git add deployments configs internal/integration internal/testsupport/supplier.go
git commit -m "test: add end-to-end reliability environment"
```

---

### Task 10: CI, README, and AI-use Disclosure

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `README.md`
- Create: `docs/ai-usage.md`
- Modify: `Makefile`
- Modify: `docs/superpowers/specs/2026-09-23-notification-service-design.md` only if implementation verification reveals a documented deviation.

**Interfaces:**
- Consumes: all test and runtime commands established in Tasks 1-9.
- Produces: evaluator-facing documentation and repeatable GitHub checks.

- [ ] **Step 1: Write CI workflow and validate its referenced commands locally**

CI jobs must:

- verify `gofmt` cleanliness;
- run `go vet ./...` and the pinned static analyzer;
- run unit tests and `go test -race ./...`;
- start PostgreSQL and RabbitMQ for integration tests;
- run migrations and all integration/end-to-end tests;
- build the binary and container image;
- cache only Go modules/build data, never generated test state.

Run every Make target referenced by CI locally before committing. Pin third-party GitHub Actions by full commit SHA and note the upstream release in a comment.

- [ ] **Step 2: Write an evaluator-focused README**

README sections:

1. Problem understanding and system boundary.
2. Architecture diagram and component responsibilities.
3. At-least-once semantics and duplicate window.
4. Transactional Outbox consistency argument.
5. Quick start and configuration.
6. API examples and status codes.
7. Failure/retry policy.
8. Security controls and known limits.
9. Test commands and repository layout.
10. Key trade-offs and evolution path.

Commands must be copy-pasteable from a clean checkout. Link the architecture spec and AI-use disclosure.

- [ ] **Step 3: Write the required AI-use disclosure truthfully**

Record:

- AI helped extract requirements, compare architectures, identify failure windows, draft tests, and review implementation details.
- The initial AI recommendation was a PostgreSQL-only queue.
- The user rejected that recommendation and selected PostgreSQL + RabbitMQ to demonstrate the common DB/MQ consistency problem.
- The project rejected Kafka, two-phase commit, microservices, Kubernetes, dynamic templates, online administration, and generic exactly-once claims.
- Human decisions include the architecture selection, scope approvals, and final acceptance of trade-offs.
- The disclosure must be updated if later implementation decisions differ from this record.

- [ ] **Step 4: Run the complete verification matrix**

Run:

```bash
make fmt
make vet
make lint
make test
make test-race
make integration
docker build -f deployments/Dockerfile .
git diff --check
```

Expected: every command exits zero. Record any platform-specific exclusions explicitly in README and CI rather than silently skipping them.

- [ ] **Step 5: Review the implementation against all acceptance criteria**

For each of the ten acceptance criteria in the architecture spec, link it to a passing automated test or a documented operational verification. Check that no unimplemented feature is described as complete and that any implementation deviation is reflected in the spec before the final commit.

- [ ] **Step 6: Commit final documentation and CI**

```bash
git add .github README.md docs Makefile
git commit -m "docs: complete delivery and AI usage guide"
```

- [ ] **Step 7: Push the verified branch**

Run: `git status --short --branch && git log --oneline --decorate -12`

Expected: clean `main` with the architecture and each implementation phase represented by a clear commit.

Run: `git push origin main`

Expected: local `main` and `origin/main` resolve to the same commit.

---

## Execution Order and Review Gates

Tasks are sequential because later services consume interfaces fixed by earlier tasks. After each task:

1. Run the task's focused verification.
2. Inspect `git diff --check` and the scoped diff.
3. Commit only that task's files.
4. Confirm the worktree is clean.
5. Continue only after the deliverable remains compatible with the architecture spec.

Before pushing the final branch, perform one whole-branch review focused on consistency semantics, stale-owner protection, secret handling, network boundaries, shutdown behavior, and reproducibility.
