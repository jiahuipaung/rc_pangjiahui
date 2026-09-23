# Acceptance Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Every behavior change follows RED-GREEN-REFACTOR and each task ends in an independently reviewable commit.

**Goal:** Make the submitted notification service pass its remote quality pipeline and align its production behavior, failure-recovery evidence, metrics, and documentation with the approved acceptance-remediation design.

**Architecture:** Preserve PostgreSQL as authoritative state and RabbitMQ as at-least-once transport. Extend the immutable delivery snapshot with a per-worker-process destination concurrency limit, inject bounded production jitter, and add narrow observer interfaces around existing services. Use real PostgreSQL/RabbitMQ integration tests for recovery behavior and keep deployment-wide distributed quotas out of scope.

**Tech Stack:** Go 1.27.1 delivery baseline, Go standard library, pgx/v5, amqp091-go, Prometheus client, PostgreSQL 17, RabbitMQ 4.1, Docker Compose, GitHub Actions, golangci-lint v2.13.2.

**Spec:** `docs/superpowers/specs/2026-09-23-acceptance-remediation-design.md`

## Global constraints

- Keep external HTTP API formats and at-least-once semantics unchanged.
- `concurrency_limit` is per destination per worker process, never described as a global quota.
- Waiting for capacity must honor cancellation and must not hold a database transaction.
- Retry jitter is deterministic in tests and bounded to plus or minus 20 percent in production.
- Do not add task, caller, idempotency key, URL, payload, or secret values as metric labels.
- Integration tests use deadlines or direct single-iteration service calls, not fixed multi-second sleeps.
- Do not weaken HTTPS, SSRF, authentication, or secret-redaction controls.

## File map

```text
.github/workflows/ci.yml                 Go/linter compatibility and diagnostics
internal/notification/model.go           immutable concurrency snapshot field
internal/notification/jitter.go          bounded production jitter
internal/notification/jitter_test.go     jitter bounds and constructor behavior
internal/intake/service.go                snapshot population
internal/delivery/limiter.go              per-destination process-local semaphore
internal/delivery/limiter_test.go         capacity, isolation and cancellation
internal/delivery/service.go              limiter and jitter injection
internal/delivery/service_test.go         delivery behavior with limiter/jitter
internal/store/postgres/*.go              persist and reload concurrency limit
migrations/000001_initial.up.sql          checked non-null concurrency column
internal/observability/metrics.go          implemented counters and observer adapters
internal/observability/metrics_test.go     actual event increments and label safety
internal/config/config.go                  optional role metrics listen address
internal/intake/service.go                 acceptance observer
internal/outbox/service.go                publish observer
internal/retry/service.go                 scheduling observer
internal/app/bootstrap.go                 production dependency wiring
deployments/compose.yaml                   per-role internal metrics endpoints
internal/integration/e2e_test.go           shared E2E harness and success case
internal/integration/recovery_test.go      retry, duplicate and expired-lease cases
README.md                                  exact runtime semantics and verification
docs/superpowers/specs/2026-09-23-notification-service-design.md
                                             remove overclaims and add verified behavior
```

---

### Task 1: Repair the Go 1.27 quality pipeline

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] Reproduce the recorded failure with the existing workflow version: `golangci-lint v2.4.0` cannot decode Go 1.27 export data.
- [ ] Update the pinned install to `github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2`; add `go version` and `golangci-lint version` diagnostic steps.
- [ ] Build the linter with the Go 1.27.1 container and run it against the repository so local host Go 1.24 cannot mask compatibility.
- [ ] Run `docker run --rm -v "$PWD:/src" -w /src golang:1.27.1-alpine sh -c '<install build tools and run pinned lint>'` and verify zero findings.
- [ ] Commit: `ci: support Go 1.27 linting`.

### Task 2: Persist and enforce per-process destination concurrency

**Files:**
- Modify: `internal/notification/model.go`
- Modify: `internal/intake/service.go`
- Modify: `migrations/000001_initial.up.sql`
- Modify: `internal/store/postgres/intake.go`
- Modify: `internal/store/postgres/delivery.go`
- Modify: `internal/store/postgres/store_integration_test.go`
- Create: `internal/delivery/limiter.go`
- Create: `internal/delivery/limiter_test.go`
- Modify: `internal/delivery/service.go`
- Modify: `internal/delivery/service_test.go`
- Modify: `internal/app/bootstrap.go`

- [ ] Add failing intake and PostgreSQL integration assertions proving `ConcurrencyLimit` survives destination-to-task creation and database reload.
- [ ] Run focused tests and verify the field is missing from `DeliverySnapshot`/schema.
- [ ] Add `ConcurrencyLimit int` to `DeliverySnapshot`, populate it during intake, and persist it in a positive checked `concurrency_limit` column.
- [ ] Add failing limiter tests proving: a destination never exceeds its capacity; different destinations progress independently; cancellation while waiting returns without acquiring or leaking capacity; mixed old/new snapshot limits conservatively use the smallest observed limit.
- [ ] Implement a mutex-protected map of destination capacity state with `Acquire(ctx, destinationID, limit) (release func(), err error)` and cancellation-aware waiting.
- [ ] Add failing delivery-service tests proving the supplier is called only after capacity acquisition and cancellation returns `Requeue` without a supplier call.
- [ ] Inject the limiter into `delivery.Service`; hold the token only around supplier I/O and result calculation, release it before the result database update.
- [ ] Wire one limiter per worker process in `internal/app/bootstrap.go`.
- [ ] Run `go test -race ./internal/intake ./internal/delivery` and PostgreSQL integration tests.
- [ ] Commit: `feat: enforce destination concurrency limits`.

### Task 3: Enable bounded production retry jitter

**Files:**
- Create: `internal/notification/jitter.go`
- Create: `internal/notification/jitter_test.go`
- Modify: `internal/notification/retry.go`
- Modify: `internal/notification/retry_test.go`
- Modify: `internal/delivery/service.go`
- Modify: `internal/delivery/service_test.go`
- Modify: `internal/app/bootstrap.go`

- [ ] Add failing tests for a seeded bounded jitter implementation: outputs remain in `[80%, 120%]`, positive delays remain positive, and repeated samples are not constant.
- [ ] Implement a concurrency-safe PRNG seeded from `crypto/rand`; constructor returns an error if secure seeding fails.
- [ ] Add a failing delivery test showing the injected jitter determines `NextAttemptAt`; verify `Retry-After` remains unchanged.
- [ ] Replace `noJitter` with an injected `notification.Jitter`; allow a deterministic identity jitter only as an explicit test dependency.
- [ ] Construct production jitter during worker startup and fail startup if seeding fails.
- [ ] Run `go test -race ./internal/notification ./internal/delivery ./internal/app`.
- [ ] Commit: `feat: add bounded retry jitter`.

### Task 4: Wire truthful runtime metrics

**Files:**
- Modify: `internal/observability/metrics.go`
- Modify: `internal/observability/metrics_test.go`
- Modify: `internal/intake/service.go`
- Modify: `internal/intake/service_test.go`
- Modify: `internal/outbox/service.go`
- Modify: `internal/outbox/service_test.go`
- Modify: `internal/delivery/service.go`
- Modify: `internal/delivery/service_test.go`
- Modify: `internal/retry/service.go`
- Modify: `internal/retry/service_test.go`
- Modify: `internal/app/bootstrap.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `deployments/compose.yaml`

- [ ] Define narrow observer interfaces for intake, Outbox, delivery, and scheduler with package-local no-op defaults.
- [ ] Add failing service tests that assert one observer event for accepted/rejected intake, publish confirm/failure, delivery outcome/dead transition, scheduled retries, and recovered leases.
- [ ] Extend `observability.Metrics` with bounded-label counters for these implemented events and adapter methods satisfying the observer interfaces.
- [ ] Add an optional `METRICS_ADDR` for non-API roles and a small observability HTTP server exposing `/metrics`, `/livez`, and `/readyz`; Compose uses `:9090` inside each isolated role container. API and `all` keep metrics on their existing HTTP server.
- [ ] Inject one metrics registry in runtime wiring for each process and expose every split-role metric set from that process's observability endpoint.
- [ ] Verify `/metrics` changes after exercising services and does not contain high-cardinality identifiers.
- [ ] Run `go test -race ./internal/intake ./internal/outbox ./internal/delivery ./internal/retry ./internal/observability ./internal/app`.
- [ ] Commit: `feat: record notification runtime metrics`.

### Task 5: Add failure-recovery integration coverage

**Files:**
- Modify: `internal/integration/e2e_test.go`
- Create: `internal/integration/recovery_test.go`
- Optionally create: `internal/integration/harness_test.go` if shared setup would otherwise be duplicated

- [ ] Extract only the shared database, broker, API request, task query, and supplier helpers needed by multiple tests.
- [ ] Add a failing `503 -> retry_wait -> scheduler -> publish -> 204 -> delivered` test that asserts two attempts and generation advancement.
- [ ] Implement or correct only the production behavior exposed by that test; do not add test-only production branches.
- [ ] Add a failing duplicate-message test proving two deliveries produce one supplier call and a valid terminal task.
- [ ] Add a failing expired-lease test proving the scheduler emits one recovery generation and the task subsequently completes.
- [ ] Run `TEST_DATABASE_URL=... TEST_RABBITMQ_URL=... go test -tags=integration -race -count=1 -timeout=90s ./internal/integration ./internal/store/postgres ./internal/messaging/rabbitmq`.
- [ ] Commit: `test: cover delivery failure recovery`.

### Task 6: Align documentation and complete final verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-09-23-notification-service-design.md`
- Modify: `docs/superpowers/specs/2026-09-23-acceptance-remediation-design.md` only if implementation reveals an approved deviation

- [ ] Update concurrency wording to the per-destination, per-worker-process boundary.
- [ ] Document plus/minus 20 percent jitter and exact `Retry-After` behavior.
- [ ] Replace the broad metrics inventory with only counters wired by Task 4; explain split-role metrics exposure.
- [ ] Correct the testing strategy to distinguish component integration tests from black-box E2E tests.
- [ ] Run the complete matrix: formatting, vet, golangci-lint v2.13.2, unit race, all real integration tests, binary build, image build, Compose startup/readiness, secret scan, and `git diff --check`.
- [ ] Review all nine remediation acceptance criteria and record evidence in the final report.
- [ ] Commit: `docs: align verified notification behavior`.
- [ ] Request final code review, address all critical/important findings, merge to `main`, push `origin/main`, and verify the resulting GitHub Actions run is green before declaring completion.

## Verification commands

```bash
make fmt
make vet
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 run
make test-race
TEST_DATABASE_URL='postgres://notifier:notifier@127.0.0.1:55432/notifier?sslmode=disable' \
TEST_RABBITMQ_URL='amqp://notifier:notifier@127.0.0.1:55672/' \
  go test -tags=integration -race -count=1 -timeout=90s ./...
go build ./cmd/notifier
docker build -f deployments/Dockerfile -t rc-notifier:acceptance .
docker compose -f deployments/compose.yaml up -d --build
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
git diff --check main...HEAD
```

## Execution handoff

This plan is intended for Native execution in the current session because the tasks share schema, service-constructor, and integration-harness changes. Execute serially in the isolated `codex/acceptance-remediation` worktree with a clean commit after each task.
