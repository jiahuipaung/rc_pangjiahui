# Acceptance Remediation Design

## 1. Purpose

This change closes the gaps found during acceptance review without expanding the notification service beyond its MVP boundary. It makes the GitHub workflow green on Go 1.27, enforces the configured destination concurrency limit inside each worker process, enables bounded production retry jitter, wires the metrics that the runtime actually exposes, and adds integration evidence for the most important failure-recovery paths.

The repair preserves the existing PostgreSQL-authoritative, RabbitMQ-at-least-once architecture. It does not introduce distributed rate limiting, a metrics aggregation subsystem, process orchestration tests that depend on timing-sensitive shell signals, or exactly-once delivery claims.

## 2. CI Toolchain Compatibility

The current workflow installs `golangci-lint` v2.4.0 while compiling with Go 1.27.1. That linter cannot decode the export-data version emitted by Go 1.27, so the quality job fails before unit, race, build, and image checks run.

The workflow will pin a `golangci-lint` release that explicitly supports Go 1.27. It will print `go version` and `golangci-lint version` before linting so future failures expose the effective toolchain. The linter remains version-pinned rather than floating at latest. All existing quality commands stay in the workflow.

## 3. Destination Concurrency

### 3.1 Semantics

`concurrency_limit` means the maximum number of simultaneous supplier HTTP calls for one destination inside one worker process. If three worker replicas each use a limit of ten, the deployment-wide maximum is thirty. The README and architecture document will state this boundary explicitly.

A global cross-process quota is out of scope because it would require a distributed semaphore, destination partitions, or broker-level routing. That complexity is not justified by the assignment load.

### 3.2 Snapshot and persistence

The destination's concurrency limit becomes part of `DeliverySnapshot` and the `notification_tasks` schema. Intake copies it into the immutable task snapshot, PostgreSQL stores and reloads it, and replay/retry generations retain it automatically.

Existing migrations may be edited because the repository has not published a stable production schema. The column is non-null and checked to be positive.

### 3.3 Enforcement

A worker-owned limiter maps `destination_id` to a buffered token channel. Delivery acquires a token before supplier network I/O and releases it with `defer`. Waiting for capacity observes the delivery context; cancellation does not leak a token or perform the HTTP call.

The limiter is injected behind a small interface so delivery tests can prove ordering and cancellation without sleeping. The default implementation lazily creates semaphores and rejects inconsistent limits for the same destination as a configuration/programming error.

## 4. Retry Jitter

Production retry delays use bounded symmetric jitter of plus or minus 20 percent around the configured delay. The result is clamped to a positive duration and remains bounded by the task lifetime. `Retry-After` supplied by the remote service is treated as an explicit server instruction and is not jittered.

The delivery service accepts a `notification.Jitter` dependency. Production wiring supplies a concurrency-safe cryptographic random implementation. Tests inject deterministic jitter. This removes the current hard-coded `noJitter` path and keeps time-based tests reproducible.

## 5. Runtime Metrics

The MVP will expose and update only metrics backed by real events:

- accepted API requests by outcome;
- supplier delivery attempts by outcome;
- notifications moved to `dead`;
- retry generations scheduled;
- expired delivery leases recovered;
- Outbox publish confirmations and failures.

Application services receive narrow observer interfaces with no-op defaults, keeping tests and domain logic independent of Prometheus. The runtime constructs one metrics registry per process and injects observers into roles present in that process.

The architecture document will remove claims for API latency, tasks-by-state gauges, Outbox age/count gauges, and RabbitMQ redelivery counters until those measurements are implemented. High-cardinality task and caller identifiers remain forbidden as labels.

## 6. Failure-recovery Tests

Real PostgreSQL and RabbitMQ integration tests will add these scenarios:

1. A supplier returns `503`; the task enters `retry_wait`; the scheduler emits a new Outbox generation; a second broker delivery reaches the supplier and returns `204`; the task becomes `delivered` with two attempts.
2. The same RabbitMQ message is handled twice; only one worker claim reaches the supplier and the terminal task remains valid.
3. A task with an expired worker lease is recovered by the scheduler exactly once and can subsequently complete.

The existing success-path E2E remains. Tests use polling with explicit deadlines or direct `RunOnce` calls, never fixed multi-second sleeps. Test suppliers may use loopback HTTP through the existing development override; HTTPS enforcement and certificate verification remain covered at the sender/configuration layer.

Compose smoke verification will build and start all roles, require a successful migration, and require `/readyz` to return ready. Destructive volume cleanup remains an explicit operator command rather than part of ordinary tests.

## 7. Documentation Alignment

The README and architecture specification will be edited only to describe implemented behavior:

- per-process destination concurrency semantics;
- plus/minus 20 percent production jitter;
- the actual metrics set;
- component integration tests versus full black-box E2E coverage;
- CI toolchain versions and commands.

The AI-use disclosure remains unchanged because these repairs do not alter the recorded human architecture decision.

## 8. Safety and Compatibility

- API request and response formats remain unchanged.
- Existing status transitions and at-least-once semantics remain unchanged.
- No secrets or payloads become metric labels or log attributes.
- Waiting for a concurrency token is cancelable.
- Random generation failures fail closed at process startup rather than silently disabling jitter.
- The migration remains compatible with clean deployments, which is the supported state of this assignment repository.

## 9. Acceptance Criteria

The remediation is complete when:

1. The pinned linter runs successfully with Go 1.27.1 in GitHub Actions and every workflow job is green.
2. A test proves no more than the configured number of supplier calls for one destination execute concurrently in one worker process.
3. A test proves different destinations do not block one another.
4. A test proves cancellation while waiting for capacity does not call the supplier or leak capacity.
5. Production wiring uses bounded non-zero jitter, while deterministic tests remain possible.
6. Metrics increment from actual intake, publishing, delivery, and scheduling events and contain no high-cardinality labels.
7. The three failure-recovery integration scenarios pass against real PostgreSQL and RabbitMQ.
8. Unit, race, vet, lint, integration, image build, Compose readiness, and `git diff --check` all pass.
9. README and architecture statements match the implemented boundaries.
