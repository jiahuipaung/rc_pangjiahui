CREATE TABLE notification_tasks (
    id                  TEXT PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 64),
    caller_id           TEXT NOT NULL CHECK (length(caller_id) BETWEEN 1 AND 128),
    idempotency_key     TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
    request_hash        BYTEA NOT NULL CHECK (octet_length(request_hash) = 32),
    destination_id      TEXT NOT NULL CHECK (length(destination_id) BETWEEN 1 AND 128),
    method              TEXT NOT NULL CHECK (method IN ('POST', 'PUT', 'PATCH')),
    url                 TEXT NOT NULL CHECK (length(url) BETWEEN 1 AND 2048),
    static_headers      JSONB NOT NULL DEFAULT '{}',
    secret_headers      JSONB NOT NULL DEFAULT '{}',
    body                JSONB NOT NULL,
    timeout_ns          BIGINT NOT NULL CHECK (timeout_ns > 0),
    max_attempts        INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 32),
    lifetime_ns         BIGINT NOT NULL CHECK (lifetime_ns > 0),
    retry_delays_ns     JSONB NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('pending','delivering','retry_wait','delivered','dead')),
    attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    generation          INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    next_attempt_at     TIMESTAMPTZ,
    lease_token         TEXT,
    lease_until         TIMESTAMPTZ,
    last_http_status    INTEGER,
    last_error_code     TEXT CHECK (last_error_code IS NULL OR length(last_error_code) <= 128),
    last_error_message  TEXT CHECK (last_error_message IS NULL OR length(last_error_message) <= 1024),
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    delivered_at        TIMESTAMPTZ,
    dead_at             TIMESTAMPTZ,
    UNIQUE (caller_id, idempotency_key)
);

CREATE TABLE outbox_events (
    id                  TEXT PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 64),
    aggregate_id        TEXT NOT NULL REFERENCES notification_tasks(id) ON DELETE CASCADE,
    generation          INTEGER NOT NULL CHECK (generation >= 0),
    event_type          TEXT NOT NULL CHECK (length(event_type) BETWEEN 1 AND 128),
    payload             JSONB NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    published_at        TIMESTAMPTZ,
    claim_token         TEXT,
    claim_until         TIMESTAMPTZ,
    publish_attempts    INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_error          TEXT CHECK (last_error IS NULL OR length(last_error) <= 1024),
    UNIQUE (aggregate_id, generation)
);

CREATE INDEX notification_retry_due_idx
ON notification_tasks (next_attempt_at, id)
WHERE status = 'retry_wait';

CREATE INDEX notification_lease_expired_idx
ON notification_tasks (lease_until, id)
WHERE status = 'delivering';

CREATE INDEX outbox_unpublished_idx
ON outbox_events (created_at, id)
WHERE published_at IS NULL;
