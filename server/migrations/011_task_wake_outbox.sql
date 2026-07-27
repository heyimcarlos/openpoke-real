CREATE TABLE task_wake_outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_version SMALLINT NOT NULL DEFAULT 1
        CHECK (event_version = 1),
    task_id UUID NOT NULL REFERENCES execution_tasks(task_id),
    executor_kind TEXT NOT NULL CHECK (
        executor_kind IN ('agent', 'synthetic')
    ),
    source_transition TEXT NOT NULL CHECK (
        source_transition IN (
            'accepted',
            'retry',
            'dependency_released',
            'signal_released',
            'lease_expired'
        )
    ),
    source_generation BIGINT NOT NULL DEFAULT 0
        CHECK (source_generation >= 0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'publishing', 'published', 'transport_dead_lettered')
    ),
    publish_attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (publish_attempt_count >= 0),
    lease_generation BIGINT NOT NULL DEFAULT 0
        CHECK (lease_generation >= 0),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    failure_code TEXT CHECK (
        failure_code IN ('unavailable', 'rejected', 'timeout')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    UNIQUE (task_id, source_transition, source_generation),
    CHECK (
        (status = 'publishing') =
        (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CHECK (
        (status = 'published') = (published_at IS NOT NULL)
    )
);

CREATE INDEX task_wake_outbox_claim_idx
    ON task_wake_outbox (available_at, created_at, event_id)
    WHERE status IN ('pending', 'publishing');

