CREATE TABLE IF NOT EXISTS execution_tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    origin_turn_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    input JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued')),
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS execution_tasks_tenant_status_idx
    ON execution_tasks (tenant_id, status, created_at);
