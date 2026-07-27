CREATE TABLE conversation_threads (
    thread_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    composio_user_id TEXT,
    channel_key TEXT NOT NULL,
    next_ingress_sequence BIGINT NOT NULL DEFAULT 0
        CHECK (next_ingress_sequence >= 0),
    next_context_sequence BIGINT NOT NULL DEFAULT 0
        CHECK (next_context_sequence >= 0),
    processed_ingress_sequence BIGINT NOT NULL DEFAULT 0
        CHECK (processed_ingress_sequence >= 0),
    context_generation BIGINT NOT NULL DEFAULT 0
        CHECK (context_generation >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, actor_id, channel_key)
);

CREATE TABLE agent_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(thread_id),
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    ingress_cutoff BIGINT NOT NULL CHECK (ingress_cutoff > 0),
    context_cutoff BIGINT NOT NULL DEFAULT 0 CHECK (context_cutoff >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    delegation_count INTEGER NOT NULL DEFAULT 0
        CHECK (delegation_count BETWEEN 0 AND 2),
    lease_generation BIGINT NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX agent_runs_one_active_per_thread_idx
    ON agent_runs (thread_id)
    WHERE status IN ('queued', 'running');

CREATE INDEX agent_runs_claim_idx
    ON agent_runs (status, created_at);

CREATE TABLE conversation_messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id UUID NOT NULL REFERENCES conversation_threads(thread_id),
    ingress_sequence BIGINT,
    context_sequence BIGINT,
    context_generation BIGINT NOT NULL,
    source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'agent', 'tool')),
    content TEXT NOT NULL,
    sdk_item JSONB NOT NULL,
    caused_by_run_id UUID REFERENCES agent_runs(run_id),
    caused_by_task_id UUID REFERENCES execution_tasks(task_id),
    producer_generation BIGINT,
    producer_index BIGINT,
    committed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (thread_id, source_kind, source_id),
    UNIQUE (thread_id, context_generation, context_sequence),
    CHECK (ingress_sequence IS NULL OR ingress_sequence > 0)
);

CREATE UNIQUE INDEX conversation_messages_producer_idx
    ON conversation_messages (
        caused_by_run_id,
        producer_generation,
        producer_index
    )
    WHERE producer_generation IS NOT NULL;

CREATE UNIQUE INDEX conversation_messages_ingress_sequence_idx
    ON conversation_messages (thread_id, ingress_sequence)
    WHERE ingress_sequence IS NOT NULL;

CREATE INDEX conversation_messages_context_idx
    ON conversation_messages (
        thread_id,
        context_generation,
        context_sequence
    );

CREATE TABLE agent_run_delegations (
    run_id UUID NOT NULL REFERENCES agent_runs(run_id),
    semantic_key TEXT NOT NULL,
    task_id UUID NOT NULL REFERENCES execution_tasks(task_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, semantic_key),
    UNIQUE (task_id)
);

ALTER TABLE execution_tasks
    ADD COLUMN origin_thread_id UUID REFERENCES conversation_threads(thread_id),
    ADD COLUMN origin_agent_run_id UUID REFERENCES agent_runs(run_id);

CREATE INDEX execution_tasks_origin_run_idx
    ON execution_tasks (origin_agent_run_id)
    WHERE origin_agent_run_id IS NOT NULL;
