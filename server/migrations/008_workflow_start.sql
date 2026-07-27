CREATE TABLE workflow_definitions (
    definition_key TEXT NOT NULL,
    definition_version INTEGER NOT NULL CHECK (definition_version > 0),
    body JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (definition_key, definition_version)
);

CREATE OR REPLACE FUNCTION openpoke_reject_definition_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published Workflow Definitions are immutable';
END;
$$;

CREATE TRIGGER workflow_definitions_immutable
BEFORE UPDATE OR DELETE ON workflow_definitions
FOR EACH ROW
EXECUTE FUNCTION openpoke_reject_definition_mutation();

INSERT INTO workflow_definitions (
    definition_key,
    definition_version,
    body,
    content_hash
)
VALUES (
    'openpoke.reliability_demo',
    1,
    '{
        "key": "openpoke.reliability_demo",
        "version": 1,
        "input_contract": [
            {"name": "mode", "value_type": "string"},
            {"name": "duration_ms", "value_type": "integer"}
        ],
        "entry_step": {
            "key": "execute",
            "agent_name": "reliability-demo",
            "executor_kind": "synthetic"
        }
    }'::jsonb,
    '136f91a7296d49456466f24cf3533f3b9abc0c0ea84aec30c27e0bacad4bc157'
);

CREATE TABLE workflow_instances (
    instance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    definition_key TEXT NOT NULL,
    definition_version INTEGER NOT NULL,
    definition_hash TEXT NOT NULL,
    input JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'completed', 'failed', 'cancelled')),
    origin_thread_id UUID REFERENCES conversation_threads(thread_id),
    origin_agent_run_id UUID REFERENCES agent_runs(run_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (definition_key, definition_version)
        REFERENCES workflow_definitions (
            definition_key,
            definition_version
        )
);

CREATE INDEX workflow_instances_tenant_created_idx
    ON workflow_instances (tenant_id, created_at);

CREATE TABLE workflow_steps (
    step_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    step_key TEXT NOT NULL,
    execution_task_id UUID NOT NULL REFERENCES execution_tasks(task_id),
    status TEXT NOT NULL DEFAULT 'runnable'
        CHECK (status IN ('runnable', 'running', 'completed', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (instance_id, step_key),
    UNIQUE (execution_task_id)
);

CREATE TABLE workflow_events (
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, sequence)
);
