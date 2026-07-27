CREATE TABLE workflow_wait_blueprints (
    wait_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    wait_key TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    input_contract JSONB NOT NULL,
    UNIQUE (instance_id, wait_key),
    UNIQUE (instance_id, wait_id)
);

CREATE TABLE workflow_wait_prerequisites (
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    wait_id UUID NOT NULL,
    prerequisite_step_id UUID NOT NULL,
    PRIMARY KEY (wait_id, prerequisite_step_id),
    FOREIGN KEY (instance_id, wait_id)
        REFERENCES workflow_wait_blueprints (instance_id, wait_id),
    FOREIGN KEY (instance_id, prerequisite_step_id)
        REFERENCES workflow_steps (instance_id, step_id)
);

CREATE TABLE workflow_wait_routes (
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    wait_id UUID NOT NULL,
    step_id UUID NOT NULL,
    PRIMARY KEY (wait_id, step_id),
    FOREIGN KEY (instance_id, wait_id)
        REFERENCES workflow_wait_blueprints (instance_id, wait_id),
    FOREIGN KEY (instance_id, step_id)
        REFERENCES workflow_steps (instance_id, step_id)
);

CREATE INDEX workflow_wait_routes_step_idx
    ON workflow_wait_routes (step_id);

CREATE TABLE workflow_waits (
    wait_id UUID PRIMARY KEY,
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'satisfied', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    satisfied_at TIMESTAMPTZ,
    UNIQUE (instance_id, wait_id),
    FOREIGN KEY (instance_id, wait_id)
        REFERENCES workflow_wait_blueprints (instance_id, wait_id)
);

CREATE TABLE workflow_signals (
    signal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wait_id UUID NOT NULL UNIQUE REFERENCES workflow_waits(wait_id),
    tenant_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    input JSONB NOT NULL,
    origin_thread_id UUID REFERENCES conversation_threads(thread_id),
    origin_agent_run_id UUID REFERENCES agent_runs(run_id),
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, idempotency_key)
);

ALTER TABLE workflow_waits
    ADD COLUMN satisfied_by_signal_id UUID UNIQUE
        REFERENCES workflow_signals(signal_id);

INSERT INTO workflow_definitions (
    definition_key,
    definition_version,
    body,
    content_hash
)
VALUES (
    'openpoke.approval_demo',
    1,
    '{
        "key": "openpoke.approval_demo",
        "version": 1,
        "input_contract": [
            {"name": "mode", "value_type": "string"},
            {"name": "duration_ms", "value_type": "integer"}
        ],
        "entry_step": {
            "key": "apply",
            "agent_name": "approved-action",
            "executor_kind": "synthetic"
        },
        "waits": [
            {
                "key": "approval",
                "signal_key": "approve",
                "input_contract": [
                    {"name": "approval_note", "value_type": "string"}
                ]
            }
        ],
        "wait_routes": [
            {"wait_key": "approval", "step_key": "apply"}
        ]
    }'::jsonb,
    '0bc0bc0c8a44a06c146d0ab3e1f210fe2e6959a33b86871113ca32374d5dbdb1'
);
