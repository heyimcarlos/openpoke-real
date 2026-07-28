CREATE TABLE workflow_step_interruption_waits (
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    step_id UUID NOT NULL UNIQUE,
    wait_id UUID NOT NULL UNIQUE,
    PRIMARY KEY (step_id, wait_id),
    FOREIGN KEY (instance_id, step_id)
        REFERENCES workflow_steps (instance_id, step_id),
    FOREIGN KEY (instance_id, wait_id)
        REFERENCES workflow_wait_blueprints (instance_id, wait_id)
);

CREATE TABLE workflow_run_state_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL,
    step_id UUID NOT NULL,
    task_id UUID NOT NULL REFERENCES execution_tasks(task_id),
    wait_id UUID NOT NULL UNIQUE REFERENCES workflow_waits(wait_id),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    lease_generation BIGINT NOT NULL CHECK (lease_generation > 0),
    codec_version INTEGER NOT NULL CHECK (codec_version > 0),
    agents_sdk_version TEXT NOT NULL CHECK (agents_sdk_version <> ''),
    agent_definition_version TEXT NOT NULL
        CHECK (agent_definition_version <> ''),
    model_requests_used INTEGER NOT NULL CHECK (model_requests_used >= 0),
    specialist_calls_used INTEGER NOT NULL CHECK (specialist_calls_used >= 0),
    state_json JSONB NOT NULL,
    state_sha256 TEXT NOT NULL CHECK (
        state_sha256 ~ '^[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (step_id, attempt_count),
    FOREIGN KEY (instance_id, step_id)
        REFERENCES workflow_steps (instance_id, step_id)
);

CREATE OR REPLACE FUNCTION openpoke_reject_run_state_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'persisted Workflow RunState is immutable';
END;
$$;

CREATE TRIGGER workflow_run_state_snapshots_immutable
BEFORE UPDATE OR DELETE ON workflow_run_state_snapshots
FOR EACH ROW
EXECUTE FUNCTION openpoke_reject_run_state_mutation();

INSERT INTO workflow_definitions (
    definition_key,
    definition_version,
    body,
    content_hash
)
VALUES (
    'openpoke.reasoning_approval_demo',
    1,
    '{
        "key": "openpoke.reasoning_approval_demo",
        "version": 1,
        "input_contract": [
            {"name": "question", "value_type": "string"},
            {"name": "evidence", "value_type": "string"},
            {"name": "constraints", "value_type": "string"}
        ],
        "entry_step": {
            "key": "decide",
            "agent_name": "bounded-reasoning-approval-manager",
            "executor_kind": "agent",
            "interruption_wait_key": "approval"
        },
        "waits": [
            {
                "key": "approval",
                "signal_key": "approve",
                "input_contract": [
                    {"name": "approval_note", "value_type": "string"}
                ]
            }
        ]
    }'::jsonb,
    '0dcbb6475dd98d485641fdc8f1dbc4ca3514e1e832fc7d54eff4831a69005874'
);
