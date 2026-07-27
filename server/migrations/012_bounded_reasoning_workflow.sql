ALTER TABLE execution_tasks
    DROP CONSTRAINT IF EXISTS execution_tasks_failure_code_check;

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_failure_code_check
        CHECK (
            failure_code IN (
                'synthetic_retryable',
                'synthetic_non_retryable',
                'lease_expired',
                'attempts_exhausted',
                'execution_timeout',
                'agent_retryable',
                'agent_non_retryable',
                'unknown_executor'
            )
        )
        NOT VALID;

ALTER TABLE execution_tasks
    VALIDATE CONSTRAINT execution_tasks_failure_code_check;

INSERT INTO workflow_definitions (
    definition_key,
    definition_version,
    body,
    content_hash
)
VALUES (
    'openpoke.reasoning_demo',
    1,
    '{
        "key": "openpoke.reasoning_demo",
        "version": 1,
        "input_contract": [
            {"name": "question", "value_type": "string"},
            {"name": "evidence", "value_type": "string"},
            {"name": "constraints", "value_type": "string"}
        ],
        "entry_step": {
            "key": "decide",
            "agent_name": "bounded-reasoning-manager",
            "executor_kind": "agent"
        }
    }'::jsonb,
    'ebc70cddd84b3b3b10da49266eb8b8e78654e4addf84473217e565d7e5755743'
);
