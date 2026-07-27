ALTER TABLE execution_tasks
    DROP CONSTRAINT execution_tasks_status_check;

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_status_check
        CHECK (
            status IN (
                'blocked',
                'queued',
                'running',
                'completed',
                'dead_lettered',
                'cancelled'
            )
        );

ALTER TABLE workflow_steps
    DROP CONSTRAINT workflow_steps_status_check;

ALTER TABLE workflow_steps
    ADD COLUMN step_position INTEGER NOT NULL DEFAULT 0
        CHECK (step_position >= 0),
    ADD CONSTRAINT workflow_steps_instance_position_key
        UNIQUE (instance_id, step_position),
    ADD CONSTRAINT workflow_steps_instance_step_key
        UNIQUE (instance_id, step_id);

ALTER TABLE workflow_steps
    ADD CONSTRAINT workflow_steps_status_check
        CHECK (
            status IN (
                'blocked',
                'runnable',
                'running',
                'completed',
                'failed'
            )
        );

CREATE TABLE workflow_step_dependencies (
    instance_id UUID NOT NULL REFERENCES workflow_instances(instance_id),
    step_id UUID NOT NULL,
    prerequisite_step_id UUID NOT NULL,
    PRIMARY KEY (step_id, prerequisite_step_id),
    CHECK (step_id <> prerequisite_step_id),
    FOREIGN KEY (instance_id, step_id)
        REFERENCES workflow_steps (instance_id, step_id),
    FOREIGN KEY (instance_id, prerequisite_step_id)
        REFERENCES workflow_steps (instance_id, step_id)
);

CREATE INDEX workflow_step_dependencies_prerequisite_idx
    ON workflow_step_dependencies (prerequisite_step_id);

INSERT INTO workflow_definitions (
    definition_key,
    definition_version,
    body,
    content_hash
)
VALUES (
    'openpoke.parallel_demo',
    1,
    '{
        "key": "openpoke.parallel_demo",
        "version": 1,
        "input_contract": [
            {"name": "mode", "value_type": "string"},
            {"name": "duration_ms", "value_type": "integer"}
        ],
        "steps": [
            {
                "key": "extract_a",
                "agent_name": "extract-a",
                "executor_kind": "synthetic"
            },
            {
                "key": "extract_b",
                "agent_name": "extract-b",
                "executor_kind": "synthetic"
            },
            {
                "key": "validate",
                "agent_name": "validate",
                "executor_kind": "synthetic"
            }
        ],
        "dependencies": [
            {
                "step_key": "validate",
                "prerequisite_key": "extract_a"
            },
            {
                "step_key": "validate",
                "prerequisite_key": "extract_b"
            }
        ]
    }'::jsonb,
    '2c18cb84f76271a35f3822e55fe4562f7b46bdd0eae46e71f115c32a5c051203'
);
