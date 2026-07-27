ALTER TABLE execution_tasks
    ADD COLUMN IF NOT EXISTS executor_kind TEXT NOT NULL DEFAULT 'agent';

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_executor_kind_check
        CHECK (executor_kind IN ('agent', 'synthetic'))
        NOT VALID;

ALTER TABLE execution_tasks
    VALIDATE CONSTRAINT execution_tasks_executor_kind_check;

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
                'agent_non_retryable',
                'unknown_executor'
            )
        )
        NOT VALID;

ALTER TABLE execution_tasks
    VALIDATE CONSTRAINT execution_tasks_failure_code_check;
