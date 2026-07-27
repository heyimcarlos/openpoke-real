ALTER TABLE execution_tasks
    DROP CONSTRAINT IF EXISTS execution_tasks_failure_code_check;

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_failure_code_check
        CHECK (
            failure_code IN (
                'synthetic_retryable',
                'synthetic_non_retryable',
                'lease_expired',
                'attempts_exhausted'
            )
        )
        NOT VALID;

ALTER TABLE execution_tasks
    VALIDATE CONSTRAINT execution_tasks_failure_code_check;
