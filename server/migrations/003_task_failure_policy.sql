ALTER TABLE execution_tasks
    ADD COLUMN IF NOT EXISTS failure_code TEXT
        CHECK (
            failure_code IN (
                'synthetic_retryable',
                'synthetic_non_retryable',
                'lease_expired',
                'attempts_exhausted'
            )
        );

ALTER TABLE execution_tasks
    DROP CONSTRAINT IF EXISTS execution_tasks_status_check;

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_status_check
        CHECK (
            status IN (
                'queued',
                'running',
                'completed',
                'dead_lettered',
                'cancelled'
            )
        );
