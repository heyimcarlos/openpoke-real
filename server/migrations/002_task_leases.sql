ALTER TABLE execution_tasks
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_count >= 0),
    ADD COLUMN IF NOT EXISTS lease_generation BIGINT NOT NULL DEFAULT 0
        CHECK (lease_generation >= 0),
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

ALTER TABLE execution_tasks
    DROP CONSTRAINT IF EXISTS execution_tasks_status_check;

ALTER TABLE execution_tasks
    ADD CONSTRAINT execution_tasks_status_check
        CHECK (status IN ('queued', 'running', 'completed'));

CREATE INDEX IF NOT EXISTS execution_tasks_claim_idx
    ON execution_tasks (status, created_at);
