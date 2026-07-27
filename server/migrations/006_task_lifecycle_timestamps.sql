ALTER TABLE execution_tasks
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

CREATE OR REPLACE FUNCTION openpoke_set_task_lifecycle_timestamps()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'running' AND OLD.status <> 'running' THEN
        NEW.started_at = COALESCE(OLD.started_at, clock_timestamp());
    END IF;

    IF NEW.status IN ('completed', 'dead_lettered', 'cancelled')
       AND OLD.status NOT IN ('completed', 'dead_lettered', 'cancelled') THEN
        NEW.finished_at = clock_timestamp();
    ELSIF NEW.status NOT IN ('completed', 'dead_lettered', 'cancelled') THEN
        NEW.finished_at = NULL;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS execution_tasks_lifecycle_timestamps
    ON execution_tasks;

CREATE TRIGGER execution_tasks_lifecycle_timestamps
BEFORE UPDATE ON execution_tasks
FOR EACH ROW
EXECUTE FUNCTION openpoke_set_task_lifecycle_timestamps();
