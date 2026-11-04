-- pyasynq Postgres schema, version 2.

-- An inserted task holds a key-share lock on its queue's registry row until
-- its transaction ends. Queue removal locks that row FOR UPDATE, so it waits
-- for uncommitted inserts (also ones made by plain SQL) and then sees them.
CREATE OR REPLACE FUNCTION pyasynq_on_task_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        LOOP
            PERFORM 1 FROM pyasynq_queues WHERE name = NEW.queue FOR KEY SHARE;
            EXIT WHEN FOUND;
            INSERT INTO pyasynq_queues (name) VALUES (NEW.queue) ON CONFLICT DO NOTHING;
        END LOOP;
    END IF;
    IF NEW.state = 'pending' THEN
        PERFORM pg_notify('pyasynq_pending', NEW.queue);
    END IF;
    RETURN NULL;
END;
$$;
