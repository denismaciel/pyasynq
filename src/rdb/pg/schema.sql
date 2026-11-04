-- pyasynq Postgres schema, version 1. Applied automatically on connect.

CREATE TABLE pyasynq_queues (
    name text PRIMARY KEY,
    paused boolean NOT NULL DEFAULT false
);

-- One row per task. `msg` is the asynq TaskMessage protobuf, the same bytes
-- the Redis backend stores; the other columns index the task's state.
CREATE TABLE pyasynq_tasks (
    queue text NOT NULL,
    id text NOT NULL,
    state text NOT NULL CHECK (
        state IN ('pending', 'active', 'scheduled', 'retry', 'archived', 'completed', 'aggregating')
    ),
    msg bytea NOT NULL,
    pending_since timestamptz,
    process_at timestamptz,
    lease_expires_at timestamptz,
    group_key text,
    grouped_at timestamptz,
    aggregation_set text,
    unique_key text,
    result bytea,
    archived_at timestamptz,
    expires_at timestamptz,
    PRIMARY KEY (queue, id)
);

CREATE INDEX pyasynq_tasks_pending ON pyasynq_tasks (queue, pending_since, id)
    WHERE state = 'pending';
CREATE INDEX pyasynq_tasks_delayed ON pyasynq_tasks (queue, process_at)
    WHERE state IN ('scheduled', 'retry');
CREATE INDEX pyasynq_tasks_leases ON pyasynq_tasks (queue, lease_expires_at)
    WHERE state = 'active';
CREATE INDEX pyasynq_tasks_groups ON pyasynq_tasks (queue, group_key, grouped_at)
    WHERE state = 'aggregating';
CREATE INDEX pyasynq_tasks_archived ON pyasynq_tasks (queue, archived_at)
    WHERE state = 'archived';
CREATE INDEX pyasynq_tasks_completed ON pyasynq_tasks (queue, expires_at)
    WHERE state = 'completed';

-- Uniqueness locks outlive their task (they expire on their own TTL), so
-- they can't be a unique index on pyasynq_tasks.
CREATE TABLE pyasynq_unique_locks (
    key text PRIMARY KEY,
    task_id text NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX pyasynq_unique_locks_expiry ON pyasynq_unique_locks (expires_at);

CREATE TABLE pyasynq_aggregation_sets (
    queue text NOT NULL,
    group_key text NOT NULL,
    set_id text NOT NULL,
    deadline timestamptz NOT NULL,
    PRIMARY KEY (queue, group_key, set_id)
);

-- Counters are spread over shards so concurrent workers don't serialize on
-- one row; readers sum the shards.
CREATE TABLE pyasynq_stats (
    queue text NOT NULL,
    day date NOT NULL,
    shard smallint NOT NULL,
    processed bigint NOT NULL DEFAULT 0,
    failed bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (queue, day, shard)
);

CREATE FUNCTION pyasynq_count(q text, d date, n_processed bigint, n_failed bigint)
RETURNS integer LANGUAGE sql AS $$
    INSERT INTO pyasynq_stats AS s (queue, day, shard, processed, failed)
    VALUES (q, d, floor(random() * 8)::smallint, n_processed, n_failed)
    ON CONFLICT (queue, day, shard) DO UPDATE
    SET processed = s.processed + EXCLUDED.processed, failed = s.failed + EXCLUDED.failed
    RETURNING 1;
$$;

CREATE TABLE pyasynq_servers (
    key text PRIMARY KEY,
    info bytea NOT NULL,
    workers bytea[] NOT NULL,
    expires_at timestamptz NOT NULL
);

CREATE TABLE pyasynq_scheduler_entries (
    scheduler_id text NOT NULL,
    position integer NOT NULL,
    entry bytea NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scheduler_id, position)
);

CREATE TABLE pyasynq_scheduler_events (
    entry_id text NOT NULL,
    enqueued_at timestamptz NOT NULL,
    event bytea NOT NULL
);
CREATE INDEX pyasynq_scheduler_events_entry
    ON pyasynq_scheduler_events (entry_id, enqueued_at DESC);

CREATE TABLE pyasynq_semaphores (
    scope text NOT NULL,
    task_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scope, task_id)
);

-- Registers the queue and wakes LISTENing workers, also for tasks inserted
-- by plain SQL in an application transaction.
CREATE FUNCTION pyasynq_on_task_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO pyasynq_queues (name) VALUES (NEW.queue) ON CONFLICT DO NOTHING;
    END IF;
    IF NEW.state = 'pending' THEN
        PERFORM pg_notify('pyasynq_pending', NEW.queue);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER pyasynq_task_inserted AFTER INSERT ON pyasynq_tasks
    FOR EACH ROW EXECUTE FUNCTION pyasynq_on_task_change();
CREATE TRIGGER pyasynq_task_pending AFTER UPDATE OF state ON pyasynq_tasks
    FOR EACH ROW WHEN (NEW.state = 'pending' AND OLD.state <> 'pending')
    EXECUTE FUNCTION pyasynq_on_task_change();
