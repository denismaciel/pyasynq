"""Dequeue, leases, and finishing tasks: the worker's Redis transitions."""

import time
from datetime import datetime, timedelta, timezone

import pytest

from pyasynq import TaskNotFoundError
from pyasynq._core import Broker, queue_key_prefix, task_key


@pytest.fixture
def broker(rdb, redis_addr):
    return Broker(f"redis://{redis_addr}/0")


def key(queue: str, suffix: str) -> str:
    return queue_key_prefix(queue) + suffix


def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_dequeue_empty_returns_none(broker):
    assert broker.dequeue("default") is None


def test_dequeue_moves_task_to_active_with_lease(client, broker, rdb, go_asynq):
    info = client.enqueue("email:send", b"hi")

    msg, lease = broker.dequeue("default")

    assert msg.id == info.id
    assert msg.payload == b"hi"
    assert rdb.llen(key("default", "pending")) == 0
    assert rdb.lrange(key("default", "active"), 0, -1) == [info.id.encode()]
    assert rdb.zscore(key("default", "lease"), info.id) == int(lease.timestamp())
    assert 29 <= (lease - datetime.now(timezone.utc)).total_seconds() <= 30
    assert rdb.hget(task_key("default", info.id), "state") == b"active"
    assert not rdb.hexists(task_key("default", info.id), "pending_since")
    assert go_asynq("inspect", queue="default", id=info.id)["state"] == "active"


def test_dequeue_is_fifo(client, broker):
    ids = [client.enqueue("t").id for _ in range(3)]
    assert [broker.dequeue("default")[0].id for _ in range(3)] == ids


def test_dequeue_tries_queues_in_order(client, broker):
    client.enqueue("low-task", queue="low")
    client.enqueue("high-task", queue="high")
    assert broker.dequeue("high", "low")[0].type == "high-task"
    assert broker.dequeue("high", "low")[0].type == "low-task"


def test_paused_queue_is_skipped(client, broker, go_asynq):
    client.enqueue("in-paused", queue="paused-q")
    client.enqueue("in-other", queue="other")
    assert go_asynq("pause", queue="paused-q") == {"paused": True}

    assert broker.dequeue("paused-q", "other")[0].type == "in-other"
    assert broker.dequeue("paused-q") is None


def test_done_deletes_task_and_counts_it(client, broker, rdb, go_asynq):
    info = client.enqueue("t")
    msg, _ = broker.dequeue("default")

    broker.done(msg)

    assert not rdb.exists(task_key("default", info.id))
    assert rdb.llen(key("default", "active")) == 0
    assert rdb.zcard(key("default", "lease")) == 0
    daily = key("default", f"processed:{today()}")
    assert rdb.get(daily) == b"1"
    assert rdb.ttl(daily) > 89 * 24 * 3600
    assert rdb.get(key("default", "processed")) == b"1"

    queue = go_asynq("queue", queue="default")
    assert queue["Processed"] == 1
    assert queue["ProcessedTotal"] == 1
    assert "error" in go_asynq("inspect", queue="default", id=info.id)


def test_done_releases_unique_lock(client, broker):
    client.enqueue("t", b"p", unique=timedelta(hours=1))
    broker.done(broker.dequeue("default")[0])
    client.enqueue("t", b"p", unique=timedelta(hours=1))


def test_done_twice_is_not_found(client, broker):
    client.enqueue("t")
    msg, _ = broker.dequeue("default")
    broker.done(msg)
    with pytest.raises(TaskNotFoundError):
        broker.done(msg)


def test_mark_as_complete_keeps_task_for_retention(client, broker, rdb, go_asynq):
    info = client.enqueue("t", retention=timedelta(hours=1))
    msg, _ = broker.dequeue("default")

    broker.mark_as_complete(msg)

    expires = rdb.zscore(key("default", "completed"), info.id)
    assert time.time() + 3590 <= expires <= time.time() + 3601
    assert rdb.hget(task_key("default", info.id), "state") == b"completed"
    assert rdb.llen(key("default", "active")) == 0
    go = go_asynq("inspect", queue="default", id=info.id)
    assert go["state"] == "completed"
    assert go_asynq("queue", queue="default")["Completed"] == 1


def test_crashed_worker_leaves_task_recoverable(client, broker, rdb):
    """At-least-once: a worker that dies after dequeue loses nothing."""
    info = client.enqueue("t")
    broker.dequeue("default")
    # The "worker" now disappears without calling done().

    assert rdb.lrange(key("default", "active"), 0, -1) == [info.id.encode()]
    assert rdb.zscore(key("default", "lease"), info.id) is not None
    assert rdb.hget(task_key("default", info.id), "msg") is not None


def test_go_enqueued_task_processed_by_python(broker, go_asynq):
    info = go_asynq("enqueue", queue="critical")
    msg, _ = broker.dequeue("critical")
    assert msg.id == info["id"]
    broker.mark_as_complete(msg)  # Go set Retention(1h)
    assert go_asynq("inspect", queue="critical", id=info["id"])["state"] == "completed"
