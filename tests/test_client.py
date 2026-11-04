import base64
import time
from datetime import datetime, timedelta, timezone

import pytest

from pyasynq import DuplicateTaskError, TaskIDConflictError
from pyasynq._core import ALL_QUEUES, queue_key_prefix, task_key


def test_enqueue_defaults_match_go(client, rdb, go_asynq):
    info = client.enqueue("email:send", b'{"to":"x"}')

    assert info.state == "pending"
    assert info.queue == "default"
    assert info.max_retry == 25
    assert info.timeout == timedelta(minutes=30)

    go = go_asynq("inspect", queue="default", id=info.id)
    assert go["state"] == "pending"
    assert go["type"] == "email:send"
    assert base64.b64decode(go["payload"]) == b'{"to":"x"}'
    assert go["max_retry"] == 25
    assert go["timeout_seconds"] == 1800
    assert rdb.smembers(ALL_QUEUES) == {b"default"}


def test_enqueue_with_options(client, go_asynq):
    deadline = datetime(2030, 1, 1, tzinfo=timezone.utc)
    info = client.enqueue(
        "report:build",
        b"\x00bin",
        queue="critical",
        max_retry=3,
        task_id="report-42",
        timeout=timedelta(seconds=60),
        deadline=deadline,
        retention=timedelta(hours=2),
        headers={"content-type": "application/octet-stream"},
    )
    assert info.id == "report-42"
    assert info.deadline == deadline

    go = go_asynq("inspect", queue="critical", id="report-42")
    assert go["max_retry"] == 3
    assert go["timeout_seconds"] == 60
    assert go["deadline"] == int(deadline.timestamp())
    assert go["retention_seconds"] == 7200
    assert go["headers"] == {"content-type": "application/octet-stream"}


def test_process_in_schedules_task(client, rdb, go_asynq):
    before = time.time()
    info = client.enqueue("t", process_in=timedelta(minutes=5))

    assert info.state == "scheduled"
    score = rdb.zscore(queue_key_prefix("default") + "scheduled", info.id)
    assert before + 299 <= score <= time.time() + 301
    assert rdb.llen(queue_key_prefix("default") + "pending") == 0

    go = go_asynq("inspect", queue="default", id=info.id)
    assert go["state"] == "scheduled"
    assert go["next_process_at"] == int(score)


def test_process_at_in_the_past_is_pending(client):
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    assert client.enqueue("t", process_at=past).state == "pending"


def test_unique_lock_is_shared_with_go(client, rdb, go_asynq):
    payload = b'{"user":1}'
    info = client.enqueue("sync:user", payload, unique=timedelta(hours=1))

    with pytest.raises(DuplicateTaskError):
        client.enqueue("sync:user", payload, unique=timedelta(hours=1))
    # Same md5-based unique key, so Go sees the lock taken by Python.
    go = go_asynq(
        "enqueue",
        queue="default",
        type="sync:user",
        payload=payload.decode(),
        unique="1h",
    )
    assert go == {"error": "task already exists"}

    unique_key = rdb.hget(task_key("default", info.id), "unique_key").decode()
    assert 3590 <= rdb.ttl(unique_key) <= 3600
    # A different payload is a different task.
    client.enqueue("sync:user", b'{"user":2}', unique=timedelta(hours=1))


def test_failed_unique_enqueue_writes_nothing(client, rdb):
    client.enqueue("t", b"p", unique=timedelta(minutes=1))
    keys_before = set(rdb.keys("*"))
    with pytest.raises(DuplicateTaskError):
        client.enqueue("t", b"p", unique=timedelta(minutes=1))
    assert set(rdb.keys("*")) == keys_before


def test_unique_schedule_lock_outlives_wait(client, rdb):
    info = client.enqueue(
        "t", process_in=timedelta(hours=1), unique=timedelta(minutes=10)
    )
    unique_key = rdb.hget(task_key("default", info.id), "unique_key").decode()
    assert 4190 <= rdb.ttl(unique_key) <= 4200


def test_task_id_conflict(client):
    client.enqueue("t", task_id="same")
    with pytest.raises(TaskIDConflictError):
        client.enqueue("t", task_id="same")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"task_type": " "},
        {"task_type": "t", "queue": ""},
        {"task_type": "t", "task_id": ""},
        {"task_type": "t", "unique": timedelta(milliseconds=500)},
        {
            "task_type": "t",
            "process_in": timedelta(1),
            "process_at": datetime.now(timezone.utc),
        },
    ],
)
def test_invalid_arguments(client, kwargs):
    with pytest.raises(ValueError):
        client.enqueue(**kwargs)


def test_ping(client):
    client.ping()
