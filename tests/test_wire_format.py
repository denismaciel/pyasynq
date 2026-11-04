"""Wire compatibility with Go asynq, in both directions."""

import base64
import time

from pyasynq._core import ALL_QUEUES, TaskMessage, pending_key, task_key


def test_decode_task_written_by_go(rdb, go_asynq):
    info = go_asynq("enqueue", queue="critical")

    raw = rdb.hget(task_key("critical", info["id"]), "msg")
    msg = TaskMessage.decode(raw)

    assert msg.id == info["id"]
    assert msg.type == "email:send"
    assert msg.payload == b'{"to":"denis@example.com"}'
    assert msg.headers == {"content-type": "application/json"}
    assert msg.queue == "critical"
    assert msg.retry == 7
    assert msg.timeout == 90
    assert msg.retention == 3600
    assert rdb.lrange(pending_key("critical"), 0, -1) == [info["id"].encode()]


def test_go_reads_task_written_by_python(rdb, go_asynq):
    msg = TaskMessage(
        "report:build",
        b"\x00\x01binary-ok",
        id="py-task-1",
        queue="critical",
        retry=3,
        timeout=60,
        headers={"content-type": "application/octet-stream"},
    )
    # What asynq's enqueue Lua script does, spelled out by hand.
    rdb.sadd(ALL_QUEUES, "critical")
    rdb.hset(
        task_key("critical", msg.id),
        mapping={
            "msg": msg.encode(),
            "state": "pending",
            "pending_since": time.time_ns(),
        },
    )
    rdb.lpush(pending_key("critical"), msg.id)

    info = go_asynq("inspect", queue="critical", id=msg.id)

    assert info["type"] == "report:build"
    assert base64.b64decode(info["payload"]) == b"\x00\x01binary-ok"
    assert info["headers"] == {"content-type": "application/octet-stream"}
    assert info["state"] == "pending"
    assert info["max_retry"] == 3
    assert info["timeout_seconds"] == 60


def test_roundtrip_and_validation():
    msg = TaskMessage("t", b"p", id="1", queue="default", retry=25)
    assert TaskMessage.decode(msg.encode()) == msg
    assert isinstance(msg.payload, bytes)
    try:
        TaskMessage.decode(b"\xff\xff\xff")
    except ValueError:
        pass
    else:
        raise AssertionError("garbage should not decode")
