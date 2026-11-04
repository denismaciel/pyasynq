"""Inspector: reading and managing queues, cross-checked with Go."""

import asyncio
from datetime import timedelta

import pytest

import pyasynq
from pyasynq.inspector import Inspector


@pytest.fixture
def inspector(rdb, redis_addr):
    return Inspector(f"redis://{redis_addr}/0")


def test_queue_info_and_lists(client, inspector, go_asynq):
    p1 = client.enqueue("a", b"1")
    p2 = client.enqueue("a", b"2")
    s = client.enqueue("b", process_in=timedelta(hours=1))
    go = go_asynq("enqueue", queue="default", type="go:task", payload="x")

    assert inspector.queues() == ["default"]
    q = inspector.get_queue_info("default")
    assert (q.pending, q.scheduled, q.size, q.paused) == (3, 1, 4, False)
    assert q.latency >= timedelta(0)

    pending = inspector.list_pending_tasks("default")
    assert [t.id for t in pending] == [p1.id, p2.id, go["id"]]  # oldest first
    assert [t.id for t in inspector.list_tasks("default", "pending", page=2, size=2)] == [go["id"]]
    [sched] = inspector.list_scheduled_tasks("default")
    assert sched.id == s.id and sched.next_process_at == s.next_process_at.replace(microsecond=0)

    go_view = go_asynq("queue", queue="default")
    assert (go_view["Pending"], go_view["Scheduled"], go_view["Size"]) == (3, 1, 4)


def test_task_operations(client, inspector, go_asynq):
    t = client.enqueue("a", process_in=3600)
    inspector.run_task("default", t.id)
    assert inspector.get_task_info("default", t.id).state == "pending"
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.run_task("default", t.id)
    inspector.archive_task("default", t.id)
    assert go_asynq("inspect", queue="default", id=t.id)["state"] == "archived"
    inspector.delete_task("default", t.id)
    with pytest.raises(pyasynq.TaskNotFoundError):
        inspector.get_task_info("default", t.id)
    with pytest.raises(pyasynq.QueueNotFoundError):
        inspector.get_task_info("nope", t.id)


def test_bulk_operations(client, inspector):
    for _ in range(3):
        client.enqueue("a", process_in=3600)
    assert inspector.archive_all_tasks("default", "scheduled") == 3
    assert inspector.get_queue_info("default").archived == 3
    assert inspector.run_all_tasks("default", "archived") == 3
    assert inspector.get_queue_info("default").pending == 3
    assert inspector.delete_all_tasks("default", "pending") == 3
    assert inspector.get_queue_info("default").size == 0
    inspector.delete_queue("default")
    assert inspector.queues() == []


def test_pause_blocks_processing(client, inspector, go_asynq):
    client.enqueue("a")
    inspector.pause_queue("default")
    assert go_asynq("queue", queue="default")["Paused"] is True
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.pause_queue("default")
    inspector.unpause_queue("default")
    assert inspector.get_queue_info("default").paused is False


async def test_paused_queue_is_skipped_by_worker(client, inspector, make_worker):
    seen = []

    async def handler(ctx):
        seen.append(ctx.queue)

    client.enqueue("a", queue="paused")
    client.enqueue("a", queue="open")
    inspector.pause_queue("paused")
    async with make_worker(handler, queues=["paused", "open"]):
        await asyncio.sleep(0.4)
        assert seen == ["open"]
        inspector.unpause_queue("paused")
        await asyncio.sleep(0.3)
    assert sorted(seen) == ["open", "paused"]


async def test_servers_and_history(client, inspector, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    info = client.enqueue("a", b"p")
    async with make_worker(handler, shutdown_timeout=0.2) as w:
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.sleep(0.3)
        [server] = inspector.servers()
        assert server.id == w.server_id and server.status == "active"
        [worker] = server.active_workers
        assert (worker.task_id, worker.task_payload) == (info.id, b"p")
        [active] = inspector.list_active_tasks("default")
        assert active.id == info.id
    today = inspector.history("default", days=2)[0]
    assert today.processed == 0  # requeued on shutdown, so not processed


def test_completed_task_result(client, inspector, rdb):
    info = client.enqueue("a", retention=60)
    rdb.hset(f"asynq:{{default}}:t:{info.id}", mapping={"state": "completed", "result": b"42"})
    rdb.lrem("asynq:{default}:pending", 0, info.id)
    rdb.zadd("asynq:{default}:completed", {info.id: 9999999999})
    got = inspector.get_task_info("default", info.id)
    assert (got.state, got.result) == ("completed", b"42")
    [listed] = inspector.list_completed_tasks("default")
    assert listed.result == b"42"
