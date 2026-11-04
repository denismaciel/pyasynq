"""Regression tests for task ownership, recovery and option handling."""

import asyncio
import gc
import time
import weakref

import pyasynq
import pytest
from conftest import wait_for


def state(rdb, queue, tid):
    return rdb.hget(f"asynq:{{{queue}}}:t:{tid}", "state")


async def test_same_id_different_queues(client, rdb, make_worker):
    entered = set()
    release_a = asyncio.Event()
    release_b = asyncio.Event()

    async def handle(ctx):
        entered.add(ctx.queue)
        await (release_a if ctx.queue == "a" else release_b).wait()
        return ctx.queue.encode()

    for queue in ["a", "b"]:
        client.enqueue("work", task_id="same", queue=queue, retention=60)
    async with make_worker(handle, queues={"a": 2, "b": 1}, strict_priority=True, concurrency=2, shutdown_timeout=0.1):
        await wait_for(lambda: len(entered) == 2)
        release_a.set()
        await asyncio.sleep(0.2)
        observed = {q: (state(rdb, q, "same"), rdb.hget(f"asynq:{{{q}}}:t:same", "result")) for q in ["a", "b"]}
        release_b.set()
        await asyncio.sleep(0.1)
    assert observed == {"a": (b"completed", b"a"), "b": (b"active", None)}


async def test_old_cancel_timer_does_not_fail_retry(client, rdb, make_worker):
    attempts = []
    second_started = asyncio.Event()

    async def handle(ctx):
        attempts.append(ctx.retry_count)
        if ctx.retry_count == 1:
            second_started.set()
            await asyncio.sleep(0.8)
            return b"ok"
        await asyncio.sleep(30)

    info = client.enqueue("work", retention=60, max_retry=1, timeout=30)
    async with make_worker(handle, concurrency=1, retry_delay=lambda *_: 0, cancel_grace=0.4, shutdown_timeout=0.1):
        await wait_for(lambda: attempts == [0])
        # Wait for the cancel subscription, then cancel only the first attempt.
        await wait_for(lambda: rdb.pubsub_numsub("asynq:cancel")[0][1] > 0)
        rdb.publish("asynq:cancel", info.id)
        await asyncio.wait_for(second_started.wait(), 2)
        await asyncio.sleep(1)
        observed = state(rdb, "default", info.id)
    assert observed == b"completed", (observed, attempts)


async def test_failed_group_is_rediscovered(client, rdb, make_worker):
    called = []

    def aggregate(group, tasks):
        called.append(group)
        raise RuntimeError("temporary aggregator failure")

    client.enqueue("work", group="g")
    async with make_worker(
        lambda ctx: None, group_aggregator=aggregate, group_max_size=1, group_grace_period=1, recoverer_interval=0.05
    ):
        await wait_for(lambda: len(called) == 1)
        sets = rdb.zrange("asynq:{default}:aggregation_sets", 0, -1)
        assert len(sets) == 1
        # Simulate reaching the fixed 120-second reclamation deadline.
        rdb.zadd("asynq:{default}:aggregation_sets", {sets[0]: time.time() - 1})
        await wait_for(lambda: rdb.zcard("asynq:{default}:aggregation_sets") == 0)
        await asyncio.sleep(1.2)
        observed = (len(called), rdb.smembers("asynq:{default}:groups"), rdb.zcard("asynq:{default}:g:g"))
    assert observed[0] >= 2, observed


async def test_scheduled_groups_are_aggregated(client, rdb, make_worker):
    ordinary = []
    aggregated = []

    async def handle(ctx):
        ordinary.append(ctx.type)

    def aggregate(group, tasks):
        aggregated.append(group)
        return pyasynq.Task("batch", b"")

    client.enqueue("original", group="g", process_in=0.2)
    async with make_worker(handle, group_aggregator=aggregate, group_max_size=1, group_grace_period=1):
        await wait_for(lambda: ordinary or aggregated)
    assert aggregated == ["g"] and "original" not in ordinary, (ordinary, aggregated)


def test_subsecond_timeout_is_rejected(client):
    # asynq stores whole seconds and treats 0 as unset (30 minute default).
    with pytest.raises(ValueError, match="timeout"):
        client.enqueue("work", timeout=0.1)
    with pytest.raises(ValueError, match="retention"):
        client.enqueue("work", retention=0.5)
    assert client.enqueue("work", timeout=1.5).timeout.total_seconds() == 2


async def test_rate_limit_is_not_archived_with_zero_retries(client, rdb, make_worker):
    async def handle(ctx):
        raise pyasynq.RateLimited(after=30)

    info = client.enqueue("work", max_retry=0)
    async with make_worker(handle):
        await wait_for(lambda: state(rdb, "default", info.id) in [b"retry", b"archived"])
    assert state(rdb, "default", info.id) == b"retry"


async def test_nonempty_group_prevents_queue_deletion(client, redis_addr, rdb):
    client.enqueue("work", group="g")
    client.enqueue("done", queue="other", retention=60)
    inspector = pyasynq.Inspector(f"redis://{redis_addr}/0")
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.delete_queue("default")
    inspector.delete_queue("default", force=True)
    assert rdb.keys("asynq:{default}:*") == []
    assert inspector.queues() == ["other"]


async def test_worker_can_be_garbage_collected(redis_addr, rdb):
    worker = pyasynq.Worker(lambda ctx: None, redis_url=f"redis://{redis_addr}/0")
    ref = weakref.ref(worker)
    del worker
    gc.collect()
    assert ref() is None


async def test_positional_only_task():
    from pyasynq.testing import run_task

    app = pyasynq.Asynq()

    @app.task
    async def handler(n: int, /):
        return n + 1

    assert await run_task(handler, 1) == 2


async def test_empty_bytes_result(client, rdb, make_worker, redis_addr):
    async def handle(ctx):
        return b""

    info = client.enqueue("work", retention=60)
    async with make_worker(handle):
        await wait_for(lambda: state(rdb, "default", info.id) == b"completed")
    inspector = pyasynq.Inspector(f"redis://{redis_addr}/0")
    assert inspector.get_task_info("default", info.id).result == b""


async def test_old_python_callback_does_not_discard_new_attempt(client, rdb, make_worker):
    attempts = []
    finished = asyncio.Event()

    async def handle(ctx):
        attempts.append(ctx.retry_count)
        if ctx.retry_count == 0:
            try:
                await asyncio.sleep(30)
            finally:
                # Cleanup in the cancelled first attempt overlaps its fast retry.
                await asyncio.sleep(0.3)
        else:
            await asyncio.sleep(0.6)
            finished.set()
            return b"ok"

    info = client.enqueue("work", retention=60, timeout=30)
    async with make_worker(handle, concurrency=2, retry_delay=lambda *_: 0, cancel_grace=10, shutdown_timeout=0.1):
        await wait_for(lambda: attempts == [0])
        await wait_for(lambda: rdb.pubsub_numsub("asynq:cancel")[0][1] > 0)
        rdb.publish("asynq:cancel", info.id)
        await asyncio.wait_for(finished.wait(), 2)
        await asyncio.sleep(0.1)
        observed = state(rdb, "default", info.id)
    assert observed == b"completed", (observed, attempts)


async def test_handler_cancelled_error_releases_slot(client, rdb, make_worker):
    called = asyncio.Event()

    async def handle(ctx):
        called.set()
        raise asyncio.CancelledError()

    info = client.enqueue("work", max_retry=0)
    async with make_worker(handle, concurrency=1, shutdown_timeout=0.1):
        await asyncio.wait_for(called.wait(), 2)
        await asyncio.sleep(0.2)
        observed = state(rdb, "default", info.id)
    assert observed == b"archived", observed


async def test_queue_reappears_when_existing_client_reuses_it(client, rdb, redis_addr):
    info = client.enqueue("work")
    inspector = pyasynq.Inspector(f"redis://{redis_addr}/0")
    inspector.delete_task("default", info.id)
    inspector.delete_queue("default")
    info = client.enqueue("work")
    assert inspector.queues() == ["default"], (inspector.queues(), state(rdb, "default", info.id))


async def test_task_id_conflict_does_not_leave_uniqueness_lock(client):
    client.enqueue("first", task_id="taken")
    with pytest.raises(pyasynq.TaskIDConflictError):
        client.enqueue("second", b"payload", task_id="taken", unique=60)
    info = client.enqueue("second", b"payload", task_id="available", unique=60)
    assert info.id == "available"
