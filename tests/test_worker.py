"""The processing loop: dequeue, run handler, record the outcome."""

import asyncio
import threading
import time

import pyasynq
from conftest import wait_for


def state(rdb, task_id, queue="default"):
    raw = rdb.hget(f"asynq:{{{queue}}}:t:{task_id}", "state")
    return raw.decode() if raw else None


def message(rdb, task_id, queue="default"):
    raw = rdb.hget(f"asynq:{{{queue}}}:t:{task_id}", "msg")
    return pyasynq.TaskMessage.decode(raw)


async def test_processes_tasks(client, rdb, make_worker):
    seen = []

    async def handler(ctx):
        seen.append((ctx.type, ctx.payload, pyasynq.current_task().id))

    infos = [client.enqueue("greet", f"hi {i}".encode()) for i in range(20)]
    async with make_worker(handler, concurrency=4):
        await wait_for(lambda: len(seen) == 20)
        await wait_for(lambda: rdb.llen("asynq:{default}:active") == 0)
    assert sorted(p for _, p, _ in seen) == sorted(f"hi {i}".encode() for i in range(20))
    assert {i for _, _, i in seen} == {i.id for i in infos}
    assert rdb.get("asynq:{default}:processed").decode() == "20"
    assert rdb.exists(f"asynq:{{default}}:t:{infos[0].id}") == 0


async def test_failure_is_retried_with_error(client, rdb, make_worker):
    async def handler(ctx):
        raise ValueError("boom")

    info = client.enqueue("t", b"")
    async with make_worker(handler):
        await wait_for(lambda: state(rdb, info.id) == "retry")
    msg = message(rdb, info.id)
    assert msg.retried == 1
    assert msg.error_msg == "ValueError: boom"
    score = rdb.zscore("asynq:{default}:retry", info.id)
    assert time.time() + 14 < score < time.time() + 46  # 0^4 + 15 + rand(30)
    assert rdb.get("asynq:{default}:failed").decode() == "1"


async def test_retry_control_flow(client, rdb, make_worker):
    async def handler(ctx):
        kind = ctx.payload.decode()
        if kind == "skip":
            raise pyasynq.SkipRetry("bad input")
        if kind == "revoke":
            raise pyasynq.RevokeTask()
        if kind == "later":
            raise pyasynq.Retry("rate limited", after=100)

    skip = client.enqueue("t", b"skip")
    revoke = client.enqueue("t", b"revoke")
    later = client.enqueue("t", b"later")
    exhausted_info = client.enqueue("x", b"", max_retry=0)

    async def failing(ctx):
        if ctx.type == "x":
            raise RuntimeError("nope")
        return await handler(ctx)

    async with make_worker(failing):
        await wait_for(lambda: state(rdb, skip.id) == "archived")
        await wait_for(lambda: state(rdb, exhausted_info.id) == "archived")
        await wait_for(lambda: state(rdb, later.id) == "retry")
        await wait_for(lambda: rdb.exists(f"asynq:{{default}}:t:{revoke.id}") == 0)
    assert message(rdb, skip.id).error_msg == "SkipRetry: bad input"
    score = rdb.zscore("asynq:{default}:retry", later.id)
    assert abs(score - (time.time() + 100)) < 3


async def test_custom_retry_delay_and_is_failure(client, rdb, make_worker):
    class Transient(Exception):
        pass

    async def handler(ctx):
        raise Transient()

    info = client.enqueue("t")
    async with make_worker(
        handler,
        retry_delay=lambda n, exc, ctx: 1000 + n,
        is_failure=lambda exc: not isinstance(exc, Transient),
    ):
        await wait_for(lambda: state(rdb, info.id) == "retry")
    # Not a failure: the retry budget and the failed counters are untouched.
    assert message(rdb, info.id).retried == 0
    assert rdb.get("asynq:{default}:failed") is None
    assert abs(rdb.zscore("asynq:{default}:retry", info.id) - (time.time() + 1000)) < 3


async def test_error_handler_receives_exception(client, rdb, make_worker):
    errors = []

    async def handler(ctx):
        raise KeyError("missing")

    async def on_error(ctx, exc):
        errors.append((ctx.id, type(exc)))

    info = client.enqueue("t")
    async with make_worker(handler, error_handler=on_error):
        await wait_for(lambda: errors)
    assert errors == [(info.id, KeyError)]


async def test_sync_handler_runs_in_thread(client, rdb, make_worker):
    threads = []

    def handler(ctx):
        threads.append((threading.current_thread().name, pyasynq.current_task().id))
        time.sleep(0.05)

    infos = [client.enqueue("t") for _ in range(4)]
    async with make_worker(handler, concurrency=4):
        await wait_for(lambda: len(threads) == 4)
    assert all(name.startswith("pyasynq-task") for name, _ in threads)
    assert {i for _, i in threads} == {i.id for i in infos}


async def test_result_is_kept_with_retention(client, rdb, make_worker):
    async def handler(ctx):
        await ctx.write_result(b"partial")
        return b"final"

    info = client.enqueue("t", retention=3600)
    async with make_worker(handler):
        await wait_for(lambda: state(rdb, info.id) == "completed")
    key = f"asynq:{{default}}:t:{info.id}"
    assert rdb.hget(key, "result") == b"final"
    assert message(rdb, info.id).completed_at > 0


async def test_deadline_cancels_and_retries(client, rdb, make_worker):
    cancelled = asyncio.Event()

    async def handler(ctx):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    info = client.enqueue("t", timeout=1)
    async with make_worker(handler):
        await wait_for(lambda: state(rdb, info.id) == "retry", timeout=5)
        await asyncio.wait_for(cancelled.wait(), 1)
    assert message(rdb, info.id).error_msg == "context deadline exceeded"


async def test_sync_handler_sees_cooperative_cancel(client, rdb, make_worker):
    observed = threading.Event()

    def handler(ctx):
        while not ctx.cancelled:
            time.sleep(0.01)
        assert ctx.cancel_reason == "deadline_exceeded"
        observed.set()

    info = client.enqueue("t", timeout=1)
    async with make_worker(handler):
        await wait_for(lambda: state(rdb, info.id) == "retry", timeout=5)
        await wait_for(observed.is_set)


async def test_cancel_via_pubsub(client, rdb, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    info = client.enqueue("t")
    async with make_worker(handler):
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.sleep(0.2)  # let the subscriber connect
        rdb.publish("asynq:cancel", info.id)
        await wait_for(lambda: state(rdb, info.id) == "retry")
    assert message(rdb, info.id).error_msg == "context canceled"


async def test_shutdown_requeues_unfinished_tasks(client, rdb, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    info = client.enqueue("t")
    t0 = time.monotonic()
    async with make_worker(handler, shutdown_timeout=0.3):
        await asyncio.wait_for(started.wait(), 3)
    assert time.monotonic() - t0 < 5
    assert state(rdb, info.id) == "pending"
    assert rdb.lrange("asynq:{default}:pending", 0, -1) == [info.id.encode()]
    assert rdb.zcard("asynq:{default}:lease") == 0


async def test_shutdown_waits_for_running_tasks(client, rdb, make_worker):
    done = []

    async def handler(ctx):
        await asyncio.sleep(0.5)
        done.append(ctx.id)

    info = client.enqueue("t")
    async with make_worker(handler, shutdown_timeout=5) as w:
        await wait_for(lambda: w.active_count == 1)
    assert done == [info.id]
    assert rdb.exists(f"asynq:{{default}}:t:{info.id}") == 0


async def test_stop_pauses_fetching(client, rdb, make_worker):
    seen = []

    async def handler(ctx):
        seen.append(ctx.id)

    async with make_worker(handler) as w:
        w.stop()
        assert w.status == "stopped"
        client.enqueue("t")
        await asyncio.sleep(0.3)
        assert seen == []


async def test_scheduled_tasks_are_forwarded(client, rdb, make_worker):
    seen = []

    async def handler(ctx):
        seen.append(ctx.id)

    info = client.enqueue("t", process_in=0.5)
    async with make_worker(handler):
        await wait_for(lambda: seen == [info.id], timeout=5)


async def test_heartbeat_publishes_server_and_workers(client, rdb, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    client.enqueue("t")
    async with make_worker(handler, shutdown_timeout=0.2) as w:
        await asyncio.wait_for(started.wait(), 3)
        key = f"asynq:servers:{{{w.engine.host}:{w.engine.pid}:{w.server_id}}}"
        await wait_for(lambda: rdb.exists(key) and rdb.hlen(key.replace("servers", "workers")) == 1)
        assert rdb.zscore("asynq:servers", key) is not None
    assert rdb.exists(key) == 0
    assert rdb.zcard("asynq:servers") == 0


async def test_queue_priority_strict(client, rdb, make_worker):
    order = []

    async def handler(ctx):
        order.append(ctx.queue)

    for _ in range(3):
        client.enqueue("t", queue="low")
        client.enqueue("t", queue="critical")
    async with make_worker(handler, concurrency=1, queues={"critical": 6, "low": 1}, strict_priority=True):
        await wait_for(lambda: len(order) == 6)
    assert order == ["critical"] * 3 + ["low"] * 3
