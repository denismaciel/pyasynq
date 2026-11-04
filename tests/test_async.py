import asyncio
import threading
from datetime import timedelta

import pytest

from pyasynq import DuplicateTaskError
from pyasynq._core import pending_key


class Ticker:
    """Counts event loop iterations; it only advances when the loop is free."""

    def __init__(self):
        self.ticks = 0
        self._task = asyncio.ensure_future(self._run())

    async def _run(self):
        while True:
            await asyncio.sleep(0)
            self.ticks += 1

    def stop(self):
        self._task.cancel()


async def test_aenqueue_returns_task_info(client, go_asynq):
    info = await client.aenqueue("email:send", b"hi", queue="critical")
    assert info.state == "pending"
    assert go_asynq("inspect", queue="critical", id=info.id)["state"] == "pending"
    await client.aping()


async def test_errors_cross_the_bridge(client):
    await client.aenqueue("t", b"p", unique=timedelta(minutes=1))
    with pytest.raises(DuplicateTaskError):
        await client.aenqueue("t", b"p", unique=timedelta(minutes=1))
    with pytest.raises(ValueError):
        await client.aenqueue(" ")


async def test_concurrent_aenqueue_keeps_loop_responsive(client, rdb):
    ticker = Ticker()
    await asyncio.sleep(0)

    start = ticker.ticks
    infos = await asyncio.gather(
        *(client.aenqueue("bulk", str(i).encode()) for i in range(10_000))
    )
    ticker.stop()

    assert len({i.id for i in infos}) == 10_000
    assert rdb.llen(pending_key("default")) == 10_000
    assert ticker.ticks > start


async def test_sync_enqueue_blocks_loop(client):
    ticker = Ticker()
    await asyncio.sleep(0)

    start = ticker.ticks
    for i in range(200):
        client.enqueue("blocking", str(i).encode())
    blocked_ticks = ticker.ticks - start
    ticker.stop()

    assert blocked_ticks == 0


async def test_cancelled_aenqueue_raises_cancelled_error(client):
    loop = asyncio.get_running_loop()
    errors = []
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))

    fut = asyncio.ensure_future(client.aenqueue("t"))
    await asyncio.sleep(0)
    fut.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fut
    # Give the tokio side time to finish and try to deliver its result.
    await asyncio.sleep(0.2)
    assert errors == []


def test_raw_future_is_eager_but_wrapper_is_lazy(client):
    with pytest.raises(RuntimeError, match="no running event loop"):
        client._aenqueue("t")
    coro = client.aenqueue("t")  # nothing runs until awaited
    coro.close()


async def test_create_task_accepts_aenqueue(client):
    info = await asyncio.create_task(client.aenqueue("t"))
    assert info.state == "pending"


def test_client_works_across_event_loops(client, rdb):
    asyncio.run(client.aenqueue("a"))
    asyncio.run(client.aenqueue("b"))
    assert rdb.llen(pending_key("default")) == 2


def test_sync_enqueue_from_threads(client, rdb):
    def work():
        for _ in range(100):
            client.enqueue("threaded")

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert rdb.llen(pending_key("default")) == 800
