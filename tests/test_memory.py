"""The in-process backend: stores shared by URL, wakeups, end-to-end use."""

import asyncio
import time
from dataclasses import dataclass

import pyasynq
from conftest import wait_for
from pyasynq import Asynq, testing
from pyasynq.inspector import Inspector


@dataclass
class Signup:
    email: str


def test_handles_with_the_same_url_share_a_store(memory_url):
    info = pyasynq.Client(memory_url).enqueue("t")
    assert Inspector(memory_url).get_task_info("default", info.id).state == "pending"
    assert Inspector(testing.memory_url()).queues() == []


def test_reset_empties_the_store(memory_url):
    pyasynq.Client(memory_url).enqueue("t")
    testing.reset_memory(memory_url)
    assert Inspector(memory_url).queues() == []


def test_named_urls_are_stable():
    url = testing.memory_url("named")
    try:
        pyasynq.Client(url).enqueue("t")
        assert Inspector(testing.memory_url("named")).queues() == ["default"]
    finally:
        testing.reset_memory(url)


async def test_enqueue_wakes_idle_worker(memory_url, run_app):
    app = Asynq(memory_url)
    done = asyncio.Event()

    @app.task("ping")
    async def ping() -> None:
        done.set()

    async with run_app(app, task_check_interval=30):
        await asyncio.sleep(0.1)
        start = time.monotonic()
        await ping.aenqueue()
        await asyncio.wait_for(done.wait(), 2)
    assert time.monotonic() - start < 1


async def test_drain_runs_tasks_without_a_broker():
    app = Asynq(testing.memory_url())
    sent: list[str] = []

    @app.task("email:welcome", retention=60)
    async def welcome(s: Signup) -> str:
        sent.append(s.email)
        return f"sent to {s.email}"

    info = await welcome.aenqueue(Signup(email="a@example.com"))
    await testing.drain(app)
    assert sent == ["a@example.com"]
    assert await welcome.wait_result(info, timeout=1) == "sent to a@example.com"


async def test_unpause_wakes_worker(memory_url, run_app):
    app = Asynq(memory_url)
    done = asyncio.Event()

    @app.task("ping")
    async def ping() -> None:
        done.set()

    app.inspector.pause_queue("default")
    await ping.aenqueue()
    async with run_app(app, task_check_interval=30):
        await asyncio.sleep(0.2)
        assert not done.is_set()
        app.inspector.unpause_queue("default")
        await asyncio.wait_for(done.wait(), 2)


async def test_many_workers_process_each_task_once(memory_url, run_app):
    n = 500
    counts: dict[int, int] = {}
    apps = [Asynq(memory_url) for _ in range(3)]
    for a in apps:

        @a.task("job")
        async def job(i: int) -> None:
            counts[i] = counts.get(i, 0) + 1

    client = pyasynq.Client(memory_url)
    for i in range(n):
        await client.aenqueue("job", str(i).encode(), headers={"content-type": "application/json"})
    async with run_app(apps[0], concurrency=10), run_app(apps[1], concurrency=10), run_app(apps[2], concurrency=10):
        await wait_for(lambda: len(counts) == n, timeout=10)
    assert set(counts.values()) == {1}
    assert Inspector(memory_url).get_queue_info("default").processed == n
