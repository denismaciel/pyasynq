"""The in-process backend: stores shared by URL, wakeups, end-to-end use, and
its controllable clock."""

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import timedelta

import pyasynq
import pytest
from conftest import wait_for
from pyasynq import Asynq, testing
from pyasynq._core import Broker
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


async def test_retries_without_waiting(memory_url, run_app):
    app = Asynq(memory_url)
    attempts = []

    @app.task("flaky", max_retry=3)
    async def flaky() -> None:
        attempts.append(time.monotonic())
        if len(attempts) == 1:
            raise RuntimeError("boom")

    info = await flaky.aenqueue()
    async with run_app(app):
        await wait_for(lambda: app.inspector.get_task_info("default", info.id).state == "retry")
        await asyncio.sleep(0.2)
        assert len(attempts) == 1
        testing.advance_time(app, timedelta(hours=1))
        await wait_for(lambda: len(attempts) == 2, timeout=2)
    assert attempts[1] - attempts[0] < 2


async def test_scheduled_task_becomes_due(memory_url):
    app = Asynq(memory_url)
    ran = []

    @app.task("later")
    async def later() -> None:
        ran.append(True)

    info = await later.aenqueue(process_in=timedelta(hours=1))
    testing.advance_time(memory_url, 3599)
    assert app.inspector.get_task_info("default", info.id).state == "scheduled"
    testing.advance_time(memory_url, 1)
    assert app.inspector.get_task_info("default", info.id).state == "pending"
    await testing.drain(app)
    assert ran == [True]


async def test_retry_delay_counts_from_the_advanced_clock(memory_url, run_app):
    testing.advance_time(memory_url, timedelta(days=1))
    app = Asynq(memory_url)
    attempts = []

    @app.task("flaky", max_retry=3, retry_delay=lambda *_: 60)
    async def flaky() -> None:
        attempts.append(True)
        raise RuntimeError("boom")

    info = await flaky.aenqueue()
    async with run_app(app):
        await wait_for(lambda: len(attempts) == 1)
        await asyncio.sleep(0.3)
        assert app.inspector.get_task_info("default", info.id).state == "retry"
        testing.advance_time(app, 60)
        await wait_for(lambda: len(attempts) == 2, timeout=2)


async def test_running_task_survives_a_jump(memory_url, run_app):
    app = Asynq(memory_url)
    release = asyncio.Event()
    started = asyncio.Event()

    @app.task("slow", retention=3600)
    async def slow() -> None:
        started.set()
        await release.wait()

    info = await slow.aenqueue()
    async with run_app(app):
        await asyncio.wait_for(started.wait(), 2)
        testing.advance_time(app, timedelta(days=1))
        await asyncio.sleep(0.3)
        release.set()
        await wait_for(lambda: app.inspector.get_task_info("default", info.id).state == "completed")
    assert app.inspector.get_task_info("default", info.id).retried == 0


def test_retention_and_uniqueness_expire(memory_url):
    client = pyasynq.Client(memory_url)
    inspector = Inspector(memory_url)
    client.enqueue("u", b"x", unique=60)
    with pytest.raises(pyasynq.DuplicateTaskError):
        client.enqueue("u", b"x", unique=60)
    testing.advance_time(memory_url, 61)
    client.enqueue("u", b"x", unique=60)

    info = client.enqueue("done", retention=60)
    broker = Broker(memory_url)
    claimed = broker.dequeue("default")
    while claimed is not None and claimed[0].id != info.id:
        claimed = broker.dequeue("default")
    assert claimed is not None
    broker.mark_as_complete(claimed[0])
    testing.advance_time(memory_url, 59)
    assert inspector.get_task_info("default", info.id).state == "completed"
    testing.advance_time(memory_url, 2)
    with pytest.raises(pyasynq.TaskNotFoundError):
        inspector.get_task_info("default", info.id)


async def test_group_grace_period_follows_the_clock(memory_url, run_app):
    app = Asynq(memory_url)
    batches = []

    @app.task("notify")
    async def notify(user: str) -> None:
        raise AssertionError("grouped tasks are only processed in aggregate")

    @app.task("notify:batch")
    async def notify_batch(users: list[str]) -> None:
        batches.append(sorted(users))

    @app.group_aggregator
    def combine(group: str, tasks: list[pyasynq.Task]) -> pyasynq.Task:
        return notify_batch.task([json.loads(t.payload) for t in tasks])

    for user in ["ann", "bob"]:
        await notify.aenqueue(user, group="daily")
    async with run_app(app, group_grace_period=600):
        await asyncio.sleep(0.5)
        assert batches == []
        testing.advance_time(app, 601)
        await wait_for(lambda: batches == [["ann", "bob"]], timeout=5)


def test_advance_time_needs_a_memory_broker():
    with pytest.raises(ValueError, match="memory://"):
        testing.advance_time("redis://localhost:6379/0", 1)
