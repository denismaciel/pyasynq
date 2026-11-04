"""Backend conformance: the public API behaves the same on Redis and Postgres."""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pyasynq
import pytest
from conftest import wait_for
from pyasynq import Asynq, TaskContext
from pyasynq.inspector import Inspector
from pyasynq.rate import RateLimited, Semaphore


@pytest.fixture
def client(broker_url):
    return pyasynq.Client(broker_url)


@pytest.fixture
def inspector(broker_url):
    return Inspector(broker_url)


@pytest.fixture
def app(broker_url):
    return Asynq(broker_url)


def test_enqueue_states_and_listing(client, inspector):
    p1 = client.enqueue("a", b"1")
    p2 = client.enqueue("a", b"2", queue="default")
    s = client.enqueue("b", process_in=timedelta(hours=1))
    g = client.enqueue("c", group="g1")
    assert (p1.state, s.state, g.state) == ("pending", "scheduled", "aggregating")

    assert inspector.queues() == ["default"]
    q = inspector.get_queue_info("default")
    assert (q.pending, q.scheduled, q.aggregating, q.groups, q.size, q.paused) == (2, 1, 1, 1, 4, False)
    assert q.memory_usage > 0 and q.latency >= timedelta(0)

    assert [t.id for t in inspector.list_pending_tasks("default")] == [p1.id, p2.id]
    assert [t.id for t in inspector.list_tasks("default", "pending", page=2, size=1)] == [p2.id]
    [sched] = inspector.list_scheduled_tasks("default")
    assert sched.next_process_at == s.next_process_at.replace(microsecond=0)
    assert [t.id for t in inspector.list_aggregating_tasks("default", "g1")] == [g.id]
    assert inspector.groups("default") == {"g1": 1}

    info = inspector.get_task_info("default", p2.id)
    assert (info.type, info.payload, info.state) == ("a", b"2", "pending")
    with pytest.raises(pyasynq.TaskNotFoundError):
        inspector.get_task_info("default", "missing")
    with pytest.raises(pyasynq.QueueNotFoundError):
        inspector.get_task_info("nope", p1.id)


def test_conflicts_and_uniqueness(client):
    client.enqueue("a", task_id="x")
    with pytest.raises(pyasynq.TaskIDConflictError):
        client.enqueue("a", task_id="x")
    client.enqueue("a", task_id="x", queue="other")  # IDs are unique per queue

    client.enqueue("u", b"same", unique=60)
    with pytest.raises(pyasynq.DuplicateTaskError):
        client.enqueue("u", b"same", unique=60)
    with pytest.raises(pyasynq.DuplicateTaskError):
        client.enqueue("u", b"same", unique=60, process_in=10)
    client.enqueue("u", b"other", unique=60)
    # An ID conflict wins over the uniqueness lock, and doesn't steal it.
    with pytest.raises(pyasynq.TaskIDConflictError):
        client.enqueue("u", b"third", unique=60, task_id="x")
    client.enqueue("u", b"third", unique=60)


def test_task_operations(client, inspector):
    t = client.enqueue("a", b"old", process_in=3600)
    inspector.update_task_payload("default", t.id, b"new")
    assert inspector.get_task_info("default", t.id).payload == b"new"
    inspector.run_task("default", t.id)
    assert inspector.get_task_info("default", t.id).state == "pending"
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.run_task("default", t.id)
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.update_task_payload("default", t.id, b"x")
    inspector.archive_task("default", t.id)
    assert inspector.get_task_info("default", t.id).state == "archived"
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.archive_task("default", t.id)
    inspector.delete_task("default", t.id)
    with pytest.raises(pyasynq.TaskNotFoundError):
        inspector.delete_task("default", t.id)


def test_delete_releases_unique_lock(client, inspector):
    t = client.enqueue("u", b"p", unique=3600)
    inspector.delete_task("default", t.id)
    client.enqueue("u", b"p", unique=3600)


def test_bulk_operations(client, inspector):
    for _ in range(3):
        client.enqueue("a", process_in=3600)
    for _ in range(2):
        client.enqueue("g", group="grp")
    assert inspector.archive_all_tasks("default", "scheduled") == 3
    assert inspector.get_queue_info("default").archived == 3
    assert inspector.run_all_tasks("default", "archived") == 3
    assert inspector.run_all_tasks("default", "aggregating", group="grp") == 2
    assert inspector.get_queue_info("default").pending == 5
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.run_all_tasks("default", "aggregating")
    assert inspector.delete_all_tasks("default", "pending") == 5
    assert inspector.get_queue_info("default").size == 0
    inspector.delete_queue("default")
    assert inspector.queues() == []


def test_delete_queue(client, inspector):
    client.enqueue("a")
    client.enqueue("b", group="g")
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.delete_queue("default")
    inspector.delete_queue("default", force=True)
    assert inspector.queues() == []
    with pytest.raises(pyasynq.QueueNotFoundError):
        inspector.get_queue_info("default")
    client.enqueue("a")
    assert inspector.get_queue_info("default").size == 1


def test_pause(client, inspector):
    client.enqueue("a")
    inspector.pause_queue("default")
    assert inspector.get_queue_info("default").paused
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.pause_queue("default")
    inspector.unpause_queue("default")
    with pytest.raises(pyasynq.FailedPreconditionError):
        inspector.unpause_queue("default")


async def test_results_retries_and_stats(app, run_app, inspector):
    attempts = []

    @app.task("ok", retention=60)
    async def ok(n: int) -> int:
        return n * 2

    @app.task("flaky", max_retry=1, retry_delay=lambda n, exc, task: 0.1)
    async def flaky() -> None:
        attempts.append(1)
        raise RuntimeError("boom")

    info = await ok.aenqueue(21)
    bad = await flaky.aenqueue()
    async with run_app(app):
        assert await app.wait_result(info, timeout=10) == 42
        dead = await wait_for(
            lambda: (t := inspector.get_task_info("default", bad.id)).state == "archived" and t,
            timeout=10,
        )
    assert len(attempts) == 2
    assert (dead.retried, dead.last_err) == (1, "RuntimeError: boom")
    done = inspector.get_task_info("default", info.id)
    assert done.state == "completed" and done.completed_at is not None
    assert [t.id for t in inspector.list_completed_tasks("default")] == [info.id]

    q = inspector.get_queue_info("default")
    assert (q.processed, q.failed, q.processed_total, q.failed_total) == (3, 2, 3, 2)
    [today] = inspector.history("default", days=1)
    assert (today.processed, today.failed) == (3, 2)
    assert len(inspector.history("default", days=3)) == 3


async def test_scheduled_and_unique_release(app, run_app, client):
    seen = []

    @app.task("later")
    async def later(n: int) -> None:
        seen.append(n)

    await later.aenqueue(1, process_in=1, unique=3600)
    await later.aenqueue(2)
    async with run_app(app):
        await wait_for(lambda: sorted(seen) == [1, 2], timeout=10)
        await asyncio.sleep(0.2)
    # Success releases the uniqueness lock.
    await later.aenqueue(1, unique=3600)


async def test_group_aggregation(app, run_app, inspector):
    batches = []

    @app.task("notify")
    async def notify(user: str) -> None:
        raise AssertionError("grouped tasks are only processed in aggregate")

    @app.task("notify:batch")
    async def notify_batch(users: list[str], ctx: TaskContext) -> None:
        batches.append(sorted(users))

    @app.group_aggregator
    def combine(group: str, tasks: list[pyasynq.Task]) -> pyasynq.Task:
        return notify_batch.task([json.loads(t.payload) for t in tasks])

    for user in ["ann", "bob", "cid", "dan", "eve"]:
        await notify.aenqueue(user, group="daily")
    async with run_app(app, group_grace_period=1, group_max_size=2):
        await wait_for(lambda: len(batches) == 3, timeout=10)
    assert sorted(len(b) for b in batches) == [1, 2, 2]
    assert inspector.groups("default") == {}
    assert inspector.get_queue_info("default").size == 0


async def test_cancel_processing(app, run_app, inspector):
    started = asyncio.Event()

    @app.task("slow", max_retry=0)
    async def slow(ctx: TaskContext) -> None:
        started.set()
        await asyncio.sleep(30)

    info = await slow.aenqueue()
    async with run_app(app):
        await asyncio.wait_for(started.wait(), 5)
        [server] = await wait_for(lambda: [s for s in inspector.servers() if s.active_workers], timeout=5)
        assert [w.task_id for w in server.active_workers] == [info.id]
        assert [t.id for t in inspector.list_active_tasks("default")] == [info.id]
        await asyncio.sleep(0.3)  # the subscriber must be listening
        inspector.cancel_processing(info.id)
        dead = await wait_for(
            lambda: (t := inspector.get_task_info("default", info.id)).state == "archived" and t,
            timeout=5,
        )
    assert dead.last_err == "context canceled"
    assert inspector.servers() == []


async def test_scheduler(app, inspector):
    @app.periodic("@every 1s", {"n": 1}, queue="cron")
    @app.task("cron:job")
    async def job(p: dict) -> None:
        pass

    sched = app.scheduler(heartbeat_interval=0.2, install_signal_handlers=False)
    run = asyncio.create_task(sched.run())
    [entry] = await wait_for(lambda: inspector.scheduler_entries(), timeout=5)
    assert (entry.spec, entry.task_type) == ("@every 1s", "cron:job")
    await wait_for(lambda: inspector.list_scheduler_enqueue_events(entry.id), timeout=5)
    [task] = await wait_for(lambda: inspector.list_pending_tasks("cron")[:1], timeout=5)
    assert (task.type, task.payload) == ("cron:job", b'{"n":1}')
    sched.shutdown()
    await run
    assert inspector.scheduler_entries() == []
    assert inspector.list_scheduler_enqueue_events(entry.id) == []


async def test_semaphore(broker_url):
    from pyasynq.testing import make_context

    sema = Semaphore(broker_url, "api", max_tokens=1)
    a, b = make_context(), make_context()
    async with sema.hold(a):
        with pytest.raises(RateLimited):
            async with sema.hold(b):
                pass
    async with sema.hold(b):
        pass
    assert not sema.release_sync(a.id)


async def test_janitor_deletes_expired_completed(app, run_app, inspector):
    @app.task("short", retention=1)
    async def short() -> None:
        pass

    info = await short.aenqueue()
    async with run_app(app, janitor_interval=0.2):
        await wait_for(lambda: inspector.get_queue_info("default").completed == 1, timeout=5)
        await wait_for(lambda: inspector.get_queue_info("default").completed == 0, timeout=5)
    with pytest.raises(pyasynq.TaskNotFoundError):
        inspector.get_task_info("default", info.id)


async def test_deadline_in_the_past_is_archived(app, run_app, inspector):
    @app.task("late", max_retry=0)
    async def late() -> None:
        await asyncio.sleep(5)

    info = await late.aenqueue(deadline=datetime.now(timezone.utc) + timedelta(seconds=1))
    async with run_app(app):
        dead = await wait_for(
            lambda: (t := inspector.get_task_info("default", info.id)).state == "archived" and t,
            timeout=10,
        )
    assert dead.last_err == "context deadline exceeded"
