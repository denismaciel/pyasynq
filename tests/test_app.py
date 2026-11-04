"""The Asynq app API: typed tasks, middleware, lifespan, results, groups."""

import asyncio
import contextlib
import json
import threading
from dataclasses import dataclass, field

import pyasynq
import pydantic
import pytest
from conftest import FAST, wait_for
from pyasynq import Asynq, TaskContext


@dataclass
class Address:
    city: str


@dataclass
class Signup:
    email: str
    tags: list[str] = field(default_factory=list)
    address: Address | None = None


class Invoice(pydantic.BaseModel):
    number: int
    amount: float


@pytest.fixture
def app(redis_addr, rdb):
    return Asynq(f"redis://{redis_addr}/0")


def task_hash(rdb, info):
    return rdb.hgetall(f"asynq:{{{info.queue}}}:t:{info.id}")


async def test_typed_payloads_and_results(app, run_app):
    @app.task("user:signup", retention=60)
    async def signup(s: Signup, ctx: TaskContext) -> dict:
        assert s.address is not None
        return {"email": s.email, "city": s.address.city, "tags": s.tags, "retry": ctx.retry_count}

    @app.task("invoice:render", retention=60)
    def render(inv: Invoice) -> Invoice:
        assert threading.current_thread().name.startswith("pyasynq-task")
        return Invoice(number=inv.number, amount=inv.amount * 2)

    @app.task(retention=60)
    async def ping() -> bytes:
        return b"pong"

    a = await signup.aenqueue(Signup("a@b.c", ["x"], Address("Berlin")))
    b = render.enqueue(Invoice(number=7, amount=1.5))
    c = await ping.aenqueue()
    assert a.headers == {"content-type": "application/json"}
    assert json.loads(a.payload) == {"email": "a@b.c", "tags": ["x"], "address": {"city": "Berlin"}}
    assert ping.name == "test_app.test_typed_payloads_and_results.<locals>.ping"
    async with run_app(app):
        assert await signup.wait_result(a, timeout=5) == {
            "email": "a@b.c",
            "city": "Berlin",
            "tags": ["x"],
            "retry": 0,
        }
        assert await app.wait_result(b, timeout=5) == Invoice(number=7, amount=3.0)
        assert await ping.wait_result(c, timeout=5) == b"pong"


async def test_direct_call_still_works(app):
    @app.task
    def add(n: int) -> int:
        return n + 1

    assert add(1) == 2


async def test_bad_payload_is_archived(app, run_app, client, rdb):
    @app.task("typed")
    async def typed(s: Signup) -> None:
        pass

    info = client.enqueue("typed", b"not json")
    async with run_app(app):
        await wait_for(lambda: task_hash(rdb, info).get(b"state") == b"archived")
    msg = pyasynq.TaskMessage.decode(task_hash(rdb, info)[b"msg"])
    assert msg.error_msg.startswith("SkipRetry: cannot decode payload")


async def test_unknown_task_type_is_retried(app, run_app, client, rdb):
    info = client.enqueue("nobody:handles")
    async with run_app(app):
        await wait_for(lambda: task_hash(rdb, info).get(b"state") == b"retry")
    msg = pyasynq.TaskMessage.decode(task_hash(rdb, info)[b"msg"])
    assert msg.error_msg == "HandlerNotFound: handler not found for task 'nobody:handles'"


async def test_wait_result_raises_when_archived(app, run_app):
    @app.task(max_retry=0, retention=60)
    async def fails() -> None:
        raise RuntimeError("no luck")

    info = await fails.aenqueue()
    async with run_app(app):
        with pytest.raises(pyasynq.TaskFailed, match="RuntimeError: no luck"):
            await app.wait_result(info, timeout=5)


async def test_middleware_lifespan_and_error_handlers(redis_addr, rdb):
    events = []

    @contextlib.asynccontextmanager
    async def lifespan(app):
        events.append("startup")
        try:
            yield
        finally:
            events.append("shutdown")

    app = Asynq(f"redis://{redis_addr}/0", lifespan=lifespan)

    @app.middleware
    async def outer(ctx, call_next):
        events.append(("outer", ctx.type))
        return await call_next(ctx)

    @app.middleware
    async def inner(ctx, call_next):
        events.append("inner")
        try:
            return await call_next(ctx)
        finally:
            events.append("inner-after")

    @app.on_error
    def record(ctx, exc):
        events.append(("error", type(exc).__name__))

    @app.task("boom", retry_delay=lambda n, exc, ctx: 500)
    async def boom() -> None:
        events.append("handler")
        raise ValueError()

    info = await boom.aenqueue()
    run = asyncio.create_task(app.arun_worker(**FAST))
    await wait_for(lambda: ("error", "ValueError") in events)
    await wait_for(lambda: task_hash(rdb, info).get(b"state") == b"retry")
    run.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run
    assert events == [
        "startup",
        ("outer", "boom"),
        "inner",
        "handler",
        "inner-after",
        ("error", "ValueError"),
        "shutdown",
    ]
    import time

    assert abs(rdb.zscore("asynq:{default}:retry", info.id) - (time.time() + 500)) < 3


async def test_group_aggregation(app, run_app, rdb):
    batches = []

    @app.task("notify")
    async def notify(user: str) -> None:
        raise AssertionError("grouped tasks are only processed in aggregate")

    @app.task("notify:batch")
    async def notify_batch(users: list[str], ctx: TaskContext) -> None:
        batches.append((ctx.queue, sorted(users)))

    @app.group_aggregator
    def combine(group: str, tasks: list[pyasynq.Task]) -> pyasynq.Task:
        users = [json.loads(t.payload) for t in tasks]
        return notify_batch.task(users)

    for user in ["ann", "bob", "cid"]:
        info = await notify.aenqueue(user, group="daily")
        assert info.state == "aggregating"
    async with run_app(app, group_grace_period=1, group_max_size=10):
        await wait_for(lambda: batches, timeout=10)
    assert batches == [("default", ["ann", "bob", "cid"])]
    assert rdb.keys("asynq:{default}:g:*") == []


async def test_group_max_size_triggers_early(app, run_app):
    batches = []

    @app.task("item")
    async def item(n: int) -> None:
        pass

    @app.task("items")
    async def items(ns: list[int]) -> None:
        batches.append(sorted(ns))

    @app.group_aggregator
    def combine(group, tasks):
        return items.task([json.loads(t.payload) for t in tasks])

    for n in range(4):
        await item.aenqueue(n, group="g")
    async with run_app(app, group_grace_period=60, group_max_size=2):
        await wait_for(lambda: len(batches) == 2, timeout=10)
    assert [len(b) for b in batches] == [2, 2]
    assert sorted(n for b in batches for n in b) == [0, 1, 2, 3]


async def test_go_enqueued_json_reaches_typed_handler(app, run_app, go_asynq):
    got = []

    @dataclass
    class Email:
        to: str

    @app.task("email:send")
    async def send(e: Email) -> None:
        got.append(e)

    go_asynq("enqueue", queue="default", type="email:send", payload='{"to":"denis@example.com"}')
    async with run_app(app):
        await wait_for(lambda: got)
    assert got == [Email("denis@example.com")]


def test_rejects_bad_task_options(app):
    with pytest.raises(TypeError, match="unknown task options"):
        app.task(qeueu="typo")
