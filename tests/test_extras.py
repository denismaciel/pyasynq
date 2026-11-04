"""Rate limiting, metrics, tracing, testing helpers, CLI and prefork."""

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import time

import pyasynq
import pytest
from conftest import ROOT, wait_for
from pyasynq import Asynq, RateLimited, TaskContext
from pyasynq.rate import Semaphore


@pytest.fixture
def url(redis_addr, rdb):
    return f"redis://{redis_addr}/0"


@pytest.fixture
def app(url):
    return Asynq(url)


async def test_semaphore_limits_across_holders(url, rdb):
    from pyasynq.testing import make_context

    sema = Semaphore(url, "api", max_tokens=1)
    a, b = make_context(), make_context()
    async with sema.hold(a):
        with pytest.raises(RateLimited):
            async with sema.hold(b):
                pass
        assert rdb.zcard("asynq:sema:api") == 1
    assert rdb.zcard("asynq:sema:api") == 0
    with sema.hold_sync(b):
        assert not sema.acquire_sync(a.id, a.deadline)


async def test_rate_limited_task_retries_without_failure(app, run_app, url):
    sema = Semaphore(url, "slow", max_tokens=1)
    release = asyncio.Event()
    started = asyncio.Event()

    @app.task("limited", retention=60)
    async def limited(n: int, ctx: TaskContext) -> int:
        async with sema.hold(ctx, retry_after=30):
            started.set()
            await release.wait()
            return n

    first = await limited.aenqueue(1)
    async with run_app(app, concurrency=2):
        await asyncio.wait_for(started.wait(), 5)
        second = await limited.aenqueue(2)
        info = await wait_for(lambda: (i := app.inspector.get_task_info("default", second.id)).state == "retry" and i)
        assert info.retried == 0
        assert info.last_err.startswith("RateLimited:")
        assert app.inspector.get_queue_info("default").failed == 0
        release.set()
        assert await limited.wait_result(first, timeout=5) == 1


async def test_prometheus_collector(app, client):
    from prometheus_client import CollectorRegistry, generate_latest
    from pyasynq.metrics import QueueMetricsCollector

    client.enqueue("a", b"", queue="q1")
    client.enqueue("b", b"", queue="q1", process_in=60)
    app.inspector.pause_queue("q1")
    registry = CollectorRegistry()
    registry.register(QueueMetricsCollector(app.inspector))
    text = generate_latest(registry).decode()
    assert 'asynq_tasks_enqueued_total{queue="q1",state="pending"} 1.0' in text
    assert 'asynq_tasks_enqueued_total{queue="q1",state="scheduled"} 1.0' in text
    assert 'asynq_queue_size{queue="q1"} 2.0' in text
    assert 'asynq_queue_paused_total{queue="q1"} 1.0' in text
    assert 'asynq_tasks_processed_total{queue="q1"} 0.0' in text


async def test_otel_trace_continues_in_worker(app, run_app):
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from pyasynq.otel import instrument

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instrument(app, tracer_provider=provider)

    @app.task("traced", retention=60)
    async def traced() -> str:
        return "ok"

    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("request") as parent:
        info = await traced.aenqueue()
    assert "traceparent" in info.headers
    async with run_app(app):
        assert await traced.wait_result(info, timeout=5) == "ok"
    spans = {s.name: s for s in exporter.get_finished_spans()}
    child = spans["process traced"]
    assert child.parent is not None
    assert child.attributes is not None
    assert child.context.trace_id == parent.get_span_context().trace_id
    assert child.parent.span_id == parent.get_span_context().span_id
    assert child.attributes["messaging.message.id"] == info.id


async def test_run_task_goes_through_middleware():
    from pyasynq.testing import run_task

    app = Asynq()
    seen = []

    @app.middleware
    async def record(ctx, call_next):
        seen.append(ctx.type)
        return await call_next(ctx)

    @app.task("double")
    def double(n: int, ctx: TaskContext) -> int:
        return n * 2 + ctx.retry_count

    assert await run_task(double, 4, retried=1) == 9
    assert seen == ["double"]


async def test_drain(app):
    from pyasynq.testing import drain

    done = []

    @app.task("work", queue="jobs")
    async def work(n: int) -> None:
        done.append(n)

    for n in range(5):
        await work.aenqueue(n)
    await drain(app, queues=["jobs"])
    assert sorted(done) == list(range(5))


def _write_app(tmp_path, url, body):
    (tmp_path / "myapp.py").write_text(
        textwrap.dedent(
            f"""
            import os
            from pyasynq import Asynq
            app = Asynq({url!r})
            """
        )
        + textwrap.dedent(body)
    )


def _cli(tmp_path, *args, **kw):
    env = {**os.environ, "PYTHONPATH": str(ROOT / "python")}
    return subprocess.Popen(
        [sys.executable, "-m", "pyasynq", *args],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **kw,
    )


def test_cli_inspect(tmp_path, url, client):
    client.enqueue("x", b"", queue="cliq")
    out, err = _cli(tmp_path, "inspect", "queue", "cliq", "--redis", url).communicate(timeout=30)
    assert '"pending": 1' in out, err
    out, _ = _cli(tmp_path, "inspect", "tasks", "cliq", "pending", "--redis", url).communicate(timeout=30)
    assert '"type": "x"' in out


@pytest.mark.parametrize("processes", [1, 3])
def test_cli_worker(tmp_path, url, rdb, processes):
    _write_app(
        tmp_path,
        url,
        """
        @app.task("touch")
        def touch(name: str) -> None:
            open(name, "w").write(str(os.getpid()))
        """,
    )
    proc = _cli(
        tmp_path,
        "worker",
        "myapp:app",
        "-q",
        "default=1",
        "-c",
        "2",
        "--processes",
        str(processes),
        "--shutdown-timeout",
        "1",
    )
    try:
        client = pyasynq.Client(url)
        deadline = time.time() + 20
        while len(rdb.keys("asynq:servers:*")) < processes:
            assert time.time() < deadline, proc.stderr.read() if proc.poll() is not None else "no servers"
            time.sleep(0.1)
        for i in range(6):
            client.enqueue("touch", f'"{tmp_path / str(i)}"'.encode())
        while not all((tmp_path / str(i)).exists() for i in range(6)):
            assert time.time() < deadline, "tasks not processed"
            time.sleep(0.1)
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=20)
    assert proc.returncode == 0
    assert rdb.keys("asynq:servers:*") == []


def test_strip_option():
    from pyasynq.cli import strip_option

    assert strip_option(["w", "--processes", "3", "-c", "2"], "--processes") == ["w", "-c", "2"]
    assert strip_option(["w", "--processes=3"], "--processes") == ["w"]
