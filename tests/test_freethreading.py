"""Free-threaded CPython: pyasynq keeps the GIL off, and its objects hold up
when shared by threads that really run at the same time."""

import os
import pkgutil
import subprocess
import sys
import sysconfig
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pyasynq
import pytest
from conftest import wait_for
from pyasynq import Asynq
from pyasynq._core import Broker, TaskMessage

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="needs a free-threaded CPython build"
)


def test_importing_every_module_keeps_the_gil_disabled():
    modules = [m.name for m in pkgutil.walk_packages(pyasynq.__path__, "pyasynq.") if m.name != "pyasynq.__main__"]
    code = f"import importlib, sys\nfor m in {modules!r}: importlib.import_module(m)\nprint(sys._is_gil_enabled())"
    env = {k: v for k, v in os.environ.items() if k != "PYTHON_GIL"}
    # CPython warns when an extension without Py_MOD_GIL_NOT_USED turns the GIL on.
    out = subprocess.run(
        [sys.executable, "-W", "error", "-c", code], env=env, capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stderr
    assert "pyasynq._core" in modules
    assert out.stdout.strip() == "False"


def _spin(n: int) -> int:
    x = 0
    for i in range(n):
        x += i
    return x


async def test_sync_handlers_run_in_parallel(memory_url, run_app):
    threads = 4
    if (os.cpu_count() or 1) < threads:
        pytest.skip(f"needs {threads} CPUs")
    start = time.perf_counter()
    _spin(1_000_000)
    n = int(1_000_000 * 0.3 / (time.perf_counter() - start))
    start = time.perf_counter()
    _spin(n)
    alone = time.perf_counter() - start

    app = Asynq(memory_url)
    spans: list[tuple[float, float]] = []

    @app.task("spin")
    def spin() -> None:
        begin = time.perf_counter()
        _spin(n)
        spans.append((begin, time.perf_counter()))

    for _ in range(threads):
        await spin.aenqueue()
    async with run_app(app, concurrency=threads):
        await wait_for(lambda: len(spans) == threads, timeout=60)

    # With a GIL the four handlers take turns: about 4x `alone` end to end.
    elapsed = max(end for _, end in spans) - min(begin for begin, _ in spans)
    assert elapsed < 2 * alone, f"{threads} handlers took {elapsed:.2f}s, one alone {alone:.2f}s"


def test_client_and_broker_shared_by_threads(broker_url):
    threads, per_thread = 8, 50
    client = pyasynq.Client(broker_url)
    broker = Broker(broker_url)
    inspector = pyasynq.Inspector(broker_url)
    barrier = threading.Barrier(threads)

    def enqueue(i: int) -> list[str]:
        barrier.wait()
        return [client.enqueue("job", f"{i}:{j}".encode()).id for j in range(per_thread)]

    def drain(_: int) -> list[str]:
        barrier.wait()
        claimed = []
        while (got := broker.dequeue("default")) is not None:
            msg, _lease = got
            claimed.append(msg.id)
            inspector.get_queue_info("default")
            broker.done(msg)
        return claimed

    with ThreadPoolExecutor(threads) as pool:
        enqueued = [id for ids in pool.map(enqueue, range(threads)) for id in ids]
        claimed = [id for ids in pool.map(drain, range(threads)) for id in ids]

    assert len(set(enqueued)) == threads * per_thread
    assert sorted(claimed) == sorted(enqueued)
    info = inspector.get_queue_info("default")
    assert (info.pending, info.active) == (0, 0)


def test_task_message_shared_by_threads():
    msg = TaskMessage("t", b"x", headers={"k": "v"})
    other = TaskMessage("u")
    threads = 8
    barrier = threading.Barrier(threads)

    def hammer(i: int) -> None:
        barrier.wait()
        for n in range(20_000):
            msg.retried = n
            msg.error_msg = str(i)
            assert msg != other
            other.retried = n
            assert msg.headers == {"k": "v"}
            repr(msg)
            TaskMessage.decode(msg.encode())

    with ThreadPoolExecutor(threads) as pool:
        for f in [pool.submit(hammer, i) for i in range(threads)]:
            f.result()
    assert msg.retried == 19_999
    assert msg == msg  # noqa: PLR0124 (both sides share one lock)
    assert msg == TaskMessage.decode(msg.encode())
