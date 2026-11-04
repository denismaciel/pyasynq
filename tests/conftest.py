import json
import shutil
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
import redis

ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_addr():
    """A throwaway redis-server for the whole test session."""
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server not installed")
    port = _free_port()
    proc = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    client = redis.Redis(port=port)
    for _ in range(50):
        try:
            client.ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    yield f"127.0.0.1:{port}"
    proc.terminate()
    proc.wait()


@pytest.fixture
def rdb(redis_addr):
    """Raw redis-py client on a freshly flushed database."""
    host, port = redis_addr.split(":")
    client = redis.Redis(host=host, port=int(port))
    client.flushall()
    yield client
    client.close()


@pytest.fixture(scope="session")
def postgres_dsn(tmp_path_factory):
    """A throwaway postgres server for the whole test session."""
    if shutil.which("initdb") is None:
        pytest.skip("postgres not installed")
    import psycopg

    root = tmp_path_factory.mktemp("pg")
    data = root / "data"
    subprocess.run(
        ["initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    port = _free_port()
    proc = subprocess.Popen(
        [
            "postgres",
            "-D",
            str(data),
            "-p",
            str(port),
            "-k",
            str(root),
            "-c",
            "listen_addresses=127.0.0.1",
            "-c",
            "max_connections=500",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    dsn = f"postgresql://postgres@127.0.0.1:{port}/postgres"
    for _ in range(100):
        try:
            psycopg.connect(dsn).close()
            break
        except psycopg.OperationalError:
            time.sleep(0.05)
    yield dsn
    # SIGTERM waits for clients to disconnect; SIGINT closes them.
    proc.send_signal(signal.SIGINT)
    proc.wait()


@pytest.fixture
def pg_url(postgres_dsn):
    """A postgres URL with an empty schema; pyasynq creates its tables on connect."""
    import psycopg

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
    return postgres_dsn


@pytest.fixture(params=["redis", "postgres"])
def broker_url(request):
    """An empty broker of each backend."""
    if request.param == "redis":
        request.getfixturevalue("rdb")
        return f"redis://{request.getfixturevalue('redis_addr')}/0"
    return request.getfixturevalue("pg_url")


@pytest.fixture
def client(rdb, redis_addr):
    import pyasynq

    return pyasynq.Client(f"redis://{redis_addr}/0")


FAST: dict[str, Any] = {
    "task_check_interval": 0.02,
    "delayed_task_check_interval": 0.1,
    "heartbeat_interval": 0.2,
    "install_signal_handlers": False,
    "shutdown_timeout": 2,
}


async def wait_for(predicate, timeout=5.0, interval=0.02):
    import asyncio

    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if loop.time() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


@pytest.fixture
def make_worker(redis_addr, rdb):
    """Builds a Worker with fast intervals and runs it as a background task."""
    import asyncio
    import contextlib

    import pyasynq

    @contextlib.asynccontextmanager
    async def run(handler, **options):
        worker = pyasynq.Worker(handler, redis_url=f"redis://{redis_addr}/0", **{**FAST, **options})
        task = asyncio.create_task(worker.run())
        await wait_for(lambda: worker.status == "active" or task.done())
        if task.done():
            task.result()
        try:
            yield worker
        finally:
            worker.shutdown()
            await task

    return run


@pytest.fixture
def run_app():
    """Runs `app.worker(...)` with fast intervals in the background."""
    import asyncio
    import contextlib

    @contextlib.asynccontextmanager
    async def run(app, **options):
        worker = app.worker(**{**FAST, **options})
        task = asyncio.create_task(worker.run())
        await wait_for(lambda: worker.status == "active" or task.done())
        if task.done():
            task.result()
        try:
            yield worker
        finally:
            worker.shutdown()
            await task

    return run


class GoAsynq:
    """Runs the Go helper in interop/go against the test redis."""

    def __init__(self, binary: Path, addr: str):
        self.binary = binary
        self.addr = addr

    def __call__(self, command: str, **flags: str) -> dict:
        args = [str(self.binary), command, "-addr", self.addr]
        for name, value in flags.items():
            args += [f"-{name}", value]
        out = subprocess.run(args, check=True, capture_output=True, text=True)
        return json.loads(out.stdout)


@pytest.fixture(scope="session")
def go_asynq(redis_addr, tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("go not installed")
    binary = tmp_path_factory.mktemp("go") / "interop"
    subprocess.run(
        ["go", "build", "-o", str(binary), "."],
        cwd=ROOT / "interop" / "go",
        check=True,
    )
    return GoAsynq(binary, redis_addr)
