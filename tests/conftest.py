import json
import shutil
import socket
import subprocess
import time
from pathlib import Path

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


@pytest.fixture
def client(rdb, redis_addr):
    import pyasynq

    return pyasynq.Client(f"redis://{redis_addr}/0")


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
