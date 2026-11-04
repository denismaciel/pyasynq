"""Crash recovery and cross-language behaviour of the worker."""

import asyncio
import os
import signal
import subprocess
import sys
import textwrap
import time

import pyasynq
from conftest import wait_for

CRASHY_WORKER = textwrap.dedent(
    """
    import asyncio, sys, pyasynq

    async def handler(ctx):
        print("started", flush=True)
        await asyncio.sleep(60)

    pyasynq.Worker(
        handler,
        redis_url=sys.argv[1],
        task_check_interval=0.02,
        heartbeat_interval=0.2,
        lease_duration=1,
        install_signal_handlers=False,
    ).run_forever()
    """
)


async def test_crashed_worker_task_is_recovered(client, rdb, redis_addr, make_worker):
    url = f"redis://{redis_addr}/0"
    info = client.enqueue("t", b"", max_retry=3)
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", CRASHY_WORKER, url, stdout=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        assert (await proc.stdout.readline()).strip() == b"started"
        assert rdb.lrange("asynq:{default}:active", 0, -1) == [info.id.encode()]
        os.kill(proc.pid, signal.SIGKILL)
    finally:
        await proc.wait()

    # Another worker's recoverer finds the expired lease and schedules a retry.
    async def never(ctx):
        raise AssertionError("not reached: the retry is in the future")

    async with make_worker(never, recoverer_interval=0.2, recoverer_skew=0):
        await wait_for(lambda: rdb.hget(f"asynq:{{default}}:t:{info.id}", "state") == b"retry", timeout=8)
    msg = pyasynq.TaskMessage.decode(rdb.hget(f"asynq:{{default}}:t:{info.id}", "msg"))
    assert msg.error_msg == "asynq: task lease expired"
    assert msg.retried == 1
    assert rdb.llen("asynq:{default}:active") == 0


async def test_sigterm_shuts_down_gracefully(client, rdb, redis_addr):
    script = CRASHY_WORKER.replace("install_signal_handlers=False,", "shutdown_timeout=0.3,")
    info = client.enqueue("t")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", script, f"redis://{redis_addr}/0", stdout=subprocess.PIPE
    )
    assert proc.stdout is not None
    assert (await proc.stdout.readline()).strip() == b"started"
    proc.send_signal(signal.SIGTERM)
    assert await asyncio.wait_for(proc.wait(), 10) == 0
    assert rdb.hget(f"asynq:{{default}}:t:{info.id}", "state") == b"pending"
    assert rdb.zcard("asynq:servers") == 0


async def test_go_inspector_sees_python_worker(client, go_asynq, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    info = client.enqueue("t", b"payload", queue="critical")
    async with make_worker(handler, queues={"critical": 6, "default": 1}, concurrency=3, shutdown_timeout=0.2) as w:
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.sleep(0.3)  # one heartbeat with the worker in it
        servers = await asyncio.to_thread(go_asynq, "servers")
    assert len(servers) == 1
    s = servers[0]
    assert s["ID"] == w.server_id
    assert s["Concurrency"] == 3
    assert s["Queues"] == {"critical": 6, "default": 1}
    assert s["Status"] == "active"
    assert [(a["TaskID"], a["Queue"]) for a in s["ActiveWorkers"]] == [(info.id, "critical")]


async def test_go_cancel_processing_reaches_python(client, rdb, go_asynq, make_worker):
    started = asyncio.Event()

    async def handler(ctx):
        started.set()
        await asyncio.sleep(30)

    info = client.enqueue("t")
    async with make_worker(handler):
        await asyncio.wait_for(started.wait(), 3)
        await asyncio.sleep(0.2)
        await asyncio.to_thread(go_asynq, "cancel", id=info.id)
        await wait_for(lambda: rdb.hget(f"asynq:{{default}}:t:{info.id}", "state") == b"retry")
    got = await asyncio.to_thread(go_asynq, "inspect", queue="default", id=info.id)
    assert got["last_err"] == "context canceled"


async def test_python_processes_go_tasks_and_go_processes_python_tasks(client, rdb, go_asynq, make_worker):
    seen = []

    async def handler(ctx):
        seen.append((ctx.id, ctx.type, ctx.payload, ctx.headers))
        if ctx.payload == b"fail":
            raise RuntimeError("python handler failed")

    go_ok = go_asynq("enqueue", queue="critical", type="email:send", payload="hello")
    go_fail = go_asynq("enqueue", queue="critical", type="email:send", payload="fail")
    async with make_worker(handler, queues=["critical"]):
        await wait_for(lambda: len(seen) == 2)
        await wait_for(lambda: rdb.llen("asynq:{critical}:active") == 0)
    assert (go_ok["id"], "email:send", b"hello", {"content-type": "application/json"}) in seen
    # Go set Retention(1h): completed, and the Go inspector agrees.
    ok = go_asynq("inspect", queue="critical", id=go_ok["id"])
    assert ok["state"] == "completed"
    failed = go_asynq("inspect", queue="critical", id=go_fail["id"])
    assert failed["state"] == "retry"
    assert failed["retried"] == 1
    assert failed["last_err"] == "RuntimeError: python handler failed"

    py_ok = client.enqueue("report", b"ok", queue="go", headers={"k": "v"})
    py_fail = client.enqueue("report", b"fail", queue="go")
    handled = await asyncio.to_thread(go_asynq, "worker", queue="go", duration="1s")
    assert {(h["id"], h["type"]) for h in handled} == {(py_ok.id, "report"), (py_fail.id, "report")}
    assert rdb.exists(f"asynq:{{go}}:t:{py_ok.id}") == 0
    msg = pyasynq.TaskMessage.decode(rdb.hget(f"asynq:{{go}}:t:{py_fail.id}", "msg"))
    assert (msg.retried, msg.error_msg) == (1, "go handler failed")
    assert time.time() < rdb.zscore("asynq:{go}:retry", py_fail.id)
