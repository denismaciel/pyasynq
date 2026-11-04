"""Helpers for testing code that uses pyasynq.

`run_task` executes a task in-process (no Redis) through the app's
middleware, exactly as a worker would. `drain` runs a real worker until
the queues are empty, for integration tests; with a `memory_url()` broker
those need no Redis or Postgres either.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pyasynq import _core
from pyasynq.app import Asynq, TaskDef
from pyasynq.context import TaskContext, _current

if TYPE_CHECKING:
    from pyasynq.worker import Worker

_P = TypeVar("_P")
_R = TypeVar("_R")


def memory_url(name: str | None = None) -> str:
    """A ``memory://`` broker URL: an in-process broker shared by every
    client, worker and inspector in this process that uses the same URL.
    Without `name` the broker is a fresh one."""
    return f"memory://{name or uuid.uuid4()}"


def reset_memory(url: str | None = None) -> None:
    """Empties the in-process broker behind `url`, or every one."""
    _core.memory_reset(url)


def advance_time(broker: str | Asynq, by: float | timedelta) -> None:
    """Moves the clock of an in-process broker (a ``memory://`` URL, or an
    app using one) forward by `by` seconds, as if that time had passed:
    scheduled and retry tasks that become due are made pending, and
    completed tasks and uniqueness locks that expire are dropped.

    Due times, retention, uniqueness TTLs and group timing follow this
    clock. Task deadlines, leases and cron schedules run on real time."""
    url = broker if isinstance(broker, str) else broker.redis_url
    if not url.startswith("memory://"):
        raise ValueError(f"advance_time needs a memory:// broker, not {url!r}")
    seconds = by.total_seconds() if isinstance(by, timedelta) else by
    _core.memory_advance(url, seconds)


class _InlineRunner:
    """Stands in for the Worker inside a TaskContext."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    async def run_in_thread(self, fn: Callable[..., Any], *args: Any) -> Any:
        return await asyncio.to_thread(fn, *args)


def make_context(
    task_type: str = "test",
    payload: bytes = b"",
    *,
    id: str | None = None,
    queue: str = "default",
    headers: dict[str, str] | None = None,
    retried: int = 0,
    max_retry: int = 25,
    timeout: timedelta = timedelta(minutes=30),
) -> TaskContext:
    """A TaskContext for calling handlers directly."""
    msg = _core.TaskMessage(
        task_type,
        payload,
        id=id or str(uuid.uuid4()),
        queue=queue,
        headers=headers or {},
        retried=retried,
        retry=max_retry,
    )
    try:
        runner = _InlineRunner(asyncio.get_running_loop())
    except RuntimeError:
        runner = None
    return TaskContext(msg, datetime.now(timezone.utc) + timeout, None, cast("Worker | None", runner))


async def run_task(task: TaskDef[_P, _R], payload: _P | None = None, **context: Any) -> _R:
    """Runs `task` with `payload` through its app's middleware and returns
    the decoded result. Exceptions propagate."""
    data, opts = task.message(payload)
    ctx = make_context(task.name, data, headers=opts.get("headers"), **context)
    _current.set(ctx)
    raw = await task.app.handle(ctx)
    result: _R = task.decode_result(raw)
    return result


async def drain(app: Asynq, *, queues: list[str] | None = None, timeout: float = 30, **worker_options: Any) -> None:
    """Processes tasks until every queue has nothing pending or active,
    then shuts the worker down. Scheduled/retry tasks are not waited for."""
    queues = queues or app.inspector.queues() or ["default"]
    options = {
        "queues": queues,
        "task_check_interval": 0.02,
        "heartbeat_interval": 0.5,
        "install_signal_handlers": False,
        **worker_options,
    }
    worker = app.worker(**options)
    run = asyncio.create_task(worker.run())

    def idle() -> bool:
        for q in queues:
            try:
                info = app.inspector.get_queue_info(q)
            except _core.QueueNotFoundError:
                continue
            if info.pending or info.active:
                return False
        return worker.active_count == 0

    try:
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while not (worker.status == "active" and idle()):
            if run.done():
                run.result()
            if loop.time() > end:
                raise TimeoutError("queues not drained in time")
            await asyncio.sleep(0.05)
    finally:
        worker.shutdown()
        await run
