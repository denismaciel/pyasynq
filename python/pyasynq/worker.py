"""The worker: runs handlers for tasks the Rust engine dequeues.

Division of labour: the Rust engine dequeues, holds leases, heartbeats,
enforces deadlines, retries/archives in Redis and shuts down gracefully.
This module turns its events into handler calls on an asyncio loop and
reports each outcome back with `engine.ack(...)`.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import logging
import signal
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Coroutine, Union, overload

from pyasynq import _core
from pyasynq.context import TaskContext, _current
from pyasynq.errors import (
    DeadlineExceeded,
    LeaseExpired,
    RedisError,
    Retry,
    RevokeTask,
    SkipRetry,
    TaskCanceled,
    error_message,
)
from pyasynq.task import Task

log = logging.getLogger("pyasynq.worker")

DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"

Seconds = Union[float, int, timedelta]
Handler = Callable[[TaskContext], Union[Awaitable[Any], Any]]
RetryDelayFunc = Callable[[int, BaseException, TaskContext], Seconds]
ErrorHandler = Callable[[TaskContext, BaseException], Union[Awaitable[None], None]]
HealthCheck = Callable[[Union[BaseException, None]], Union[Awaitable[None], None]]
GroupAggregator = Callable[[str, list[Task]], Union[Awaitable[Task], Task]]

_RUST_ERRORS: dict[str, type[BaseException]] = {
    "context deadline exceeded": DeadlineExceeded,
    "context canceled": TaskCanceled,
    "asynq: task lease expired": LeaseExpired,
}


@overload
def seconds(v: Seconds) -> float: ...
@overload
def seconds(v: None) -> None: ...
def seconds(v: Seconds | None) -> float | None:
    if v is None:
        return None
    return v.total_seconds() if isinstance(v, timedelta) else float(v)


def is_async_callable(fn: Any) -> bool:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(
        getattr(fn, "__call__", None)
    )


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def default_retry_delay(retried: int, exc: BaseException, ctx: TaskContext) -> float:
    """asynq's default: retried^4 + 15 + rand(30) * (retried + 1) seconds."""
    return _core.default_retry_delay_seconds(retried)


@dataclass
class _Job:
    exec: int
    ctx: TaskContext
    task: asyncio.Task[Any] | None
    finalized: bool = False


class Worker:
    """Processes tasks with `handler(ctx)`, which may be sync or async.

    The handler's return value is stored as the task result if it is
    bytes. Raise `SkipRetry`, `RevokeTask` or `Retry(after=...)` to steer
    what happens on failure; any other exception is retried with
    `retry_delay` until `max_retry` is exhausted, then archived.
    """

    def __init__(
        self,
        handler: Handler,
        *,
        redis_url: str = DEFAULT_REDIS_URL,
        concurrency: int | None = None,
        queues: dict[str, int] | list[str] | None = None,
        strict_priority: bool = False,
        shutdown_timeout: Seconds = 8,
        retry_delay: RetryDelayFunc | None = None,
        is_failure: Callable[[BaseException], bool] | None = None,
        error_handler: ErrorHandler | None = None,
        health_check: HealthCheck | None = None,
        health_check_interval: Seconds = 15,
        task_check_interval: Seconds = 1,
        delayed_task_check_interval: Seconds = 5,
        group_aggregator: GroupAggregator | None = None,
        group_grace_period: Seconds = 60,
        group_max_delay: Seconds = 0,
        group_max_size: int = 0,
        janitor_interval: Seconds = 8,
        janitor_batch_size: int = 100,
        thread_pool_size: int | None = None,
        install_signal_handlers: bool = True,
        **engine_options: Any,
    ):
        if isinstance(queues, (list, tuple)):
            queues = {q: 1 for q in queues}
        self._handler = handler
        self._handler_is_async = is_async_callable(handler)
        self._retry_delay = retry_delay or default_retry_delay
        self._is_failure = is_failure or (lambda exc: True)
        self._error_handler = error_handler
        self._health_check = health_check
        self._group_aggregator = group_aggregator
        self._install_signals = install_signal_handlers
        self._redis_url = redis_url
        opts: dict[str, Any] = {k: seconds(v) if isinstance(v, timedelta) else v for k, v in engine_options.items()}
        self._engine = _core.Engine(
            redis_url,
            concurrency=concurrency,
            queues=queues,
            strict_priority=strict_priority,
            shutdown_timeout=seconds(shutdown_timeout),
            health_check_interval=seconds(health_check_interval),
            task_check_interval=seconds(task_check_interval),
            delayed_task_check_interval=seconds(delayed_task_check_interval),
            group_aggregation=group_aggregator is not None,
            group_grace_period=seconds(group_grace_period),
            group_max_delay=seconds(group_max_delay),
            group_max_size=group_max_size,
            janitor_interval=seconds(janitor_interval),
            janitor_batch_size=janitor_batch_size,
            failure_policy=_weak_policy(self),
            **opts,
        )
        self._pool_size = thread_pool_size or self._engine.concurrency * 2
        self._pool: ThreadPoolExecutor | None = None
        self._pool_lock = threading.Lock()
        self._jobs: dict[int, _Job] = {}
        self._background: set[asyncio.Task[Any]] = set()
        self._client: Any = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._shutdown_requested: asyncio.Event | None = None

    # -- introspection -----------------------------------------------------

    @property
    def engine(self) -> _core.Engine:
        return self._engine

    @property
    def status(self) -> str:
        return self._engine.status

    @property
    def server_id(self) -> str:
        return self._engine.server_id

    @property
    def active_count(self) -> int:
        return self._engine.active_count

    # -- lifecycle -----------------------------------------------------------

    def run_forever(self) -> None:
        """Blocking entry point: `asyncio.run(worker.run())`."""
        asyncio.run(self.run())

    async def run(self) -> None:
        """Processes tasks until `shutdown()` or SIGTERM/SIGINT.

        SIGTSTP stops pulling new tasks (like asynq's `Server.Stop`) while
        the in-flight ones finish.
        """
        self.loop = asyncio.get_running_loop()
        self._shutdown_requested = asyncio.Event()
        await self._engine._astart()
        installed = self._add_signal_handlers()
        pump = self.loop.create_task(self._pump(), name="pyasynq-pump")
        try:
            await self._shutdown_requested.wait()
        finally:
            await self._engine._ashutdown()
            await pump
            if self._background:
                await asyncio.gather(*self._background, return_exceptions=True)
            for sig in installed:
                self.loop.remove_signal_handler(sig)
            if self._pool is not None:
                self._pool.shutdown(wait=False, cancel_futures=True)

    def stop(self) -> None:
        """Stops taking new tasks; running ones finish. Thread-safe."""
        self._engine.stop()

    def shutdown(self) -> None:
        """Requests a graceful shutdown; `run()` returns once done. Thread-safe."""
        if self.loop is None or self._shutdown_requested is None:
            return
        self.loop.call_soon_threadsafe(self._shutdown_requested.set)

    def _add_signal_handlers(self) -> list[signal.Signals]:
        if not self._install_signals or threading.current_thread() is not threading.main_thread():
            return []
        assert self.loop is not None
        installed: list[signal.Signals] = []
        handlers = {signal.SIGTERM: self.shutdown, signal.SIGINT: self.shutdown}
        if hasattr(signal, "SIGTSTP"):
            handlers[signal.SIGTSTP] = self._on_tstp
        for sig, fn in handlers.items():
            try:
                self.loop.add_signal_handler(sig, fn)
                installed.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        return installed

    def _on_tstp(self) -> None:
        log.info("received SIGTSTP; no new tasks will be processed")
        self.stop()

    # -- running handlers ----------------------------------------------------

    async def run_in_thread(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Runs a blocking function in the worker's thread pool, keeping the
        current task context visible via `current_task()`."""
        assert self.loop is not None
        if self._pool is None:
            with self._pool_lock:
                if self._pool is None:
                    self._pool = ThreadPoolExecutor(
                        max_workers=self._pool_size, thread_name_prefix="pyasynq-task"
                    )
        ctx = contextvars.copy_context()
        return await self.loop.run_in_executor(self._pool, ctx.run, fn, *args)

    async def _invoke(self, ctx: TaskContext) -> Any:
        _current.set(ctx)
        if self._handler_is_async:
            return await self._handler(ctx)
        return await self.run_in_thread(self._handler, ctx)

    async def _pump(self) -> None:
        while (events := await self._engine._anext_events(256)) is not None:
            for ev in events:
                try:
                    self._dispatch(ev)
                except Exception:
                    log.exception("failed to handle engine event %r", ev[0])

    def _dispatch(self, ev: tuple[Any, ...]) -> None:
        kind = ev[0]
        if kind == "job":
            self._start(ev[1], ev[2], ev[3])
        elif kind == "cancel":
            self._cancel(ev[1], ev[3])
        elif kind == "aggregate":
            self._spawn(self._aggregate(ev[1], ev[2], ev[3], ev[4]))
        elif kind == "health":
            if self._health_check is not None:
                err = RedisError(ev[1]) if ev[1] else None
                self._spawn(self._call_user(self._health_check, err))
        elif kind == "closed":
            for job in self._jobs.values():
                job.finalized = True
                job.ctx._mark_cancelled("shutdown")
                if job.task is not None:
                    job.task.cancel()

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        assert self.loop is not None
        t = self.loop.create_task(coro)
        self._background.add(t)
        t.add_done_callback(self._background.discard)

    def _start(self, exec: int, msg: _core.TaskMessage, deadline: datetime) -> None:
        assert self.loop is not None
        ctx = TaskContext(msg, deadline, self._engine, self)
        if deadline <= datetime.now(timezone.utc):
            self._spawn(self._fail(exec, ctx, DeadlineExceeded()))
            return
        task = self.loop.create_task(self._invoke(ctx), name=f"pyasynq:{msg.type}:{msg.id}")
        self._jobs[exec] = _Job(exec, ctx, task)
        task.add_done_callback(functools.partial(self._on_done, exec))

    def _on_done(self, exec: int, task: asyncio.Task[Any]) -> None:
        job = self._jobs.pop(exec, None)
        if job is None or job.finalized:
            return
        if task.cancelled():
            # Not cancelled by the engine (that sets `finalized`): the
            # handler raised CancelledError itself.
            self._spawn(self._fail(exec, job.ctx, TaskCanceled()))
            return
        exc = task.exception()
        if exc is None:
            result = task.result()
            if isinstance(result, (bytes, bytearray, memoryview)):
                self._engine.ack(exec, "success", result=bytes(result))
            else:
                self._engine.ack(exec, "success")
        else:
            self._spawn(self._fail(exec, job.ctx, exc))

    def _cancel(self, exec: int, reason: str) -> None:
        job = self._jobs.get(exec)
        if job is None or job.finalized:
            return
        job.finalized = True
        job.ctx._mark_cancelled(reason)
        if job.task is not None:
            job.task.cancel()
        if reason == "deadline_exceeded":
            self._spawn(self._fail(exec, job.ctx, DeadlineExceeded()))
        elif reason == "canceled":
            self._spawn(self._fail(exec, job.ctx, TaskCanceled()))
        elif reason == "lease_expired":
            self._spawn(self._call_error_handler(job.ctx, LeaseExpired()))

    async def _fail(self, exec: int, ctx: TaskContext, exc: BaseException) -> None:
        await self._call_error_handler(ctx, exc)
        msg = ctx.message
        err = error_message(exc)
        if isinstance(exc, RevokeTask):
            log.warning("revoke task id=%s", ctx.id)
            self._engine.ack(exec, "revoke", error=err)
            return
        if isinstance(exc, SkipRetry):
            self._engine.ack(exec, "archive", error=err)
            return
        delay, is_failure = self._decide(ctx, exc)
        if is_failure and msg.retried >= msg.retry:
            log.warning("retry exhausted for task id=%s: %s", ctx.id, err)
            self._engine.ack(exec, "archive", error=err)
        else:
            self._engine.ack(exec, "retry", error=err, retry_in=delay, is_failure=is_failure)

    def _decide(self, ctx: TaskContext, exc: BaseException) -> tuple[float, bool]:
        try:
            if isinstance(exc, Retry):
                delay = exc.after
            else:
                delay = seconds(self._retry_delay(ctx.retry_count, exc, ctx)) or 0.0
        except Exception:
            log.exception("retry_delay raised; using the default delay")
            delay = default_retry_delay(ctx.retry_count, exc, ctx)
        try:
            is_failure = exc.failure if isinstance(exc, Retry) else bool(self._is_failure(exc))
        except Exception:
            log.exception("is_failure raised; counting as failure")
            is_failure = True
        return max(delay, 0.0), is_failure

    def _rust_failure_policy(
        self, retried: int, err: str, msg: _core.TaskMessage
    ) -> tuple[float, bool]:
        """Called by the engine (from a Rust thread) for failures it records
        itself: lease recovery and unacked cancellations."""
        exc = _RUST_ERRORS.get(err, RuntimeError)(err)
        ctx = TaskContext(msg, datetime.now(timezone.utc), self._engine, self)
        return self._decide(ctx, exc)

    async def _call_error_handler(self, ctx: TaskContext, exc: BaseException) -> None:
        if self._error_handler is not None:
            await self._call_user(self._error_handler, ctx, exc)

    async def _call_user(self, fn: Callable[..., Any], *args: Any) -> None:
        try:
            if is_async_callable(fn):
                await fn(*args)
            else:
                fn(*args)
        except Exception:
            log.exception("%s raised", getattr(fn, "__name__", fn))

    # -- groups ----------------------------------------------------------------

    async def _aggregate(
        self, queue: str, group: str, set_id: str, msgs: list[_core.TaskMessage]
    ) -> None:
        from pyasynq.client import Client

        assert self._group_aggregator is not None
        tasks = [Task(m.type, bytes(m.payload), dict(m.headers)) for m in msgs]
        try:
            agg = await maybe_await(self._group_aggregator(group, tasks))
            if self._client is None:
                self._client = Client(self._redis_url)
            opts = {"queue": queue, **agg.options}
            await self._client.aenqueue(agg.type, agg.payload, headers=agg.headers, **opts)
            await self._engine._acomplete_aggregation(queue, group, set_id)
        except Exception:
            # The set is reclaimed into the group by the recoverer later.
            log.exception("failed to aggregate group %r in queue %r", group, queue)


def _weak_policy(worker: Worker) -> Callable[[int, str, _core.TaskMessage], tuple[float, bool]]:
    """The engine's failure policy without a strong reference back to the
    worker (Rust-held references are invisible to Python's GC)."""
    ref = weakref.ref(worker)

    def policy(retried: int, err: str, msg: _core.TaskMessage) -> tuple[float, bool]:
        w = ref()
        if w is None:
            return _core.default_retry_delay_seconds(retried), True
        return w._rust_failure_policy(retried, err, msg)

    return policy
