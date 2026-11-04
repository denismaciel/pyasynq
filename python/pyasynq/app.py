"""The application API: declare tasks once, enqueue and process them.

    app = Asynq("redis://localhost:6379/0")

    @app.task("email:send", queue="critical", max_retry=5)
    async def send_email(msg: Email, ctx: TaskContext) -> None: ...

    await send_email.aenqueue(Email(to="a@b.c"), process_in=10)
    app.run_worker(queues={"critical": 6, "default": 3})
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import typing
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Protocol,
    TypeVar,
    cast,
    overload,
)

from pyasynq._core import TaskInfo
from pyasynq.client import Client
from pyasynq.context import TaskContext
from pyasynq.errors import AsynqError, HandlerNotFound, SkipRetry
from pyasynq.inspector import Inspector
from pyasynq.postgres import EnqueueStatement, enqueue_statement
from pyasynq.serde import CONTENT_TYPE, Codec, JsonCodec, RawCodec, codec_for, encode_result
from pyasynq.task import Task
from pyasynq.worker import (
    DEFAULT_REDIS_URL,
    ErrorHandler,
    RetryDelayFunc,
    Worker,
    default_retry_delay,
    is_async_callable,
    maybe_await,
)

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from pyasynq.options import EnqueueOptions, TaskOptions
    from pyasynq.postgres import ParamStyle, StatementOptions
    from pyasynq.scheduler import Scheduler

PayloadT = TypeVar("PayloadT", contravariant=True)
ResultT = TypeVar("ResultT")
_P = TypeVar("_P")
_R = TypeVar("_R")
_TD = TypeVar("_TD", bound="TaskDef[Any, Any]")
Middleware = Callable[[TaskContext, Callable[[TaskContext], Awaitable[Any]]], Awaitable[Any]]
Lifespan = Callable[["Asynq"], contextlib.AbstractAsyncContextManager[Any]]

_ENQUEUE_OPTIONS = ("queue", "max_retry", "timeout", "deadline", "unique", "retention", "group")


class TaskFailed(AsynqError):
    """`wait_result`: the task was archived after exhausting its retries."""

    def __init__(self, info: TaskInfo):
        super().__init__(info.last_err or "task failed")
        self.info = info


class TaskDef(Generic[PayloadT, ResultT]):
    """A registered task: callable directly, enqueueable by name.

    `PayloadT` is what `enqueue` accepts (`None` for tasks without a
    payload) and `ResultT` what `wait_result` returns.
    """

    def __init__(
        self,
        app: Asynq,
        fn: Callable[..., Any],
        name: str,
        options: dict[str, Any],
        retry_delay: RetryDelayFunc | None,
        codec: Codec | None,
    ):
        self.app = app
        self.fn = fn
        self.name = name
        self.options = options
        self.retry_delay = retry_delay
        self.is_async = is_async_callable(fn)
        functools.update_wrapper(self, fn)

        hints = typing.get_type_hints(fn)
        self._ctx_param: str | None = None
        self._payload_param: str | None = None
        self._positional: set[str] = set()
        for p in inspect.signature(fn).parameters.values():
            if p.kind is p.POSITIONAL_ONLY:
                self._positional.add(p.name)
            if hints.get(p.name) is TaskContext or (p.name == "ctx" and p.name not in hints):
                self._ctx_param = p.name
            elif self._payload_param is None and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
                self._payload_param = p.name
            elif p.default is p.empty and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
                raise TypeError(f"task {name!r}: unexpected required parameter {p.name!r}")
        if codec is not None:
            self.codec = codec
        elif self._payload_param is None:
            self.codec = RawCodec()
        else:
            self.codec = codec_for(hints.get(self._payload_param))
        ret = hints.get("return")
        self._result_codec: Codec = RawCodec() if ret is bytes else (codec_for(ret) if ret else JsonCodec())

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<TaskDef {self.name!r}>"

    # -- producing ---------------------------------------------------------

    def message(self, payload: Any = None, **options: Any) -> tuple[bytes, dict[str, Any]]:
        data = self.codec.encode(payload) if self._payload_param or payload is not None else b""
        opts = {**self.options, **options}
        headers = dict(opts.pop("headers", None) or {})
        if self.codec.content_type and CONTENT_TYPE not in headers:
            headers[CONTENT_TYPE] = self.codec.content_type
        for hook in self.app._enqueue_hooks:
            hook(self.name, headers)
        if headers:
            opts["headers"] = headers
        return data, opts

    @overload
    def enqueue(self: TaskDef[None, ResultT], /, **options: Unpack[EnqueueOptions]) -> TaskInfo[ResultT]: ...
    @overload
    def enqueue(self, payload: PayloadT, /, **options: Unpack[EnqueueOptions]) -> TaskInfo[ResultT]: ...
    def enqueue(self, payload: Any = None, /, **options: Any) -> TaskInfo[ResultT]:
        data, opts = self.message(payload, **options)
        return self.app.client.enqueue(self.name, data, **opts)

    @overload
    async def aenqueue(
        self: TaskDef[None, ResultT], /, **options: Unpack[EnqueueOptions]
    ) -> TaskInfo[ResultT]: ...
    @overload
    async def aenqueue(self, payload: PayloadT, /, **options: Unpack[EnqueueOptions]) -> TaskInfo[ResultT]: ...
    async def aenqueue(self, payload: Any = None, /, **options: Any) -> TaskInfo[ResultT]:
        data, opts = self.message(payload, **options)
        return await self.app.client.aenqueue(self.name, data, **opts)

    @overload
    def enqueue_statement(
        self: TaskDef[None, ResultT],
        /,
        *,
        paramstyle: ParamStyle = ...,
        **options: Unpack[StatementOptions],
    ) -> EnqueueStatement[ResultT]: ...
    @overload
    def enqueue_statement(
        self, payload: PayloadT, /, *, paramstyle: ParamStyle = ..., **options: Unpack[StatementOptions]
    ) -> EnqueueStatement[ResultT]: ...
    def enqueue_statement(
        self, payload: Any = None, /, *, paramstyle: ParamStyle = "format", **options: Any
    ) -> EnqueueStatement[ResultT]:
        """The SQL that enqueues this task from your own Postgres transaction
        (see `pyasynq.postgres`). Pass ``paramstyle="numeric"`` for asyncpg."""
        data, opts = self.message(payload, **options)
        return enqueue_statement(self.name, data, paramstyle=paramstyle, **opts)

    @overload
    def task(self: TaskDef[None, ResultT], /, **options: Unpack[EnqueueOptions]) -> Task: ...
    @overload
    def task(self, payload: PayloadT, /, **options: Unpack[EnqueueOptions]) -> Task: ...
    def task(self, payload: Any = None, /, **options: Any) -> Task:
        """A `Task` value, e.g. for a group aggregator to return."""
        data, opts = self.message(payload, **options)
        return Task(self.name, data, opts.pop("headers", {}), opts)

    async def wait_result(self, info: TaskInfo[ResultT], *, timeout: float | None = None) -> ResultT:
        result: ResultT = await self.app.wait_result(info, timeout=timeout)
        return result

    def decode_result(self, data: bytes | None) -> Any:
        if data is None:
            return None
        return self._result_codec.decode(data)

    # -- consuming -----------------------------------------------------------

    async def _run(self, ctx: TaskContext) -> Any:
        kwargs: dict[str, Any] = {}
        if self._payload_param is not None:
            try:
                kwargs[self._payload_param] = self.codec.decode(ctx.payload)
            except Exception as exc:
                raise SkipRetry(f"cannot decode payload: {exc}") from exc
        if self._ctx_param is not None:
            kwargs[self._ctx_param] = ctx
        # Parameters come in signature order, so positional-only ones lead.
        args = [kwargs.pop(name) for name in list(kwargs) if name in self._positional]
        if self.is_async:
            return await self.fn(*args, **kwargs)
        assert ctx._worker is not None
        return await ctx._worker.run_in_thread(functools.partial(self.fn, *args, **kwargs))


def _wrap(
    mw: Middleware, call_next: Callable[[TaskContext], Awaitable[Any]]
) -> Callable[[TaskContext], Awaitable[Any]]:
    async def call(ctx: TaskContext) -> Any:
        return await mw(ctx, call_next)

    return call


class TaskDecorator(Protocol):
    """What `@app.task("name", ...)` returns: turns a handler into a TaskDef.

    Handlers take an optional payload and an optional `ctx: TaskContext`;
    the payload's annotation becomes the TaskDef's payload type.
    """

    @overload
    def __call__(self, fn: Callable[[], Awaitable[_R]], /) -> TaskDef[None, _R]: ...
    @overload
    def __call__(self, fn: Callable[[TaskContext], Awaitable[_R]], /) -> TaskDef[None, _R]: ...  # type: ignore[overload-overlap]
    @overload
    def __call__(self, fn: Callable[[_P, TaskContext], Awaitable[_R]], /) -> TaskDef[_P, _R]: ...
    @overload
    def __call__(self, fn: Callable[[_P], Awaitable[_R]], /) -> TaskDef[_P, _R]: ...
    @overload
    def __call__(self, fn: Callable[[], _R], /) -> TaskDef[None, _R]: ...
    @overload
    def __call__(self, fn: Callable[[TaskContext], _R], /) -> TaskDef[None, _R]: ...
    @overload
    def __call__(self, fn: Callable[[_P, TaskContext], _R], /) -> TaskDef[_P, _R]: ...
    @overload
    def __call__(self, fn: Callable[[_P], _R], /) -> TaskDef[_P, _R]: ...
    @overload
    def __call__(self, fn: Callable[..., Awaitable[_R]], /) -> TaskDef[Any, _R]: ...  # type: ignore[overload-cannot-match]
    @overload
    def __call__(self, fn: Callable[..., _R], /) -> TaskDef[Any, _R]: ...  # type: ignore[overload-cannot-match]


class Asynq:
    """Holds task definitions plus the shared worker policies."""

    def __init__(
        self,
        redis_url: str = DEFAULT_REDIS_URL,
        *,
        lifespan: Lifespan | None = None,
        retry_delay: RetryDelayFunc | None = None,
        is_failure: Callable[[BaseException], bool] | None = None,
    ):
        self.redis_url = redis_url
        self.tasks: dict[str, TaskDef[Any, Any]] = {}
        self._middleware: list[Middleware] = []
        self._error_handlers: list[ErrorHandler] = []
        self._enqueue_hooks: list[Callable[[str, dict[str, str]], None]] = []
        self._lifespan = lifespan
        self._retry_delay = retry_delay
        self._is_failure = is_failure
        self._group_aggregator: Callable[..., Any] | None = None
        self._client: Client | None = None
        self._inspector: Inspector | None = None
        self.periodic_entries: list[Any] = []

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = Client(self.redis_url)
        return self._client

    @property
    def inspector(self) -> Inspector:
        if self._inspector is None:
            self._inspector = Inspector(self.redis_url)
        return self._inspector

    # -- registration --------------------------------------------------------

    @overload
    def task(self, fn: Callable[[], Awaitable[_R]], /) -> TaskDef[None, _R]: ...
    @overload
    def task(self, fn: Callable[[TaskContext], Awaitable[_R]], /) -> TaskDef[None, _R]: ...  # type: ignore[overload-overlap]
    @overload
    def task(self, fn: Callable[[_P, TaskContext], Awaitable[_R]], /) -> TaskDef[_P, _R]: ...
    @overload
    def task(self, fn: Callable[[_P], Awaitable[_R]], /) -> TaskDef[_P, _R]: ...
    @overload
    def task(self, fn: Callable[[], _R], /) -> TaskDef[None, _R]: ...
    @overload
    def task(self, fn: Callable[[TaskContext], _R], /) -> TaskDef[None, _R]: ...
    @overload
    def task(self, fn: Callable[[_P, TaskContext], _R], /) -> TaskDef[_P, _R]: ...
    @overload
    def task(self, fn: Callable[[_P], _R], /) -> TaskDef[_P, _R]: ...
    @overload
    def task(self, fn: Callable[..., Awaitable[_R]], /) -> TaskDef[Any, _R]: ...  # type: ignore[overload-cannot-match]
    @overload
    def task(self, fn: Callable[..., _R], /) -> TaskDef[Any, _R]: ...  # type: ignore[overload-cannot-match]
    @overload
    def task(
        self,
        name: str | None = None,
        /,
        *,
        retry_delay: RetryDelayFunc | None = None,
        codec: Codec | None = None,
        **options: Unpack[TaskOptions],
    ) -> TaskDecorator: ...

    def task(
        self,
        name_or_fn: Any = None,
        /,
        *,
        retry_delay: RetryDelayFunc | None = None,
        codec: Codec | None = None,
        **options: Any,
    ) -> Any:
        """Registers a handler. `name` defaults to `module.qualname`; enqueue
        defaults (queue, max_retry, timeout, deadline, unique, retention,
        group, headers) can be overridden per call."""
        unknown = set(options) - set(_ENQUEUE_OPTIONS) - {"headers"}
        if unknown:
            raise TypeError(f"unknown task options: {sorted(unknown)}")

        def register(fn: Callable[..., Any], name: str | None) -> TaskDef[Any, Any]:
            name = name or f"{fn.__module__}.{fn.__qualname__}"
            if name in self.tasks:
                raise ValueError(f"task {name!r} is already registered")
            td: TaskDef[Any, Any] = TaskDef(self, fn, name, dict(options), retry_delay, codec)
            self.tasks[name] = td
            return td

        if callable(name_or_fn):
            return register(name_or_fn, None)
        return lambda fn: register(fn, name_or_fn)

    def middleware(self, fn: Middleware) -> Middleware:
        """`async def mw(ctx, call_next): return await call_next(ctx)`;
        first registered is outermost."""
        if not is_async_callable(fn):
            raise TypeError("middleware must be async")
        self._middleware.append(fn)
        return fn

    def before_enqueue(self, fn: Callable[[str, dict[str, str]], None]) -> Callable[[str, dict[str, str]], None]:
        """`fn(task_type, headers)` may add headers to every task this app
        enqueues (e.g. trace context)."""
        self._enqueue_hooks.append(fn)
        return fn

    def on_error(self, fn: ErrorHandler) -> ErrorHandler:
        """Called with `(ctx, exc)` for every failed attempt."""
        self._error_handlers.append(fn)
        return fn

    def lifespan(self, fn: Callable[[Asynq], AsyncIterator[Any]]) -> Callable[..., Any]:
        """Async generator run around the worker/scheduler (setup, yield, teardown)."""
        self._lifespan = contextlib.asynccontextmanager(fn)
        return fn

    def group_aggregator(self, fn: Callable[[str, list[Task]], Task | Awaitable[Task]]) -> Any:
        """Combines a group's tasks into one task (enables group aggregation)."""
        self._group_aggregator = fn
        return fn

    def periodic(
        self, cronspec: str, payload: Any = None, **options: Unpack[TaskOptions]
    ) -> Callable[[_TD], _TD]:
        """Registers `cronspec` (cron or "@every 30s") for the scheduler.
        Apply on top of `@app.task`."""
        from pyasynq.scheduler import PeriodicEntry

        def register(td: _TD) -> _TD:
            if not isinstance(td, TaskDef):
                raise TypeError("@app.periodic must be applied above @app.task")
            self.periodic_entries.append(PeriodicEntry(cronspec, td, payload, dict(options)))
            return td

        return register

    # -- processing ------------------------------------------------------------

    async def handle(self, ctx: TaskContext) -> bytes | None:
        td = self.tasks.get(ctx.type)
        if td is None:
            raise HandlerNotFound(f"handler not found for task {ctx.type!r}")
        call: Callable[[TaskContext], Awaitable[Any]] = td._run
        for mw in reversed(self._middleware):
            call = _wrap(mw, call)
        return encode_result(await call(ctx))

    def _retry_delay_for(self, retried: int, exc: BaseException, ctx: TaskContext) -> Any:
        td = self.tasks.get(ctx.type)
        fn = (td.retry_delay if td else None) or self._retry_delay or default_retry_delay
        return fn(retried, exc, ctx)

    async def _on_error(self, ctx: TaskContext, exc: BaseException) -> None:
        for h in self._error_handlers:
            await maybe_await(h(ctx, exc))

    def worker(self, **options: Any) -> Worker:
        """A `Worker` for this app; options as for `Worker`."""
        options.setdefault("redis_url", self.redis_url)
        options.setdefault("retry_delay", self._retry_delay_for)
        if self._is_failure is not None:
            options.setdefault("is_failure", self._is_failure)
        if self._error_handlers:
            options.setdefault("error_handler", self._on_error)
        if self._group_aggregator is not None:
            options.setdefault("group_aggregator", self._group_aggregator)
        return Worker(self.handle, **options)

    @contextlib.asynccontextmanager
    async def _lifespan_cm(self) -> AsyncIterator[None]:
        if self._lifespan is None:
            yield
        else:
            async with self._lifespan(self):
                yield

    async def arun_worker(self, **options: Any) -> None:
        async with self._lifespan_cm():
            await self.worker(**options).run()

    def run_worker(self, **options: Any) -> None:
        asyncio.run(self.arun_worker(**options))

    def scheduler(self, **options: Any) -> Scheduler:
        from pyasynq.scheduler import Scheduler

        options.setdefault("redis_url", self.redis_url)
        sched = Scheduler(**options)
        for e in self.periodic_entries:
            sched.register(e.cronspec, e.task.name, *e.task.message(e.payload, **e.options))
        return sched

    async def arun_scheduler(self, **options: Any) -> None:
        async with self._lifespan_cm():
            await self.scheduler(**options).run()

    def run_scheduler(self, **options: Any) -> None:
        asyncio.run(self.arun_scheduler(**options))

    # -- results ---------------------------------------------------------------

    async def wait_result(
        self,
        info: TaskInfo[_R],
        *,
        timeout: float | timedelta | None = None,
        poll_interval: float = 0.1,
    ) -> _R:
        """Waits until the task completes and returns its decoded result.

        Needs `retention` on the task: without it the task is deleted on
        success and there is nothing to read (raises TaskNotFoundError).
        Raises TaskFailed if it was archived.
        """
        if isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()
        td = self.tasks.get(info.type)
        loop = asyncio.get_running_loop()
        end = None if timeout is None else loop.time() + timeout
        while True:
            current = await self.inspector.aget_task_info(info.queue, info.id)
            if current.state == "completed":
                return cast(_R, td.decode_result(current.result) if td else current.result)
            if current.state == "archived":
                raise TaskFailed(current)
            if end is not None and loop.time() >= end:
                raise TimeoutError(f"task {info.id} still {current.state}")
            await asyncio.sleep(poll_interval)
