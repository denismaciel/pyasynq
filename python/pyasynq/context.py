"""Per-task information handed to handlers."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyasynq.errors import DeadlineExceeded, TaskCanceled

if TYPE_CHECKING:
    from pyasynq._core import Engine, TaskMessage
    from pyasynq.worker import Worker

_current: contextvars.ContextVar[TaskContext] = contextvars.ContextVar("pyasynq_task")


def current_task() -> TaskContext:
    """The task being processed by the calling handler (async or sync)."""
    try:
        return _current.get()
    except LookupError:
        raise RuntimeError("not running inside a pyasynq task handler") from None


class TaskContext:
    """What a handler knows about the task it is running.

    Sync handlers cannot be interrupted; they should poll `cancelled` (or
    call `raise_if_cancelled()`) in long loops. Async handlers are cancelled
    with `asyncio.CancelledError` at their next `await`.
    """

    __slots__ = ("_msg", "deadline", "_engine", "_cancel", "_reason", "_worker")

    def __init__(
        self,
        msg: TaskMessage,
        deadline: datetime,
        engine: Engine | None = None,
        worker: Worker | None = None,
    ):
        self._msg = msg
        self.deadline = deadline
        self._engine = engine
        self._cancel = threading.Event()
        self._reason: str | None = None
        self._worker = worker

    @property
    def id(self) -> str:
        return self._msg.id

    @property
    def type(self) -> str:
        return self._msg.type

    @property
    def payload(self) -> bytes:
        return bytes(self._msg.payload)

    @property
    def headers(self) -> dict[str, str]:
        return dict(self._msg.headers)

    @property
    def queue(self) -> str:
        return self._msg.queue

    @property
    def retry_count(self) -> int:
        """How many times this task has been retried before."""
        return self._msg.retried

    @property
    def max_retry(self) -> int:
        return self._msg.retry

    @property
    def message(self) -> TaskMessage:
        return self._msg

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @property
    def cancel_reason(self) -> str | None:
        return self._reason

    def remaining(self) -> float:
        """Seconds until the deadline (negative once passed)."""
        return (self.deadline - datetime.now(timezone.utc)).total_seconds()

    def raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            if self._reason == "deadline_exceeded":
                raise DeadlineExceeded()
            raise TaskCanceled()

    async def write_result(self, data: bytes) -> int:
        """Stores `data` as the task result (visible while the task is retained)."""
        if self._engine is None:
            raise RuntimeError("no worker engine attached")
        return await self._engine._awrite_result(self.queue, self.id, bytes(data))

    def write_result_sync(self, data: bytes) -> int:
        """`write_result` for sync handlers running in the worker's thread pool."""
        if self._worker is None or self._worker.loop is None:
            raise RuntimeError("no worker loop attached")
        fut = asyncio.run_coroutine_threadsafe(self.write_result(data), self._worker.loop)
        return fut.result()

    def _mark_cancelled(self, reason: str) -> None:
        self._reason = reason
        self._cancel.set()

    def __repr__(self) -> str:
        return f"TaskContext(id={self.id!r}, type={self.type!r}, queue={self.queue!r})"
