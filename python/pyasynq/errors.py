"""Exceptions, including the ones handlers raise to steer retries."""

from __future__ import annotations

from datetime import timedelta

from pyasynq._core import (
    AsynqError,
    DuplicateTaskError,
    FailedPreconditionError,
    QueueNotFoundError,
    RedisError,
    TaskIDConflictError,
    TaskNotFoundError,
)

__all__ = [
    "AsynqError",
    "DuplicateTaskError",
    "FailedPreconditionError",
    "QueueNotFoundError",
    "RedisError",
    "TaskIDConflictError",
    "TaskNotFoundError",
    "SkipRetry",
    "RevokeTask",
    "Retry",
    "TaskCanceled",
    "DeadlineExceeded",
    "LeaseExpired",
    "HandlerNotFound",
]


class SkipRetry(AsynqError):
    """Raise from a handler to archive the task right away."""


class RevokeTask(AsynqError):
    """Raise from a handler to drop the task: no retry, no archive."""


class Retry(AsynqError):
    """Raise from a handler to retry after a specific delay.

    Counts against `max_retry` like any other failure unless the worker's
    `is_failure` says otherwise.
    """

    def __init__(self, message: str = "retry requested", *, after: float | timedelta = 0):
        super().__init__(message)
        self.after = after.total_seconds() if isinstance(after, timedelta) else float(after)


class TaskCanceled(AsynqError):
    """The task was canceled via `Inspector.cancel_processing`."""

    def __init__(self, message: str = "context canceled"):
        super().__init__(message)


class DeadlineExceeded(AsynqError, TimeoutError):
    """The task ran past its timeout/deadline."""

    def __init__(self, message: str = "context deadline exceeded"):
        super().__init__(message)


class LeaseExpired(AsynqError):
    """The worker could not extend the task lease (Redis unreachable); the
    recoverer will retry the task elsewhere."""

    def __init__(self, message: str = "asynq: task lease expired"):
        super().__init__(message)


class HandlerNotFound(AsynqError):
    """No handler registered for the task type. Retried like asynq does, so
    deploying the handler later still processes the task."""


def error_message(exc: BaseException) -> str:
    """The string stored as the task's `last_err`."""
    if isinstance(exc, (TaskCanceled, DeadlineExceeded, LeaseExpired)):
        return str(exc)
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
