"""Typed keyword options for registering and enqueuing tasks.

Used as `**options: Unpack[EnqueueOptions]`, so type checkers flag
misspelled or mistyped options at the call site.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TypedDict, Union

Duration = Union[float, timedelta]


class TaskOptions(TypedDict, total=False):
    """Defaults set on `@app.task(...)`; each enqueue can override them."""

    queue: str
    max_retry: int
    timeout: Duration
    deadline: datetime
    unique: Duration
    retention: Duration
    group: str
    headers: dict[str, str]


class EnqueueOptions(TaskOptions, total=False):
    """Options for a single enqueue."""

    task_id: str
    process_at: datetime
    process_in: Duration
