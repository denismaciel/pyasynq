from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Task:
    """A task type plus payload, e.g. what a group aggregator returns.

    `options` are passed to `Client.enqueue` (queue, max_retry, timeout, ...).
    """

    type: str
    payload: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
