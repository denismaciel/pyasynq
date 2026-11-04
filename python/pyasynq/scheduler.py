from __future__ import annotations

import asyncio
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyasynq import _core
from pyasynq.worker import DEFAULT_REDIS_URL, seconds

PostEnqueue = Callable[[str, "_core.TaskInfo | None", "BaseException | None"], None]


@dataclass
class PeriodicEntry:
    cronspec: str
    task: Any
    payload: Any = None
    options: dict[str, Any] = field(default_factory=dict)


class Scheduler:
    """Enqueues tasks on cron specs ("*/5 * * * *", "@daily",
    "@every 30s", "CRON_TZ=Europe/Berlin 0 9 * * *").

    Runs alongside workers; every scheduler instance enqueues its own
    entries, so run exactly one per set of periodic tasks.
    """

    def __init__(
        self,
        redis_url: str = DEFAULT_REDIS_URL,
        *,
        location: str = "UTC",
        heartbeat_interval: float = 10,
        post_enqueue: PostEnqueue | None = None,
        install_signal_handlers: bool = True,
    ):
        cb = None
        if post_enqueue is not None:

            def cb(entry_id: str, info: Any, err: str | None) -> None:
                post_enqueue(entry_id, info, _core.AsynqError(err) if err else None)

        self._core = _core.Scheduler(
            redis_url,
            location=location,
            heartbeat_interval=seconds(heartbeat_interval),
            post_enqueue=cb,
        )
        self._install_signals = install_signal_handlers

    @property
    def id(self) -> str:
        return self._core.id

    def register(
        self,
        cronspec: str,
        task_type: str,
        payload: bytes | None = None,
        options: dict[str, Any] | None = None,
        **more: Any,
    ) -> str:
        """Returns the entry id."""
        return self._core.register(cronspec, task_type, payload, **{**(options or {}), **more})

    def unregister(self, entry_id: str) -> bool:
        return self._core.unregister(entry_id)

    def shutdown(self) -> None:
        self._core.shutdown()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        installed = []
        if self._install_signals and threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, self.shutdown)
                    installed.append(sig)
                except (NotImplementedError, RuntimeError):
                    pass
        fut = self._core._arun()
        try:
            await asyncio.shield(fut)
        except asyncio.CancelledError:
            # Let the Rust side clear its entries before propagating.
            self.shutdown()
            await fut
            raise
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)

    def run_forever(self) -> None:
        asyncio.run(self.run())
