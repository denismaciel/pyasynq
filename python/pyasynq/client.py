from __future__ import annotations

from typing import TYPE_CHECKING

from pyasynq import _core

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from pyasynq.options import EnqueueOptions


class Client(_core.Client):
    """Enqueues tasks into Redis using asynq's wire format.

    The blocking methods release the GIL while waiting on Redis, so the same
    client can be shared between threads and event loops.
    """

    # The Rust side returns an eager asyncio.Future bound to the loop running
    # at call time; wrapping it makes these lazy coroutines like any other
    # async API, so `asyncio.run(client.aenqueue(...))` and `create_task` work.
    async def aenqueue(
        self, task_type: str, payload: bytes | None = None, **options: Unpack[EnqueueOptions]
    ) -> _core.TaskInfo:
        return await self._aenqueue(task_type, payload, **options)

    async def aping(self) -> None:
        await self._aping()
