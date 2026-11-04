"""Cross-worker concurrency limits (port of asynq's `x/rate`).

    sema = Semaphore(app.redis_url, "openai", max_tokens=5)

    @app.task
    async def summarize(doc: Doc, ctx: TaskContext):
        async with sema.hold(ctx, retry_after=3):
            ...

Uses the same Redis key as Go (`asynq:sema:<scope>`), so Go and Python
workers can share a limit.
"""

from __future__ import annotations

import contextlib
from typing import AsyncIterator, Iterator

from pyasynq import _core
from pyasynq.context import TaskContext
from pyasynq.errors import RateLimited


class Semaphore(_core.Semaphore):
    async def acquire(self, ctx: TaskContext) -> bool:
        """Takes a token for this task until released or its deadline."""
        return await self._aacquire(ctx.id, ctx.deadline)

    async def release(self, ctx: TaskContext) -> bool:
        return await self._arelease(ctx.id)

    @contextlib.asynccontextmanager
    async def hold(self, ctx: TaskContext, *, retry_after: float = 1) -> AsyncIterator[None]:
        """Runs the block with a token, or raises RateLimited (the task is
        retried later without counting as a failure)."""
        if not await self.acquire(ctx):
            raise RateLimited(f"semaphore {self.scope!r} is full", after=retry_after)
        try:
            yield
        finally:
            await self.release(ctx)

    @contextlib.contextmanager
    def hold_sync(self, ctx: TaskContext, *, retry_after: float = 1) -> Iterator[None]:
        """`hold` for sync handlers."""
        if not self.acquire_sync(ctx.id, ctx.deadline):
            raise RateLimited(f"semaphore {self.scope!r} is full", after=retry_after)
        try:
            yield
        finally:
            self.release_sync(ctx.id)
