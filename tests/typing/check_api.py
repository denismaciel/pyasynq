"""Checked by mypy, never executed. `# type: ignore[...]` lines must
be errors (warn_unused_ignores turns a missing error into a failure)."""

from dataclasses import dataclass
from datetime import timedelta

from typing_extensions import assert_type

from pyasynq import Asynq, Inspector, TaskContext, TaskDef, TaskInfo
from pyasynq._core import TaskState
from pyasynq.testing import run_task

app = Asynq()


@dataclass
class Email:
    to: str


@app.task("email:send", queue="critical", timeout=30)
async def send(email: Email, ctx: TaskContext) -> dict[str, bool]:
    return {"sent": True}


@app.task
def render(n: int) -> bytes:
    return b""


@app.task("tick")
async def tick() -> None:
    pass


@app.task("ctx-only")
def ctx_only(ctx: TaskContext) -> int:
    return ctx.retry_count


@app.task("optional")
def optional(n: int | None) -> None:
    pass


async def check() -> None:
    assert_type(send, TaskDef[Email, dict[str, bool]])
    assert_type(render, TaskDef[int, bytes])
    assert_type(tick, TaskDef[None, None])
    assert_type(ctx_only, TaskDef[None, int])

    info = await send.aenqueue(Email("a@b.c"), process_in=timedelta(seconds=5), max_retry=3)
    assert_type(info, TaskInfo[dict[str, bool]])
    assert_type(await send.wait_result(info), dict[str, bool])
    assert_type(await app.wait_result(info), dict[str, bool])
    assert_type(render.enqueue(1).state, TaskState)

    tick.enqueue()
    optional.enqueue()
    optional.enqueue(3)
    await run_task(render, 2)

    send.enqueue("not an email")  # type: ignore[arg-type]
    send.enqueue()  # type: ignore[call-arg]
    tick.enqueue(1)  # type: ignore[call-overload]
    send.enqueue(Email("a@b.c"), proces_in=5)  # type: ignore[call-arg]
    send.enqueue(Email("a@b.c"), timeout="30")  # type: ignore[arg-type]
    await app.client.aenqueue("x", b"", queue=1)  # type: ignore[arg-type]
    Inspector().list_tasks("default", "retrying")  # type: ignore[arg-type]


@app.task("bad", proces_in=5)  # type: ignore[call-overload, untyped-decorator]
def bad() -> None:
    pass
