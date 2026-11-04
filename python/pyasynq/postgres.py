"""Helpers for the Postgres backend (``postgres://`` URLs).

Postgres stores tasks in the application's own database, so a task can be
enqueued in the same transaction as the data it refers to: it becomes
visible to workers when, and only if, the transaction commits.

    stmt = send_welcome.enqueue_statement(Signup(email=email))
    with conn.transaction():
        conn.execute("INSERT INTO users (email) VALUES (%s)", (email,))
        conn.execute(stmt.sql, stmt.params)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Literal, TypedDict, TypeVar

from pyasynq import _core
from pyasynq._core import TaskInfo

if TYPE_CHECKING:
    from typing_extensions import LiteralString, Unpack

    from pyasynq.options import Duration

__all__ = ["EnqueueStatement", "StatementOptions", "enqueue_statement", "schema_sql"]

R = TypeVar("R")

ParamStyle = Literal["format", "numeric"]


class StatementOptions(TypedDict, total=False):
    """Enqueue options supported in a transaction. Uniqueness needs pyasynq's
    lock table and is only available through the client."""

    queue: str
    max_retry: int
    task_id: str
    timeout: Duration
    deadline: datetime
    process_at: datetime
    process_in: Duration
    retention: Duration
    group: str
    headers: dict[str, str]


@dataclass(frozen=True)
class EnqueueStatement(Generic[R]):
    """An ``INSERT`` to run on your own connection: ``cursor.execute(stmt.sql, stmt.params)``."""

    sql: LiteralString
    params: tuple[Any, ...]
    info: TaskInfo[R]


def enqueue_statement(
    task_type: str,
    payload: bytes | None = None,
    *,
    paramstyle: ParamStyle = "format",
    **options: Unpack[StatementOptions],
) -> EnqueueStatement[Any]:
    """The SQL that enqueues a task. ``paramstyle="format"`` gives ``%s``
    placeholders (psycopg, Django, SQLAlchemy ``exec_driver_sql``);
    ``"numeric"`` gives ``$1`` (asyncpg)."""
    if "unique" in options:
        raise ValueError("unique tasks can't be enqueued by SQL; use the client")
    sql, params, info = _core.pg_enqueue_statement(task_type, payload, paramstyle=paramstyle, **options)
    return EnqueueStatement(sql, params, info)


def schema_sql() -> LiteralString:
    """The schema pyasynq creates on connect, for applying it with your own
    migration tool instead."""
    return _core.pg_schema()
