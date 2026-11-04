"""Postgres-only behavior: NOTIFY wakeups, transactional enqueue, SKIP LOCKED."""

import asyncio
import time
from collections import Counter
from dataclasses import dataclass

import psycopg
import pyasynq
import pytest
from conftest import wait_for
from pyasynq import Asynq
from pyasynq.inspector import Inspector
from pyasynq.postgres import enqueue_statement, schema_sql


@dataclass
class Signup:
    email: str


@pytest.fixture
def app(pg_url):
    return Asynq(pg_url)


async def test_notify_wakes_idle_worker(app, run_app):
    done = asyncio.Event()

    @app.task("ping")
    async def ping() -> None:
        done.set()

    # With a 10s poll interval only the NOTIFY can wake the worker quickly.
    async with run_app(app, task_check_interval=10):
        await asyncio.sleep(0.5)
        start = time.monotonic()
        await ping.aenqueue()
        await asyncio.wait_for(done.wait(), 3)
    assert time.monotonic() - start < 1


async def test_transactional_enqueue(app, run_app, pg_url):
    seen = []

    @app.task("welcome")
    async def welcome(s: Signup) -> None:
        seen.append(s.email)

    inspector = Inspector(pg_url)  # connecting creates the schema
    with psycopg.connect(pg_url) as conn:
        conn.execute("CREATE TABLE users (email text)")
        conn.commit()
        for email, commit in [("rolled@back", False), ("kept@x", True)]:
            stmt = welcome.enqueue_statement(Signup(email))
            with conn.transaction(force_rollback=not commit):
                conn.execute("INSERT INTO users VALUES (%s)", (email,))
                conn.execute(stmt.sql, stmt.params)

    assert stmt.info.state == "pending"
    assert inspector.get_task_info("default", stmt.info.id).headers == {"content-type": "application/json"}
    async with run_app(app):
        await wait_for(lambda: seen, timeout=5)
    assert seen == ["kept@x"]


def test_enqueue_statement_options(pg_url):
    stmt = enqueue_statement("t", b"p", queue="q", process_in=60, paramstyle="numeric")
    assert "$8" in stmt.sql and "%s" not in stmt.sql
    assert stmt.info.state == "scheduled"
    grouped = enqueue_statement("t", group="g")
    assert grouped.info.state == "aggregating"
    pyasynq.Client(pg_url)  # creates the schema
    with psycopg.connect(pg_url) as conn:
        conn.execute(grouped.sql, grouped.params)
    assert Inspector(pg_url).groups("default") == {"g": 1}
    with pytest.raises(ValueError):
        enqueue_statement("t", paramstyle="qmark")  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError):
        enqueue_statement("t", unique=60)  # type: ignore[call-arg]


async def test_competing_workers_process_each_task_once(pg_url, run_app):
    counts: Counter[int] = Counter()
    apps = [Asynq(pg_url) for _ in range(3)]
    for a in apps:

        @a.task("job")
        async def job(n: int) -> None:
            counts[n] += 1
            await asyncio.sleep(0.001)

    client = pyasynq.Client(pg_url)
    for n in range(300):
        await client.aenqueue("job", str(n).encode(), headers={"content-type": "application/json"})
    async with run_app(apps[0], concurrency=10), run_app(apps[1], concurrency=10), run_app(apps[2], concurrency=10):
        await wait_for(lambda: len(counts) == 300, timeout=20)
        await asyncio.sleep(0.2)
    assert set(counts.values()) == {1}
    assert Inspector(pg_url).get_queue_info("default").processed == 300


def test_schema_applied_externally(pg_url):
    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute(schema_sql())
        pyasynq.Client(pg_url).enqueue("t")
        pyasynq.Client(pg_url)
        [(versions,)] = conn.execute("SELECT count(*) FROM pyasynq_schema_version").fetchall()
    assert versions == 1


def test_bad_url_raises_broker_error():
    with pytest.raises(pyasynq.PostgresError) as e:
        pyasynq.Client("postgresql://nobody@127.0.0.1:1/db")
    assert isinstance(e.value, pyasynq.BrokerError)
