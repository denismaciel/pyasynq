"""Postgres lifecycle operations racing each other. Triggers that take an
advisory lock hold one side of the race at a chosen point."""

import asyncio
from typing import Any

import psycopg
import pyasynq
import pytest
from conftest import FAST, wait_for
from pyasynq._core import Broker
from pyasynq.postgres import enqueue_statement

SHUTDOWN_FAST: dict[str, Any] = {**FAST, "shutdown_timeout": 0.1}


def task_state(conn, tid):
    row = conn.execute("SELECT state FROM pyasynq_tasks WHERE id = %s", (tid,)).fetchone()
    return row[0] if row else None


def scalar(conn, query, params=()):
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def hold_updates_to(conn, state, key):
    """Makes updates into `state` wait on advisory lock `key`, held by `conn`."""
    conn.execute(
        f"""CREATE FUNCTION test_hold() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.state = '{state}' THEN PERFORM pg_advisory_xact_lock({key}); END IF;
                RETURN NEW;
            END $$"""
    )
    conn.execute("CREATE TRIGGER test_hold AFTER UPDATE ON pyasynq_tasks FOR EACH ROW EXECUTE FUNCTION test_hold()")
    conn.execute("SELECT pg_advisory_lock(%s)", (key,))


def waiting(conn, query_like):
    return scalar(
        conn,
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE %s",
        (query_like,),
    )


def waiting_on_advisory(conn):
    return scalar(conn, "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'advisory'")


async def test_queue_removal_waits_for_transactional_enqueue(pg_url):
    client = pyasynq.Client(pg_url)
    inspector = pyasynq.Inspector(pg_url)
    first = client.enqueue("first")
    inspector.delete_task("default", first.id)
    with psycopg.connect(pg_url) as tx:
        stmt = enqueue_statement("next")
        tx.execute(stmt.sql, stmt.params)
        removal = asyncio.create_task(asyncio.to_thread(inspector.delete_queue, "default"))
        await asyncio.wait({removal}, timeout=0.3)
        assert not removal.done()
        tx.commit()
        with pytest.raises(pyasynq.FailedPreconditionError):
            await asyncio.wait_for(removal, 2)
    with psycopg.connect(pg_url, autocommit=True) as conn:
        assert task_state(conn, stmt.info.id) == "pending"
    assert inspector.queues() == ["default"]


async def test_forced_queue_removal_spares_concurrently_claimed_task(pg_url):
    info = pyasynq.Client(pg_url).enqueue("work")
    inspector = pyasynq.Inspector(pg_url)
    broker = Broker(pg_url)
    with psycopg.connect(pg_url, autocommit=True) as conn:
        hold_updates_to(conn, "active", 85012)
        claim = asyncio.create_task(asyncio.to_thread(broker.dequeue, "default"))
        try:
            await wait_for(lambda: waiting_on_advisory(conn))
            removal = asyncio.create_task(asyncio.to_thread(inspector.delete_queue, "default", force=True))
            await wait_for(lambda: waiting(conn, "%DELETE FROM pyasynq_tasks WHERE queue = $1 AND state%"))
        finally:
            conn.execute("SELECT pg_advisory_unlock(85012)")
        claimed = await asyncio.wait_for(claim, 3)
        assert claimed is not None and claimed[0].id == info.id
        with pytest.raises(pyasynq.FailedPreconditionError, match="active"):
            await asyncio.wait_for(removal, 3)
        assert task_state(conn, info.id) == "active"
    assert inspector.queues() == ["default"]


async def test_janitor_spares_task_requeued_while_it_waited(pg_url, run_app):
    info = pyasynq.Client(pg_url).enqueue("work", retention=60)
    broker = Broker(pg_url)
    claimed = broker.dequeue("default")
    assert claimed is not None
    broker.mark_as_complete(claimed[0])
    inspector = pyasynq.Inspector(pg_url)
    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute("UPDATE pyasynq_tasks SET expires_at = now() - interval '1 day' WHERE id = %s", (info.id,))
        hold_updates_to(conn, "pending", 85013)
        try:
            requeue = asyncio.create_task(asyncio.to_thread(inspector.run_task, "default", info.id))
            await wait_for(lambda: waiting_on_advisory(conn))
            app = pyasynq.Asynq(pg_url)
            async with run_app(app, task_check_interval=10, janitor_interval=100, shutdown_timeout=0.1) as worker:
                worker.stop()
                await wait_for(lambda: waiting(conn, "%DELETE FROM pyasynq_tasks t%"))
                conn.execute("SELECT pg_advisory_unlock(85013)")
                await asyncio.wait_for(requeue, 3)
                await asyncio.sleep(0.2)
                assert task_state(conn, info.id) == "pending"
        finally:
            conn.execute("SELECT pg_advisory_unlock_all()")


async def test_shutdown_is_bounded_while_a_claim_blocks(pg_url):
    info = pyasynq.Client(pg_url).enqueue("work")
    with psycopg.connect(pg_url, autocommit=True) as conn:
        hold_updates_to(conn, "active", 85014)
        worker = pyasynq.Worker(lambda ctx: None, redis_url=pg_url, **SHUTDOWN_FAST)
        run = asyncio.create_task(worker.run())
        try:
            await wait_for(lambda: waiting_on_advisory(conn))
            worker.shutdown()
            await asyncio.wait_for(asyncio.shield(run), 1)
        finally:
            conn.execute("SELECT pg_advisory_unlock(85014)")
            worker.shutdown()
            await asyncio.wait_for(run, 5)
        # The interrupted claim was canceled on the server, not left to commit.
        await asyncio.sleep(0.2)
        assert task_state(conn, info.id) == "pending"


async def test_shutdown_is_bounded_while_the_table_is_locked(pg_url):
    pyasynq.Client(pg_url)
    worker = pyasynq.Worker(lambda ctx: None, redis_url=pg_url, **SHUTDOWN_FAST)
    run = asyncio.create_task(worker.run())
    await wait_for(lambda: worker.status == "active")
    with psycopg.connect(pg_url) as blocker, psycopg.connect(pg_url, autocommit=True) as observer:
        blocker.execute("LOCK TABLE pyasynq_tasks IN ACCESS EXCLUSIVE MODE")
        try:
            await wait_for(lambda: waiting(observer, "%FOR UPDATE SKIP LOCKED%"))
            worker.shutdown()
            await asyncio.wait_for(asyncio.shield(run), 1)
        finally:
            blocker.rollback()
            worker.shutdown()
            await asyncio.wait_for(run, 5)
