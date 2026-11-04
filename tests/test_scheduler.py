"""Periodic tasks, cross-checked with Go's Inspector."""

import asyncio

import pytest

import pyasynq
from conftest import wait_for


@pytest.fixture
def make_scheduler(redis_addr, rdb):
    def make(**opts):
        return pyasynq.Scheduler(
            f"redis://{redis_addr}/0", heartbeat_interval=0.2, install_signal_handlers=False, **opts
        )

    return make


async def test_every_enqueues_and_records_history(make_scheduler, rdb, go_asynq):
    posted = []
    sched = make_scheduler(post_enqueue=lambda entry, info, err: posted.append((entry, info.id, err)))
    entry = sched.register("@every 1s", "tick", b"t", queue="periodic", max_retry=2)
    sched.register("0 0 1 1 *", "yearly", options={"timeout": 30})
    run = asyncio.create_task(sched.run())
    await wait_for(lambda: rdb.llen("asynq:{periodic}:pending") >= 2, timeout=5)

    entries = await asyncio.to_thread(go_asynq, "schedulers")
    by_spec = {e["Spec"]: e for e in entries}
    assert set(by_spec) == {"@every 1s", "0 0 1 1 *"}
    # Go parses our option strings back into Options (it drops ones it can't parse).
    assert len(by_spec["@every 1s"]["Opts"]) == 2
    assert by_spec["@every 1s"]["ID"] == entry
    history = await asyncio.to_thread(go_asynq, "history", entry=entry)
    assert len(history) >= 2

    sched.shutdown()
    await run
    assert rdb.zcard("asynq:schedulers") == 0
    assert rdb.exists(f"asynq:scheduler_history:{entry}") == 0
    assert {e for e, _, _ in posted} == {entry}
    assert all(err is None for _, _, err in posted)


async def test_inspector_lists_python_entries(make_scheduler, redis_addr):
    sched = make_scheduler(location="Europe/Berlin")
    sched.register("CRON_TZ=Asia/Tokyo 0 9 * * *", "report", queue="reports", retention=3600)
    run = asyncio.create_task(sched.run())
    inspector = pyasynq.Inspector(f"redis://{redis_addr}/0")
    await wait_for(lambda: inspector.scheduler_entries())
    [e] = inspector.scheduler_entries()
    assert (e.spec, e.task_type) == ("CRON_TZ=Asia/Tokyo 0 9 * * *", "report")
    assert e.options == ['Queue("reports")', "Retention(3600s)"]
    assert e.next_enqueue_at.hour == 0  # 09:00 Tokyo is 00:00 UTC
    sched.shutdown()
    await run


def test_invalid_specs(make_scheduler):
    sched = make_scheduler()
    with pytest.raises(ValueError):
        sched.register("every minute", "t")
    with pytest.raises(ValueError):
        sched.register("@every 0s", "t")
    with pytest.raises(ValueError):
        make_scheduler(location="Mars/Olympus")


async def test_app_periodic(redis_addr, rdb):
    app = pyasynq.Asynq(f"redis://{redis_addr}/0")

    @app.periodic("@every 1s", {"n": 1}, queue="cron")
    @app.task("cron:job")
    async def job(p: dict) -> None:
        pass

    sched = app.scheduler(heartbeat_interval=0.2, install_signal_handlers=False)
    run = asyncio.create_task(sched.run())
    await wait_for(lambda: rdb.llen("asynq:{cron}:pending") >= 1, timeout=5)
    sched.shutdown()
    await run
    [tid] = rdb.lrange("asynq:{cron}:pending", 0, 0)
    msg = pyasynq.TaskMessage.decode(rdb.hget(f"asynq:{{cron}}:t:{tid.decode()}", "msg"))
    assert (msg.type, msg.payload, msg.headers) == ("cron:job", b'{"n":1}', {"content-type": "application/json"})
