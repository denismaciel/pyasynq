# pyasynq

A distributed task queue for Python, backed by Redis or Postgres, with the engine written in Rust.

pyasynq is a port of Go's [asynq](https://github.com/hibiken/asynq). On Redis it is **wire-compatible** with asynq: Python and Go services can enqueue tasks for each other, run workers on the same queues, and use the same tools ([asynqmon](https://github.com/hibiken/asynqmon), the `asynq` CLI). On [Postgres](#postgres-backend) your tasks live in your application database and can be enqueued inside your own transactions.

Rust (pyo3 + tokio) owns everything that doesn't need to be Python: dequeuing, leases, heartbeats, retries, recovery, scheduling and the storage layer (Redis Lua scripts or Postgres queries). Your handlers are plain Python, either `async def` on the event loop or `def` in a thread pool.

## Install

```sh
pip install pyasynq
```

Wheels are built for Linux (x86_64, aarch64, musl) and macOS (x86_64, arm64), for Python 3.10 and newer, plus free-threaded CPython 3.14t, where importing pyasynq keeps the GIL off and sync handlers run in parallel. You need Redis, or Postgres (the test suite runs on Postgres 17).

## Quick start

```python
# tasks.py
from dataclasses import dataclass
from pyasynq import Asynq, TaskContext

app = Asynq("redis://localhost:6379/0")

@dataclass
class Email:
    to: str
    subject: str

@app.task("email:send", queue="critical", max_retry=5, timeout=30)
async def send_email(email: Email, ctx: TaskContext) -> dict:
    ...  # payload is decoded from the type hint (dataclass, pydantic, bytes, JSON)
    return {"sent": True}
```

Enqueue from anywhere:

```python
info = send_email.enqueue(Email("a@b.c", "hi"))                  # sync
info = await send_email.aenqueue(Email("a@b.c", "hi"), process_in=60)  # async, delayed
result = await send_email.wait_result(info, timeout=10)          # needs retention=...
```

Run workers and the scheduler:

```sh
pyasynq worker tasks:app -q critical=6 -q default=3 -c 20
pyasynq worker tasks:app --processes 4     # one worker server per process
pyasynq scheduler tasks:app                # enqueues @app.periodic tasks
```

## Typing

The package ships type information, including stubs for the Rust extension. Each task's payload and result types come from the handler's signature, so type checkers catch mistakes at the call site:

```python
send_email                                   # TaskDef[Email, dict]
info = send_email.enqueue(Email("a@b.c", "hi"))   # TaskInfo[dict]
await send_email.wait_result(info)           # dict
send_email.enqueue("a@b.c")                  # error: expected Email
send_email.enqueue(email, proces_in=60)      # error: unknown option
```

The payload is the handler's first positional parameter. A parameter annotated `ctx: TaskContext` receives the context. A task without a payload is `TaskDef[None, R]` and is enqueued with no arguments.

## Features

**Task options:** `queue`, `max_retry`, `timeout`, `deadline`, `unique` (dedupe window), `retention` (keep results), `group` (batching), `process_in` / `process_at`, custom `headers`.

**Errors control retries:**

| raise | effect |
|---|---|
| any exception | retry with exponential backoff, then archive |
| `SkipRetry` | archive immediately |
| `RevokeTask` | drop the task, no retry or archive |
| `Retry("msg", after=30)` | retry after 30s |
| `RateLimited(after=5)` | retry later, not counted as a failure |

**Middleware, error hooks and lifespan:**

```python
@app.middleware
async def timing(ctx, call_next):
    return await call_next(ctx)

@app.on_error
def report(ctx, exc): ...

@app.lifespan
async def resources(app):
    db = await connect(); yield; await db.close()
```

**Cancellation is cooperative.** Timeouts, deadlines and `inspector.cancel_processing(id)` cancel async handlers. Sync handlers should check `ctx.cancelled` or call `ctx.raise_if_cancelled()`.

**Periodic tasks:**

```python
app.periodic("@every 30s", payload=None)(cleanup)
app.periodic("CRON_TZ=Europe/Berlin 0 9 * * *", Report(kind="daily"))(report)
```

**Groups (batching):** tasks enqueued with `group="key"` are combined into one task once the grace period, max delay or max size is reached:

```python
@app.group_aggregator
def merge(group: str, tasks: list[Task]) -> Task: ...
```

**Rate limiting across workers** (uses the same Redis key as Go's `x/rate`):

```python
from pyasynq.rate import Semaphore
openai = Semaphore(app.redis_url, "openai", max_tokens=5)

@app.task
async def summarize(doc: Doc, ctx: TaskContext):
    async with openai.hold(ctx, retry_after=3):
        ...
```

**Inspection:**

```python
from pyasynq import Inspector
i = Inspector("redis://localhost:6379/0")
i.get_queue_info("default"); i.list_tasks("default", "retry"); i.pause_queue("default")
```

```sh
pyasynq inspect queues
pyasynq inspect tasks default retry
pyasynq inspect cancel <task-id>
```

**Observability:**

- `pyasynq.metrics.QueueMetricsCollector` is a Prometheus collector that uses asynq's metric names. Install with `pip install pyasynq[metrics]`.
- `pyasynq.otel.instrument(app)` propagates OpenTelemetry trace context through task headers. Install with `pip install pyasynq[otel]`.
- Logs go to Python `logging` under `pyasynq.*`.

**Testing:**

```python
from pyasynq.testing import run_task, drain, memory_url
assert await run_task(send_email, Email("a@b.c", "hi")) == {"sent": True}  # no broker
await drain(app)  # real worker until queues are empty
```

For tests that go through a real worker without Redis or Postgres, use an in-process broker: `Asynq(memory_url())` (or any `memory://<name>` URL). Every client, worker, inspector and scheduler in the process that opens the same URL shares one store, and `pyasynq.testing.reset_memory()` empties it. It runs the same conformance suite as the other backends. It doesn't persist anything and can't be shared between processes.

Its clock can be moved forward, so retries, scheduled tasks, retention and uniqueness TTLs don't need real waits:

```python
from pyasynq.testing import advance_time
info = await report.aenqueue(process_in=timedelta(hours=1))
advance_time(app, timedelta(hours=1))   # the task is pending now
await drain(app)
```

Leases, task deadlines and cron schedules stay on real time, so advancing the clock never makes a running task look abandoned.

**Low-level API:** `pyasynq.Client` and `pyasynq.Worker(handler)` mirror Go's `Client` and `Server` when you don't want the `Asynq` app layer.

## Postgres backend

Pass a `postgres://` URL anywhere a Redis URL goes (`Asynq`, `Client`, `Worker(redis_url=...)`, `Inspector`, `Scheduler`, `Semaphore`). The whole API works the same; the test suite runs one conformance suite against every backend.

```python
app = Asynq("postgresql://app@localhost/app")
```

The design follows [procrastinate](https://github.com/procrastinate-org/procrastinate), with the queries issued from Rust:

- Every task is a row in `pyasynq_tasks`, holding the same asynq protobuf message the Redis backend stores, plus indexed columns for its state.
- Workers claim tasks with `UPDATE ... FOR UPDATE SKIP LOCKED`, so any number of workers can share a queue without blocking each other. Every state transition is a single statement.
- A trigger runs `NOTIFY` when a task becomes pending. Idle workers `LISTEN` and pick the task up right away, and the poll interval stays as a fallback. `Inspector.cancel_processing` also travels over `NOTIFY`.
- The schema is created on first connect, guarded by an advisory lock and a version table. To manage it with your own migration tool, apply `pyasynq.postgres.schema_sql()` instead.

The main reason to use it is **transactional enqueue**. The task is committed or rolled back together with your data:

```python
stmt = send_email.enqueue_statement(Email("a@b.c", "hi"))   # paramstyle="numeric" for asyncpg
with conn.transaction():                                     # psycopg
    conn.execute("INSERT INTO users (email) VALUES (%s)", ("a@b.c",))
    conn.execute(stmt.sql, stmt.params)
```

Trade-offs:

- Go asynq, asynqmon and the `asynq` CLI only understand the Redis layout.
- Unique tasks can't be enqueued with `enqueue_statement` (use the client).
- Postgres is slower than Redis. On a laptop with durable commits, one worker processed about 7k no-op tasks/s on Postgres (it claims up to 32 tasks per statement) and about 20k/s on Redis. Each `Client.enqueue` is its own commit, so one client enqueued about 2k tasks/s on Postgres, against 25k/s on Redis. Several `enqueue_statement`s in one transaction share a commit.

## Differences from Go asynq

The data format and Redis layout are the same as asynq v0.26. pyasynq fixes a few of asynq's edge cases:

- Errors that don't count as failures (`RateLimited`, or `is_failure` returning False) are retried without using up `max_retry`.
- `timeout` and `retention` under one second raise `ValueError`. Go silently truncates them to 0, which means "unset".
- Delayed tasks with a `group` (`process_in` combined with `group`) join their group when due, and failed aggregations are put back into their group instead of being stranded.
- `delete_queue` also counts completed, grouped and aggregating tasks. `force=True` removes them.
- A task-ID conflict doesn't leave a uniqueness lock behind, and a deleted queue reappears when a client enqueues into it again.

## Development

Everything you need (Rust, Python, uv, maturin, Redis, Postgres, Go) comes from the Nix dev shell:

```sh
nix develop
make dev    # build the Rust extension into .venv
make test   # cargo + pytest (spawns its own redis and postgres; Go interop tests need go)
make test-ft  # pytest on free-threaded Python 3.14t in .venv-ft; fails if the GIL turns on
make lint
```

Layout: `src/` holds the Rust engine and pyo3 bindings. `src/rdb/` has the storage layer: the Redis Lua scripts in `src/rdb/lua/` and the Postgres backend and schema in `src/rdb/pg/`, and the in-process backend in `src/rdb/memory.rs`. `python/pyasynq/` holds the Python API, and `interop/go/` holds a small Go program the tests use to check compatibility against real asynq.

## License

MIT. See [LICENSE](LICENSE).
