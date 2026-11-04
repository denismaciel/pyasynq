# pyasynq

A distributed task queue for Python, backed by Redis, with the engine written in Rust.

pyasynq is a port of Go's [asynq](https://github.com/hibiken/asynq) and is **wire-compatible** with it. Python and Go services can enqueue tasks for each other, run workers on the same queues, and use the same tools ([asynqmon](https://github.com/hibiken/asynqmon), the `asynq` CLI).

Rust (pyo3 + tokio) owns everything that doesn't need to be Python: dequeuing, leases, heartbeats, retries, recovery, scheduling and the Redis Lua scripts. Your handlers are plain Python, either `async def` on the event loop or `def` in a thread pool.

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
from pyasynq.testing import run_task, drain
assert await run_task(send_email, Email("a@b.c", "hi")) == {"sent": True}  # no Redis
await drain(app)  # real worker until queues are empty
```

**Low-level API:** `pyasynq.Client` and `pyasynq.Worker(handler)` mirror Go's `Client` and `Server` when you don't want the `Asynq` app layer.

## Development

Everything you need (Rust, Python, uv, maturin, Redis, Go) comes from the Nix dev shell:

```sh
nix develop
make dev    # build the Rust extension into .venv
make test   # cargo + pytest (spawns its own redis; Go interop tests need go)
make lint
```

Layout: `src/` holds the Rust engine and pyo3 bindings (Lua scripts in `src/rdb/lua/`), `python/pyasynq/` holds the Python API, and `interop/go/` holds a small Go program the tests use to check compatibility against real asynq.

## License

MIT. See [LICENSE](LICENSE).
