"""OpenTelemetry tracing: trace context travels in task headers.

    from pyasynq.otel import instrument
    instrument(app)

Producers inject the current context (W3C `traceparent`) into the task
headers; workers continue that trace with a CONSUMER span per attempt.
Requires `opentelemetry-api`.
"""

from __future__ import annotations

from typing import Any

from pyasynq.app import Asynq
from pyasynq.context import TaskContext


def instrument(app: Asynq, tracer_provider: Any = None) -> None:
    from opentelemetry import propagate, trace
    from opentelemetry.trace import SpanKind, Status, StatusCode

    tracer = trace.get_tracer("pyasynq", tracer_provider=tracer_provider)

    @app.before_enqueue
    def inject(task_type: str, headers: dict[str, str]) -> None:
        propagate.inject(headers)

    @app.middleware
    async def trace_task(ctx: TaskContext, call_next: Any) -> Any:
        parent = propagate.extract(ctx.headers)
        attributes: dict[str, str | int] = {
            "messaging.system": "asynq",
            "messaging.destination.name": ctx.queue,
            "messaging.message.id": ctx.id,
            "asynq.task.type": ctx.type,
            "asynq.task.retry_count": ctx.retry_count,
        }
        with tracer.start_as_current_span(
            f"process {ctx.type}", context=parent, kind=SpanKind.CONSUMER, attributes=attributes
        ) as span:
            try:
                return await call_next(ctx)
            except BaseException as exc:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
