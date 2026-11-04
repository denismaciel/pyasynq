"""Prometheus metrics for queues, matching asynq's `x/metrics` names.

    from prometheus_client import REGISTRY, start_http_server
    REGISTRY.register(QueueMetricsCollector(Inspector(url)))
    start_http_server(9090)

Requires `prometheus_client`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from pyasynq.inspector import Inspector

log = logging.getLogger("pyasynq.metrics")

NAMESPACE = "asynq"
_STATES = ("active", "pending", "scheduled", "retry", "archived", "completed", "aggregating")


class QueueMetricsCollector:
    """Collects on scrape by reading every queue's stats from Redis."""

    def __init__(self, inspector: Inspector):
        self.inspector = inspector

    def describe(self) -> list[Any]:
        return []

    def collect(self) -> Iterator[Any]:
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

        enqueued = GaugeMetricFamily(
            f"{NAMESPACE}_tasks_enqueued_total",
            "Number of tasks enqueued; broken down by queue and state.",
            labels=["queue", "state"],
        )
        size = GaugeMetricFamily(f"{NAMESPACE}_queue_size", "Number of tasks in a queue", labels=["queue"])
        latency = GaugeMetricFamily(
            f"{NAMESPACE}_queue_latency_seconds",
            "Number of seconds the oldest pending task is waiting in pending state to be processed.",
            labels=["queue"],
        )
        memory = GaugeMetricFamily(
            f"{NAMESPACE}_queue_memory_usage_approx_bytes",
            "Number of memory used by a given queue (approximated number by sampling).",
            labels=["queue"],
        )
        processed = CounterMetricFamily(
            f"{NAMESPACE}_tasks_processed",
            "Number of tasks processed (both succeeded and failed); broken down by queue",
            labels=["queue"],
        )
        failed = CounterMetricFamily(
            f"{NAMESPACE}_tasks_failed", "Number of tasks failed; broken down by queue", labels=["queue"]
        )
        paused = GaugeMetricFamily(f"{NAMESPACE}_queue_paused_total", "Number of queues paused", labels=["queue"])
        try:
            infos = [self.inspector.get_queue_info(q) for q in self.inspector.queues()]
        except Exception:
            log.exception("failed to collect queue metrics")
            infos = []
        for info in infos:
            q = info.queue
            for state in _STATES:
                enqueued.add_metric([q, state], getattr(info, state))
            size.add_metric([q], info.size)
            latency.add_metric([q], info.latency.total_seconds())
            memory.add_metric([q], info.memory_usage)
            processed.add_metric([q], info.processed_total)
            failed.add_metric([q], info.failed_total)
            paused.add_metric([q], 1 if info.paused else 0)
        yield from (enqueued, size, latency, memory, processed, failed, paused)
