"""Prometheus metrics. All series register on the default ``REGISTRY`` so an embedded
``start_http_server`` (workers) or the FastAPI ``/metrics`` route (control plane) expose them.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_DURATION_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
)
_LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
)

# --- Throughput counters ---
TASKS_ENQUEUED = Counter("dtq_tasks_enqueued_total", "Tasks enqueued", ["task"])
TASKS_STARTED = Counter("dtq_tasks_started_total", "Task executions started", ["task"])
TASKS_COMPLETED = Counter(
    "dtq_tasks_completed_total", "Task executions finished", ["task", "status"]
)
TASKS_RETRIED = Counter("dtq_tasks_retried_total", "Task retries scheduled", ["task"])
TASKS_DEAD = Counter("dtq_tasks_dead_total", "Tasks dead-lettered", ["task"])
TASKS_PROMOTED = Counter("dtq_tasks_promoted_total", "Delayed tasks promoted to ready")
TASKS_REAPED = Counter("dtq_tasks_reaped_total", "Expired in-flight tasks reclaimed")
SCHEDULES_FIRED = Counter(
    "dtq_schedules_fired_total", "Periodic schedule fires", ["schedule"]
)

# --- Latency histograms ---
TASK_DURATION = Histogram(
    "dtq_task_duration_seconds", "Task execution duration", ["task"],
    buckets=_DURATION_BUCKETS,
)
TASK_QUEUE_LATENCY = Histogram(
    "dtq_task_queue_latency_seconds", "Time from enqueue to first execution", ["task"],
    buckets=_LATENCY_BUCKETS,
)

# --- Gauges (cluster + per-process state) ---
QUEUE_DEPTH = Gauge("dtq_queue_depth", "Queue depth by sub-queue", ["queue"])
WORKERS_ACTIVE = Gauge("dtq_workers_active", "Workers seen alive (control plane view)")
WORKER_CONCURRENCY = Gauge(
    "dtq_worker_concurrency", "Target concurrency of a worker process", ["worker"]
)
WORKER_INFLIGHT = Gauge(
    "dtq_worker_inflight", "In-flight tasks in a worker process", ["worker"]
)
