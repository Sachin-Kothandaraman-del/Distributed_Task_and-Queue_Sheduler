"""Distributed Task Queue & Job Scheduler.

A fault-tolerant, Redis-backed distributed task queue supporting:
  * at-least-once delivery with acknowledgements and visibility timeouts,
  * exponential-backoff retries and dead-letter handling,
  * priority queues, delayed and periodic (cron) scheduling,
  * worker autoscaling, and a FastAPI + Prometheus control plane.
"""
from __future__ import annotations

from .config import Settings, get_settings
from .models import Priority, Schedule, Task, TaskStatus
from .registry import TaskRegistry, registry, task
from .broker import Broker
from .worker import WorkerPool
from .scheduler import Scheduler

__all__ = [
    "Settings",
    "get_settings",
    "Priority",
    "Schedule",
    "Task",
    "TaskStatus",
    "TaskRegistry",
    "registry",
    "task",
    "Broker",
    "WorkerPool",
    "Scheduler",
]

__version__ = "0.1.0"
