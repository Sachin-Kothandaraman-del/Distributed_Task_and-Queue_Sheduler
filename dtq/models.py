"""Pydantic models for tasks and schedules (the wire + storage format)."""
from __future__ import annotations

import enum
import time
import uuid
from typing import Any

from pydantic import BaseModel, Field


def new_id() -> str:
    return uuid.uuid4().hex


def now() -> float:
    return time.time()


class TaskStatus(str, enum.Enum):
    PENDING = "pending"        # in the ready queue, waiting for a worker
    SCHEDULED = "scheduled"    # in the delayed set, waiting for its eta
    RUNNING = "running"        # leased to a worker (present in the in-flight set)
    SUCCESS = "success"        # completed successfully
    FAILED = "failed"          # an attempt failed; a retry is scheduled
    DEAD = "dead"              # retries exhausted; moved to the dead-letter queue
    CANCELLED = "cancelled"    # cancelled before it started running


class Priority(int, enum.Enum):
    """Lower value == higher priority (dequeued first)."""

    CRITICAL = 0
    HIGH = 20
    NORMAL = 100
    LOW = 200


class Task(BaseModel):
    id: str = Field(default_factory=new_id)
    name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)

    # 0 (highest) .. 255 (lowest). Kept small so priority*1e13 stays exact in a ZSET score.
    priority: int = Field(default=Priority.NORMAL.value, ge=0, le=255)
    # None = not set on this task; falls back to the handler's default, then the global one.
    max_retries: int | None = Field(default=None, ge=0)
    attempts: int = 0  # number of execution attempts started so far

    status: TaskStatus = TaskStatus.PENDING
    timeout: float | None = None
    eta: float | None = None  # epoch seconds; if set in the future the task is delayed

    created_at: float = Field(default_factory=now)
    enqueued_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None

    result: Any = None
    error: str | None = None
    traceback: str | None = None

    schedule_name: str | None = None  # set when produced by a periodic schedule

    # Per-task backoff overrides (fall back to global settings when unset).
    retry_backoff_base: float | None = None
    retry_backoff_max: float | None = None

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Task":
        return cls.model_validate_json(raw)


class Schedule(BaseModel):
    """A periodic or interval-based job definition stored in Redis."""

    name: str
    task: str
    cron: str | None = None          # e.g. "*/5 * * * *"
    interval: float | None = None    # seconds (alternative to cron)
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=Priority.NORMAL.value, ge=0, le=255)
    max_retries: int | None = Field(default=None, ge=0)
    timeout: float | None = None
    enabled: bool = True

    next_run: float | None = None
    last_run: float | None = None
    last_task_id: str | None = None

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Schedule":
        return cls.model_validate_json(raw)
