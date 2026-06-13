"""Centralised Redis key naming so the data model lives in one place."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Keys:
    """All Redis keys used by the queue, namespaced by a common prefix.

    Data model
    ----------
    ready        ZSET  member=task_id  score=priority*1e13 + seq          (ZPOPMIN = highest priority, FIFO)
    seq          STR   global INCR counter; the tie-free FIFO component of ready scores
    delayed      ZSET  member=task_id  score=run_at_ms                    (delayed + retry-scheduled tasks)
    inflight     ZSET  member=task_id  score=visibility_deadline_ms       (tasks leased to a worker)
    dlq          LIST  task_id                                            (dead-lettered tasks)
    recent       LIST  task_id, newest first, capped at 1000              (dashboard activity feed)
    task:<id>    STR   JSON task record                                   (full task payload + status/result)
    workers      HASH  worker_id -> JSON heartbeat                        (worker registry / health)
    schedules    HASH  name -> JSON schedule                             (periodic job definitions)
    schedule:due ZSET  member=name     score=next_run_ms                  (due index for periodic jobs)
    lock:scheduler STR leader token                                       (single-firing cron election)
    """

    namespace: str = "dtq"

    @property
    def ready(self) -> str:
        return f"{self.namespace}:ready"

    @property
    def seq(self) -> str:
        return f"{self.namespace}:seq"

    @property
    def delayed(self) -> str:
        return f"{self.namespace}:delayed"

    @property
    def inflight(self) -> str:
        return f"{self.namespace}:inflight"

    @property
    def dlq(self) -> str:
        return f"{self.namespace}:dlq"

    @property
    def recent(self) -> str:
        return f"{self.namespace}:recent"

    @property
    def workers(self) -> str:
        return f"{self.namespace}:workers"

    @property
    def schedules(self) -> str:
        return f"{self.namespace}:schedules"

    @property
    def schedule_due(self) -> str:
        return f"{self.namespace}:schedule:due"

    @property
    def task_prefix(self) -> str:
        return f"{self.namespace}:task:"

    def task(self, task_id: str) -> str:
        return f"{self.task_prefix}{task_id}"

    @property
    def scheduler_lock(self) -> str:
        return f"{self.namespace}:lock:scheduler"
