"""Redis-backed broker: the single source of truth for queue state.

Reliability model (at-least-once delivery):
  * ``dequeue`` atomically pops the highest-priority task and leases it into the
    ``inflight`` set with a visibility deadline (one Lua round-trip).
  * A worker must ``ack`` (success), ``retry`` (reschedule) or ``dead_letter`` it.
  * If a worker crashes, ``reap_expired`` returns the lease to the ready queue once the
    visibility deadline passes — so no task is silently lost (it may be re-run: at-least-once).
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

import redis.asyncio as aioredis

from . import lua, metrics
from .config import Settings, get_settings
from .keys import Keys
from .models import Priority, Schedule, Task, TaskStatus
from .util import ms

PRIORITY_MULTIPLIER = 10 ** 13  # keep in sync with the Lua scripts


class Broker:
    def __init__(self, settings: Settings | None = None, redis_client=None) -> None:
        self.settings = settings or get_settings()
        self.keys = Keys(self.settings.namespace)
        self._redis = redis_client
        self._scripts: dict[str, Any] = {}

    # ------------------------------------------------------------------ lifecycle
    @property
    def redis(self):
        if self._redis is None:
            raise RuntimeError("Broker is not connected; call await broker.connect()")
        return self._redis

    async def connect(self) -> "Broker":
        if self._redis is None:
            self._redis = aioredis.from_url(self.settings.redis_url, decode_responses=True)
        self._scripts = {
            "enqueue_ready": self._redis.register_script(lua.ENQUEUE_READY),
            "dequeue": self._redis.register_script(lua.DEQUEUE),
            "promote": self._redis.register_script(lua.PROMOTE),
            "reap": self._redis.register_script(lua.REAP),
            "cancel": self._redis.register_script(lua.CANCEL),
            "refresh_lock": self._redis.register_script(lua.REFRESH_LOCK),
        }
        await self._redis.ping()
        return self

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    # ------------------------------------------------------------------ producing
    def _ready_score(self, priority: int, seq: int) -> int:
        return priority * PRIORITY_MULTIPLIER + seq

    async def enqueue(self, task: Task) -> Task:
        """Persist a task and place it on the ready or delayed queue."""
        t = time.time()
        task.enqueued_at = t
        if task.eta and task.eta > t:
            task.status = TaskStatus.SCHEDULED
            pipe = self.redis.pipeline()
            pipe.set(self.keys.task(task.id), task.to_json())
            pipe.zadd(self.keys.delayed, {task.id: ms(task.eta)})
            pipe.lpush(self.keys.recent, task.id)
            pipe.ltrim(self.keys.recent, 0, 999)
            await pipe.execute()
        else:
            task.status = TaskStatus.PENDING
            # Atomic SET + INCR(seq) + ZADD: the sequence counter (not a timestamp)
            # provides the FIFO component, so same-millisecond enqueues never tie.
            await self._scripts["enqueue_ready"](
                keys=[self.keys.ready, self.keys.task(task.id), self.keys.seq, self.keys.recent],
                args=[task.id, task.priority, task.to_json()],
            )
        metrics.TASKS_ENQUEUED.labels(task.name).inc()
        return task

    async def submit(
        self,
        name: str,
        args: list | None = None,
        kwargs: dict | None = None,
        *,
        priority: int = Priority.NORMAL.value,
        delay: float | None = None,
        eta: float | None = None,
        max_retries: int | None = None,
        timeout: float | None = None,
        task_id: str | None = None,
    ) -> Task:
        """Convenience producer used by the API and example scripts."""
        task = Task(
            name=name,
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            priority=int(priority),
            # None means "not set here" — the worker falls back to the handler's
            # default and then the global setting (per-task wins when given).
            max_retries=max_retries,
            timeout=timeout,
        )
        if task_id:
            task.id = task_id
        if delay:
            task.eta = time.time() + delay
        elif eta:
            task.eta = eta
        return await self.enqueue(task)

    # ------------------------------------------------------------------ consuming
    async def dequeue(self) -> Optional[Task]:
        """Claim the next task and lease it. Returns None when the queue is empty."""
        deadline = ms(time.time() + self.settings.visibility_timeout)
        payload = await self._scripts["dequeue"](
            keys=[self.keys.ready, self.keys.inflight],
            args=[deadline, self.keys.task_prefix],
        )
        if not payload:
            return None
        task = Task.from_json(payload)
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()
        task.attempts += 1
        # Persist the running snapshot (the in-flight set remains the recovery source of truth).
        await self.redis.set(self.keys.task(task.id), task.to_json())
        return task

    async def ack(self, task: Task, result: Any = None) -> None:
        task.status = TaskStatus.SUCCESS
        task.finished_at = time.time()
        task.result = result
        task.error = None
        task.traceback = None
        pipe = self.redis.pipeline()
        pipe.zrem(self.keys.inflight, task.id)
        pipe.set(self.keys.task(task.id), task.to_json(), ex=int(self.settings.result_ttl))
        await pipe.execute()

    async def retry(self, task: Task, error: str, tb: str | None, delay: float) -> None:
        """Reschedule a failed attempt into the delayed set after ``delay`` seconds."""
        run_at = time.time() + delay
        task.status = TaskStatus.FAILED
        task.error = error
        task.traceback = tb
        task.eta = run_at
        pipe = self.redis.pipeline()
        pipe.zrem(self.keys.inflight, task.id)
        pipe.set(self.keys.task(task.id), task.to_json())
        pipe.zadd(self.keys.delayed, {task.id: ms(run_at)})
        await pipe.execute()

    async def dead_letter(self, task: Task, error: str, tb: str | None) -> None:
        task.status = TaskStatus.DEAD
        task.error = error
        task.traceback = tb
        task.finished_at = time.time()
        pipe = self.redis.pipeline()
        pipe.zrem(self.keys.inflight, task.id)
        # Retain dead tasks longer than ordinary results so they can be inspected/requeued.
        pipe.set(self.keys.task(task.id), task.to_json(), ex=int(self.settings.result_ttl * 24))
        pipe.lpush(self.keys.dlq, task.id)
        await pipe.execute()

    async def renew_leases(self, task_ids: list[str], deadline_ms: int) -> None:
        """Extend the visibility deadline for tasks still being processed (long jobs)."""
        if not task_ids:
            return
        pipe = self.redis.pipeline()
        for tid in task_ids:
            pipe.zadd(self.keys.inflight, {tid: deadline_ms}, xx=True)
        await pipe.execute()

    # ------------------------------------------------------------------ maintenance
    async def promote_due(self) -> int:
        moved = int(await self._scripts["promote"](
            keys=[self.keys.delayed, self.keys.ready, self.keys.seq],
            args=[ms(time.time()), self.settings.promote_batch, self.keys.task_prefix],
        ) or 0)
        if moved:
            metrics.TASKS_PROMOTED.inc(moved)
        return moved

    async def reap_expired(self) -> int:
        reaped = int(await self._scripts["reap"](
            keys=[self.keys.inflight, self.keys.ready, self.keys.seq],
            args=[ms(time.time()), self.settings.reaper_batch, self.keys.task_prefix],
        ) or 0)
        if reaped:
            metrics.TASKS_REAPED.inc(reaped)
        return reaped

    # ------------------------------------------------------------------ introspection
    async def stats(self) -> dict[str, int]:
        pipe = self.redis.pipeline()
        pipe.zcard(self.keys.ready)
        pipe.zcard(self.keys.delayed)
        pipe.zcard(self.keys.inflight)
        pipe.llen(self.keys.dlq)
        ready, delayed, inflight, dlq = await pipe.execute()
        return {"ready": ready, "delayed": delayed, "inflight": inflight, "dlq": dlq}

    async def recent_tasks(self, limit: int = 50) -> list[Task]:
        """Most recently enqueued tasks, newest first (the dashboard activity feed).

        The recent list may contain duplicates (a task requeued from the DLQ is pushed
        again) and ids whose records have expired — both are filtered out here.
        """
        ids = await self.redis.lrange(self.keys.recent, 0, max(0, limit * 2 - 1))
        seen: set[str] = set()
        ordered = [tid for tid in ids if not (tid in seen or seen.add(tid))]
        if not ordered:
            return []
        raws = await self.redis.mget([self.keys.task(tid) for tid in ordered])
        out: list[Task] = []
        for raw in raws:
            if raw:
                out.append(Task.from_json(raw))
            if len(out) >= limit:
                break
        return out

    async def get_task(self, task_id: str) -> Optional[Task]:
        raw = await self.redis.get(self.keys.task(task_id))
        if not raw:
            return None
        task = Task.from_json(raw)
        if task.status in (TaskStatus.PENDING, TaskStatus.SCHEDULED):
            if await self.redis.zscore(self.keys.inflight, task_id) is not None:
                task.status = TaskStatus.RUNNING
        return task

    async def cancel(self, task_id: str) -> bool:
        removed = await self._scripts["cancel"](
            keys=[self.keys.ready, self.keys.delayed], args=[task_id]
        )
        if not removed or int(removed) == 0:
            return False  # already running or finished — cannot cancel
        raw = await self.redis.get(self.keys.task(task_id))
        if raw:
            task = Task.from_json(raw)
            task.status = TaskStatus.CANCELLED
            task.finished_at = time.time()
            await self.redis.set(
                self.keys.task(task_id), task.to_json(), ex=int(self.settings.result_ttl)
            )
        return True

    # ------------------------------------------------------------------ dead-letter ops
    async def list_dlq(self, start: int = 0, end: int = 49) -> list[Task]:
        ids = await self.redis.lrange(self.keys.dlq, start, end)
        out: list[Task] = []
        for tid in ids:
            raw = await self.redis.get(self.keys.task(tid))
            if raw:
                out.append(Task.from_json(raw))
        return out

    async def requeue_dlq(self, task_id: str, reset_attempts: bool = True) -> bool:
        removed = await self.redis.lrem(self.keys.dlq, 1, task_id)
        if not removed:
            return False
        raw = await self.redis.get(self.keys.task(task_id))
        if not raw:
            return False
        task = Task.from_json(raw)
        if reset_attempts:
            task.attempts = 0
        task.status = TaskStatus.PENDING
        task.error = None
        task.traceback = None
        task.eta = None
        await self.enqueue(task)
        return True

    # ------------------------------------------------------------------ worker registry
    async def heartbeat(self, worker_id: str, info: dict) -> None:
        payload = {**info, "ts": time.time()}
        await self.redis.hset(self.keys.workers, worker_id, json.dumps(payload))

    async def deregister_worker(self, worker_id: str) -> None:
        await self.redis.hdel(self.keys.workers, worker_id)

    async def list_workers(self) -> list[dict]:
        raw = await self.redis.hgetall(self.keys.workers)
        out: list[dict] = []
        t = time.time()
        for wid, data in raw.items():
            try:
                info = json.loads(data)
            except (ValueError, TypeError):
                continue
            info["id"] = wid
            info["alive"] = (t - info.get("ts", 0)) <= self.settings.worker_ttl
            out.append(info)
        return out

    async def prune_workers(self) -> int:
        raw = await self.redis.hgetall(self.keys.workers)
        t = time.time()
        stale = []
        for wid, data in raw.items():
            try:
                ts = json.loads(data).get("ts", 0)
            except (ValueError, TypeError):
                ts = 0
            if t - ts > self.settings.worker_ttl * 2:
                stale.append(wid)
        if stale:
            await self.redis.hdel(self.keys.workers, *stale)
        return len(stale)

    # ------------------------------------------------------------------ schedules
    async def put_schedule(self, sched: Schedule) -> None:
        pipe = self.redis.pipeline()
        pipe.hset(self.keys.schedules, sched.name, sched.to_json())
        if sched.enabled and sched.next_run:
            pipe.zadd(self.keys.schedule_due, {sched.name: ms(sched.next_run)})
        else:
            pipe.zrem(self.keys.schedule_due, sched.name)
        await pipe.execute()

    async def get_schedule(self, name: str) -> Optional[Schedule]:
        raw = await self.redis.hget(self.keys.schedules, name)
        return Schedule.from_json(raw) if raw else None

    async def list_schedules(self) -> list[Schedule]:
        raw = await self.redis.hgetall(self.keys.schedules)
        return [Schedule.from_json(v) for v in raw.values()]

    async def delete_schedule(self, name: str) -> bool:
        pipe = self.redis.pipeline()
        pipe.hdel(self.keys.schedules, name)
        pipe.zrem(self.keys.schedule_due, name)
        res = await pipe.execute()
        return bool(res[0])

    async def due_schedules(self, now_ms: int) -> list[str]:
        return await self.redis.zrangebyscore(self.keys.schedule_due, "-inf", now_ms)

    # ------------------------------------------------------------------ leader lock
    async def acquire_lock(self, token: str, ttl_ms: int) -> bool:
        return bool(
            await self.redis.set(self.keys.scheduler_lock, token, nx=True, px=ttl_ms)
        )

    async def refresh_lock(self, token: str, ttl_ms: int) -> bool:
        return bool(
            await self._scripts["refresh_lock"](
                keys=[self.keys.scheduler_lock], args=[token, ttl_ms]
            )
        )
