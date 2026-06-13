"""Scheduler service.

Runs three independent loops:
  * promote  — moves due delayed/retry tasks into the ready queue,
  * reaper   — requeues in-flight tasks whose lease expired (crash recovery),
  * cron     — fires periodic schedules; guarded by a Redis leader lock so that with
               multiple scheduler replicas, each schedule fires exactly once per tick.

Promote and reaper are atomic and idempotent, so every replica may run them safely.
"""
from __future__ import annotations

import asyncio
import time
import uuid

from . import cron, metrics
from .broker import Broker
from .config import Settings, get_settings
from .models import Schedule, Task
from .util import ms


class Scheduler:
    def __init__(self, broker: Broker | None = None, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.broker = broker or Broker(self.settings)
        self._token = uuid.uuid4().hex
        self._running = False
        self._lock_ttl_ms = max(2000, int(self.settings.scheduler_interval * 1000 * 6))

    async def serve(self) -> None:
        await self.broker.connect()
        self._running = True
        loops = [
            asyncio.create_task(self._promote_loop(), name="dtq-promote"),
            asyncio.create_task(self._reaper_loop(), name="dtq-reaper"),
            asyncio.create_task(self._cron_loop(), name="dtq-cron"),
            asyncio.create_task(self._prune_loop(), name="dtq-prune"),
        ]
        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            for t in loops:
                t.cancel()

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------ loops
    async def _promote_loop(self) -> None:
        while self._running:
            try:
                moved = await self.broker.promote_due()
            except Exception:
                moved = 0
            # If we filled a whole batch there may be more — drain without sleeping.
            if moved < self.settings.promote_batch:
                await asyncio.sleep(self.settings.scheduler_interval)

    async def _reaper_loop(self) -> None:
        while self._running:
            try:
                reaped = await self.broker.reap_expired()
            except Exception:
                reaped = 0
            if reaped < self.settings.reaper_batch:
                await asyncio.sleep(self.settings.reaper_interval)

    async def _prune_loop(self) -> None:
        while self._running:
            try:
                await self.broker.prune_workers()
            except Exception:
                pass
            await asyncio.sleep(self.settings.worker_ttl)

    async def _cron_loop(self) -> None:
        while self._running:
            try:
                if await self._is_leader():
                    await self._fire_due_schedules()
            except Exception:
                pass
            await asyncio.sleep(self.settings.scheduler_interval)

    # ------------------------------------------------------------------ helpers
    async def _is_leader(self) -> bool:
        if await self.broker.refresh_lock(self._token, self._lock_ttl_ms):
            return True
        return await self.broker.acquire_lock(self._token, self._lock_ttl_ms)

    async def _fire_due_schedules(self) -> None:
        t = time.time()
        for name in await self.broker.due_schedules(ms(t)):
            sched = await self.broker.get_schedule(name)
            if sched is None or not sched.enabled:
                await self.broker.redis.zrem(self.broker.keys.schedule_due, name)
                continue

            task = Task(
                name=sched.task,
                args=sched.args,
                kwargs=sched.kwargs,
                priority=sched.priority,
                max_retries=sched.max_retries,
                timeout=sched.timeout,
                schedule_name=sched.name,
            )
            await self.broker.enqueue(task)
            metrics.SCHEDULES_FIRED.labels(sched.name).inc()

            sched.last_run = t
            sched.last_task_id = task.id
            try:
                sched.next_run = cron.next_run_time(
                    cron=sched.cron, interval=sched.interval, base=t
                )
            except ValueError:
                sched.enabled = False  # malformed schedule — disable instead of looping
            await self.broker.put_schedule(sched)


def make_schedule(
    name: str,
    task: str,
    *,
    cron_expr: str | None = None,
    interval: float | None = None,
    args: list | None = None,
    kwargs: dict | None = None,
    priority: int = 100,
    max_retries: int | None = None,
    timeout: float | None = None,
    enabled: bool = True,
    base: float | None = None,
) -> Schedule:
    """Build a Schedule with its first ``next_run`` computed."""
    next_run = cron.next_run_time(cron=cron_expr, interval=interval, base=base) if enabled else None
    return Schedule(
        name=name,
        task=task,
        cron=cron_expr,
        interval=interval,
        args=list(args or []),
        kwargs=dict(kwargs or {}),
        priority=priority,
        max_retries=max_retries,
        timeout=timeout,
        enabled=enabled,
        next_run=next_run,
    )
