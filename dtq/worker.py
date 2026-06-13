"""Async worker pool.

A single dispatcher coroutine keeps up to ``target`` task executions running
concurrently (each execution is its own asyncio task). The autoscaler adjusts ``target``
based on backlog; a lease-renewal loop keeps long-running tasks from being reaped; a
heartbeat loop publishes liveness for the control plane.
"""
from __future__ import annotations

import asyncio
import math
import os
import socket
import time
import traceback
import uuid

from . import metrics
from .broker import Broker
from .config import Settings, get_settings
from .models import Task, TaskStatus
from .registry import TaskDef, TaskRegistry, registry as default_registry
from .util import backoff_delay, json_safe, ms


def resolve_max_retries(task: Task, definition: TaskDef | None, settings: Settings) -> int:
    """Retry budget precedence: per-task override > handler default > global setting."""
    if task.max_retries is not None:
        return task.max_retries
    if definition is not None and definition.max_retries is not None:
        return definition.max_retries
    return settings.default_max_retries


class WorkerPool:
    def __init__(
        self,
        broker: Broker | None = None,
        registry: TaskRegistry | None = None,
        settings: Settings | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.broker = broker or Broker(self.settings)
        self.registry = registry or default_registry
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"

        self._target = self.settings.worker_concurrency
        self._running = False
        self._dispatcher: asyncio.Task | None = None
        self._background: list[asyncio.Task] = []
        self._executing: set[asyncio.Task] = set()
        self._leases: set[str] = set()  # task ids currently being processed

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        await self.broker.connect()
        self._running = True
        self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="dtq-dispatch")
        self._background = [
            asyncio.create_task(self._heartbeat_loop(), name="dtq-heartbeat"),
            asyncio.create_task(self._lease_loop(), name="dtq-lease"),
            asyncio.create_task(self._autoscale_loop(), name="dtq-autoscale"),
        ]

    async def serve(self) -> None:
        """Start and block until stopped (used by the CLI)."""
        await self.start()
        assert self._dispatcher is not None
        try:
            await self._dispatcher
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for t in self._background:
            t.cancel()
        if self._dispatcher:
            self._dispatcher.cancel()
        # Let in-flight executions finish so their acks land (graceful drain).
        if self._executing:
            await asyncio.gather(*self._executing, return_exceptions=True)
        try:
            await self.broker.deregister_worker(self.worker_id)
        except Exception:
            pass

    # ------------------------------------------------------------------ dispatcher
    async def _dispatch_loop(self) -> None:
        empty_sleep = self.settings.worker_poll_interval
        try:
            while self._running:
                progressed = False
                while len(self._executing) < self._target and self._running:
                    task = await self.broker.dequeue()
                    if task is None:
                        break
                    progressed = True
                    exec_task = asyncio.create_task(self._execute(task))
                    self._executing.add(exec_task)
                    exec_task.add_done_callback(self._executing.discard)

                if len(self._executing) >= self._target and self._executing:
                    # At capacity — wait until a slot frees up.
                    await asyncio.wait(self._executing, return_when=asyncio.FIRST_COMPLETED)
                    empty_sleep = self.settings.worker_poll_interval
                elif not progressed:
                    # Capacity available but nothing to do — adaptive backoff.
                    await asyncio.sleep(empty_sleep)
                    empty_sleep = min(empty_sleep * 1.5, self.settings.worker_max_poll_interval)
                else:
                    empty_sleep = self.settings.worker_poll_interval
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------ execution
    async def _execute(self, task: Task) -> None:
        self._leases.add(task.id)
        metrics.TASKS_STARTED.labels(task.name).inc()
        if task.started_at and task.enqueued_at:
            metrics.TASK_QUEUE_LATENCY.labels(task.name).observe(
                max(0.0, task.started_at - task.enqueued_at)
            )

        definition = self.registry.get(task.name)
        if definition is None:
            await self._handle_failure(
                task, f"No handler registered for task '{task.name}'", None, retryable=False
            )
            self._leases.discard(task.id)
            return

        timeout = task.timeout or definition.timeout or self.settings.task_default_timeout
        started = time.perf_counter()
        try:
            if definition.is_async:
                coro = definition.func(*task.args, **task.kwargs)
            else:
                coro = asyncio.to_thread(definition.func, *task.args, **task.kwargs)
            result = await asyncio.wait_for(coro, timeout=timeout)
            await self.broker.ack(task, json_safe(result))
            metrics.TASKS_COMPLETED.labels(task.name, "success").inc()
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._handle_failure(task, f"Task timed out after {timeout}s", None)
        except Exception as exc:  # noqa: BLE001 — task handlers may raise anything
            await self._handle_failure(task, f"{type(exc).__name__}: {exc}", traceback.format_exc())
        finally:
            metrics.TASK_DURATION.labels(task.name).observe(time.perf_counter() - started)
            self._leases.discard(task.id)

    async def _handle_failure(
        self, task: Task, error: str, tb: str | None, retryable: bool = True
    ) -> None:
        definition = self.registry.get(task.name)
        max_retries = resolve_max_retries(task, definition, self.settings)

        retries_used = task.attempts - 1  # the just-failed attempt already counted
        if retryable and retries_used < max_retries:
            delay = backoff_delay(
                attempt=task.attempts,
                base=task.retry_backoff_base or self.settings.retry_backoff_base,
                factor=self.settings.retry_backoff_factor,
                max_delay=task.retry_backoff_max or self.settings.retry_backoff_max,
                jitter=self.settings.retry_jitter,
            )
            await self.broker.retry(task, error, tb, delay)
            metrics.TASKS_RETRIED.labels(task.name).inc()
            metrics.TASKS_COMPLETED.labels(task.name, "retry").inc()
        else:
            await self.broker.dead_letter(task, error, tb)
            metrics.TASKS_DEAD.labels(task.name).inc()
            metrics.TASKS_COMPLETED.labels(task.name, "dead").inc()

    # ------------------------------------------------------------------ background loops
    async def _heartbeat_loop(self) -> None:
        try:
            while self._running:
                try:
                    await self.broker.heartbeat(
                        self.worker_id,
                        {
                            "host": socket.gethostname(),
                            "pid": os.getpid(),
                            "concurrency": self._target,
                            "inflight": len(self._executing),
                        },
                    )
                    metrics.WORKER_CONCURRENCY.labels(self.worker_id).set(self._target)
                    metrics.WORKER_INFLIGHT.labels(self.worker_id).set(len(self._executing))
                except Exception:
                    pass
                await asyncio.sleep(self.settings.heartbeat_interval)
        except asyncio.CancelledError:
            pass

    async def _lease_loop(self) -> None:
        interval = max(1.0, self.settings.visibility_timeout / 3)
        try:
            while self._running:
                try:
                    if self._leases:
                        deadline = ms(time.time() + self.settings.visibility_timeout)
                        await self.broker.renew_leases(list(self._leases), deadline)
                except Exception:
                    pass
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    async def _autoscale_loop(self) -> None:
        if not self.settings.autoscale_enabled:
            return
        try:
            while self._running:
                try:
                    stats = await self.broker.stats()
                    backlog = stats["ready"] + len(self._executing)
                    per = max(1, self.settings.autoscale_tasks_per_worker)
                    desired = max(self.settings.worker_min_concurrency, math.ceil(backlog / per))
                    desired = min(self.settings.worker_max_concurrency, desired)
                    step = self.settings.autoscale_scale_step
                    if desired > self._target:
                        self._target = min(desired, self._target + step)
                    elif desired < self._target:
                        self._target = max(desired, self._target - step)
                except Exception:
                    pass
                await asyncio.sleep(self.settings.autoscale_interval)
        except asyncio.CancelledError:
            pass
