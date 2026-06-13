"""Example task handlers.

Import this module (it is the default ``DTQ_IMPORT``) to register a handful of demo
tasks. Real applications register their own handlers the same way::

    from dtq import task

    @task(name="email.send", max_retries=10, timeout=30)
    async def send_email(to, subject): ...
"""
from __future__ import annotations

import asyncio
import random
import time

from .registry import task


@task
def add(a: float, b: float) -> float:
    return a + b


@task
def multiply(a: float, b: float) -> float:
    return a * b


@task
def fibonacci(n: int) -> int:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


@task(name="sleep")
async def sleep_task(seconds: float = 1.0) -> str:
    await asyncio.sleep(seconds)
    return f"slept {seconds}s"


@task(name="cpu.work")
def cpu_work(iterations: int = 100_000) -> int:
    total = 0
    for i in range(iterations):
        total += i * i
    return total


@task(name="email.send", max_retries=8, timeout=15)
async def send_email(to: str, subject: str = "Hello") -> dict:
    # Simulate a network call to an email provider.
    await asyncio.sleep(random.uniform(0.02, 0.1))
    return {"to": to, "subject": subject, "status": "sent", "at": time.time()}


@task(name="flaky", max_retries=5)
def flaky(success_rate: float = 0.5) -> str:
    """Fails randomly to demonstrate retries, backoff and dead-lettering."""
    if random.random() > success_rate:
        raise RuntimeError("flaky task failed; will retry with backoff")
    return "ok"


@task(name="heartbeat")
def heartbeat() -> str:
    """A trivial task handy for periodic (cron) schedule demos."""
    return f"beat @ {time.strftime('%H:%M:%S')}"
