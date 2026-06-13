"""Enqueue a few example tasks and (optionally) register a periodic schedule.

Usage:
    python -m examples.producer
"""
from __future__ import annotations

import asyncio

from dtq import Broker, Priority
from dtq.scheduler import make_schedule


async def main() -> None:
    broker = await Broker().connect()
    try:
        # Immediate tasks at different priorities.
        await broker.submit("add", args=[2, 3], priority=Priority.HIGH.value)
        await broker.submit("multiply", args=[6, 7])
        await broker.submit("email.send", kwargs={"to": "a@example.com", "subject": "Hi"})

        # A delayed task (runs in 5 seconds).
        delayed = await broker.submit("heartbeat", delay=5)
        print(f"delayed task {delayed.id} will run in ~5s")

        # A flaky task to exercise retries / dead-lettering.
        flaky = await broker.submit("flaky", kwargs={"success_rate": 0.3}, max_retries=4)
        print(f"flaky task {flaky.id} enqueued")

        # A periodic schedule firing every minute.
        sched = make_schedule("beat-every-minute", "heartbeat", cron_expr="* * * * *")
        await broker.put_schedule(sched)
        print("registered cron schedule 'beat-every-minute' (every minute)")

        print("queue stats:", await broker.stats())
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
