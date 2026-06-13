"""Integration tests against a real Redis (skipped automatically if unavailable)."""
from __future__ import annotations

import asyncio

import pytest

from dtq.models import Priority, Task, TaskStatus
from dtq.scheduler import make_schedule

pytestmark = pytest.mark.asyncio


async def test_enqueue_dequeue_ack(broker):
    task = await broker.submit("add", args=[2, 3])
    assert (await broker.stats())["ready"] == 1

    claimed = await broker.dequeue()
    assert claimed is not None
    assert claimed.id == task.id
    assert claimed.attempts == 1
    stats = await broker.stats()
    assert stats["ready"] == 0 and stats["inflight"] == 1

    await broker.ack(claimed, result=5)
    assert (await broker.stats())["inflight"] == 0
    stored = await broker.get_task(task.id)
    assert stored.status == TaskStatus.SUCCESS
    assert stored.result == 5


async def test_priority_ordering(broker):
    await broker.submit("add", args=[1], priority=Priority.LOW.value)
    await broker.submit("add", args=[2], priority=Priority.CRITICAL.value)
    await broker.submit("add", args=[3], priority=Priority.NORMAL.value)

    first = await broker.dequeue()
    second = await broker.dequeue()
    third = await broker.dequeue()
    assert first.args == [2]   # CRITICAL
    assert second.args == [3]  # NORMAL
    assert third.args == [1]   # LOW


async def test_fifo_within_same_priority(broker):
    ids = []
    for i in range(5):
        t = await broker.submit("add", args=[i])
        ids.append(t.id)
    dequeued = [(await broker.dequeue()).id for _ in range(5)]
    assert dequeued == ids


async def test_delayed_task_promotes_when_due(broker):
    task = await broker.submit("heartbeat", delay=0.5)
    assert (await broker.stats())["delayed"] == 1
    assert await broker.dequeue() is None  # not ready yet

    await asyncio.sleep(0.6)
    moved = await broker.promote_due()
    assert moved == 1
    claimed = await broker.dequeue()
    assert claimed.id == task.id


async def test_visibility_timeout_reaps_crashed_task(broker):
    task = await broker.submit("add", args=[1])
    claimed = await broker.dequeue()
    assert claimed is not None
    # Simulate a crash: never ack. Wait past the 1s visibility timeout.
    await asyncio.sleep(1.1)
    reaped = await broker.reap_expired()
    assert reaped == 1
    again = await broker.dequeue()
    assert again.id == task.id  # redelivered (at-least-once)


async def test_retry_then_dead_letter(broker):
    task = Task(name="flaky", max_retries=1)
    await broker.enqueue(task)

    # Attempt 1 fails -> retry scheduled in the delayed set.
    claimed = await broker.dequeue()
    await broker.retry(claimed, "boom", None, delay=0.05)
    assert (await broker.stats())["delayed"] == 1

    await asyncio.sleep(0.1)
    await broker.promote_due()

    # Attempt 2 fails and retries are exhausted -> dead-letter.
    claimed = await broker.dequeue()
    assert claimed.attempts == 2
    await broker.dead_letter(claimed, "boom again", None)

    stats = await broker.stats()
    assert stats["dlq"] == 1
    dead = await broker.get_task(task.id)
    assert dead.status == TaskStatus.DEAD

    # Requeue from the DLQ.
    assert await broker.requeue_dlq(task.id) is True
    assert (await broker.stats())["dlq"] == 0
    assert (await broker.stats())["ready"] == 1


async def test_cancel_pending_task(broker):
    task = await broker.submit("add", args=[1], delay=10)
    assert await broker.cancel(task.id) is True
    assert (await broker.stats())["delayed"] == 0
    stored = await broker.get_task(task.id)
    assert stored.status == TaskStatus.CANCELLED
    # Cancelling something already gone returns False.
    assert await broker.cancel(task.id) is False


async def test_lease_renewal_prevents_reaping(broker):
    from dtq.util import ms
    import time as _time

    task = await broker.submit("sleep", args=[5])
    claimed = await broker.dequeue()
    assert claimed is not None
    # Keep renewing the lease past the original 1s visibility timeout.
    for _ in range(3):
        await asyncio.sleep(0.4)
        await broker.renew_leases([claimed.id], ms(_time.time() + broker.settings.visibility_timeout))
    reaped = await broker.reap_expired()
    assert reaped == 0  # still owned, not reclaimed


async def test_worker_registry_and_pruning(broker):
    await broker.heartbeat("w1", {"host": "h", "pid": 1})
    workers = await broker.list_workers()
    assert any(w["id"] == "w1" and w["alive"] for w in workers)
    await broker.deregister_worker("w1")
    assert all(w["id"] != "w1" for w in await broker.list_workers())


async def test_recent_tasks_feed(broker):
    a = await broker.submit("add", args=[1])
    b = await broker.submit("add", args=[2])
    c = await broker.submit("heartbeat", delay=30)  # the delayed path is recorded too

    recent = await broker.recent_tasks(10)
    assert [t.id for t in recent][:3] == [c.id, b.id, a.id]  # newest first

    # A DLQ requeue pushes the id again — the feed must deduplicate.
    claimed = await broker.dequeue()
    await broker.dead_letter(claimed, "boom", None)
    await broker.requeue_dlq(claimed.id)
    ids = [t.id for t in await broker.recent_tasks(10)]
    assert ids.count(claimed.id) == 1
    assert ids[0] == claimed.id  # and it moved to the front


async def test_schedule_crud(broker):
    sched = make_schedule("beat", "heartbeat", cron_expr="* * * * *")
    await broker.put_schedule(sched)
    got = await broker.get_schedule("beat")
    assert got is not None and got.task == "heartbeat"
    assert got.next_run is not None

    due = await broker.due_schedules(int(got.next_run * 1000) + 1)
    assert "beat" in due

    assert await broker.delete_schedule("beat") is True
    assert await broker.get_schedule("beat") is None
