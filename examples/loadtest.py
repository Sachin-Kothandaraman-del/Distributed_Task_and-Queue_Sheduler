"""Throughput load generator.

Enqueues N tasks as fast as possible using pipelined producers, then prints the
achieved enqueue rate. Run workers separately to drain the queue.

Usage:
    python -m examples.loadtest --count 100000 --concurrency 16 --task add
"""
from __future__ import annotations

import argparse
import asyncio
import time

from dtq import Broker, Task


async def _produce(broker: Broker, sha: str, name: str, n: int) -> None:
    """Enqueue ``n`` tasks using one big pipeline per 1000 to maximise throughput.

    Buffers EVALSHA of the broker's atomic enqueue script so each task gets a tie-free
    FIFO sequence number, exactly like the production enqueue path.
    """
    keys = broker.keys
    batch = 1000
    t = time.time()
    remaining = n
    while remaining > 0:
        chunk = min(batch, remaining)
        pipe = broker.redis.pipeline(transaction=False)
        for _ in range(chunk):
            task = Task(name=name, args=[1, 2])
            task.enqueued_at = t
            pipe.evalsha(
                sha, 4,
                keys.ready, keys.task(task.id), keys.seq, keys.recent,
                task.id, task.priority, task.to_json(),
            )
        await pipe.execute()
        remaining -= chunk


async def main() -> None:
    parser = argparse.ArgumentParser(description="dtq load test")
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--task", default="add")
    args = parser.parse_args()

    broker = await Broker().connect()
    try:
        from dtq import lua

        sha = await broker.redis.script_load(lua.ENQUEUE_READY)
        per = args.count // args.concurrency
        start = time.perf_counter()
        await asyncio.gather(
            *[_produce(broker, sha, args.task, per) for _ in range(args.concurrency)]
        )
        elapsed = time.perf_counter() - start
        total = per * args.concurrency
        print(f"enqueued {total:,} tasks in {elapsed:.2f}s = {total / elapsed:,.0f} tasks/sec")
        print("queue stats:", await broker.stats())
    finally:
        await broker.close()


if __name__ == "__main__":
    asyncio.run(main())
