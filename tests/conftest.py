"""Shared fixtures. Integration tests need a real Redis; they skip cleanly if absent.

Point them at a Redis with DTQ_TEST_REDIS_URL (defaults to redis://localhost:6379/15).
Each test run uses a unique namespace and flushes its own keys, so it is safe to run
against a shared Redis.
"""
from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from dtq.broker import Broker
from dtq.config import Settings

TEST_REDIS_URL = os.environ.get("DTQ_TEST_REDIS_URL", "redis://localhost:6379/15")


async def _redis_available(url: str) -> bool:
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=3)
        await client.ping()
        await client.aclose()
        return True
    except Exception:
        return False


@pytest_asyncio.fixture
async def broker():
    if not await _redis_available(TEST_REDIS_URL):
        pytest.skip(f"Redis not available at {TEST_REDIS_URL}")

    settings = Settings(
        redis_url=TEST_REDIS_URL,
        namespace=f"dtqtest:{uuid.uuid4().hex[:8]}",
        visibility_timeout=1.0,
        retry_backoff_base=0.05,
        retry_backoff_max=0.2,
        retry_jitter=0.0,
        autoscale_enabled=False,
    )
    b = Broker(settings)
    await b.connect()
    try:
        yield b
    finally:
        # Clean up only this test's namespace.
        keys = await b.redis.keys(f"{settings.namespace}:*")
        if keys:
            await b.redis.delete(*keys)
        await b.close()
