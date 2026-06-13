"""Small pure helpers (no I/O) — easy to unit test in isolation."""
from __future__ import annotations

import json
import random
from typing import Any


def backoff_delay(
    attempt: int,
    base: float,
    factor: float,
    max_delay: float,
    jitter: float = 0.0,
) -> float:
    """Exponential backoff for the ``attempt``-th retry (1 = first retry).

    delay = min(base * factor**(attempt-1), max_delay), then +/- jitter fraction.
    """
    attempt = max(1, attempt)
    delay = base * (factor ** (attempt - 1))
    delay = min(delay, max_delay)
    if jitter:
        spread = delay * jitter
        delay += random.uniform(-spread, spread)
    return max(0.0, delay)


def json_safe(value: Any) -> Any:
    """Return ``value`` unchanged if JSON-serialisable, else its ``repr``.

    Keeps task results storable without letting an exotic return value crash the worker.
    """
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def ms(t: float) -> int:
    """Epoch seconds -> integer milliseconds (used for ZSET scores)."""
    return int(t * 1000)
