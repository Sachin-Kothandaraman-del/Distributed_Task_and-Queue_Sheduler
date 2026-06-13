"""Next-run computation for periodic schedules (cron expression or fixed interval)."""
from __future__ import annotations

import datetime as _dt
import time

from croniter import croniter


def is_valid_cron(expr: str) -> bool:
    return croniter.is_valid(expr)


def next_run_time(
    cron: str | None = None,
    interval: float | None = None,
    base: float | None = None,
) -> float:
    """Return the next fire time (epoch seconds) strictly after ``base``.

    Exactly one of ``cron`` or ``interval`` must be provided.
    """
    base = time.time() if base is None else base
    if cron:
        itr = croniter(cron, _dt.datetime.fromtimestamp(base))
        return itr.get_next(_dt.datetime).timestamp()
    if interval:
        if interval <= 0:
            raise ValueError("interval must be positive")
        return base + interval
    raise ValueError("a schedule requires either a cron expression or an interval")
