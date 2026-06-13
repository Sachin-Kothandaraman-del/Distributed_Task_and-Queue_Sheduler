"""Pure unit tests (no Redis required)."""
from __future__ import annotations

import time

import pytest

from dtq.broker import PRIORITY_MULTIPLIER, Broker
from dtq.config import Settings
from dtq.cron import is_valid_cron, next_run_time
from dtq.models import Priority, Schedule, Task, TaskStatus
from dtq.registry import TaskDef, TaskRegistry
from dtq.util import backoff_delay, json_safe, ms
from dtq.worker import resolve_max_retries


def test_backoff_is_exponential_and_capped():
    base, factor, cap = 0.5, 2.0, 10.0
    d1 = backoff_delay(1, base, factor, cap, jitter=0.0)
    d2 = backoff_delay(2, base, factor, cap, jitter=0.0)
    d3 = backoff_delay(3, base, factor, cap, jitter=0.0)
    assert d1 == pytest.approx(0.5)
    assert d2 == pytest.approx(1.0)
    assert d3 == pytest.approx(2.0)
    # capped
    assert backoff_delay(20, base, factor, cap, jitter=0.0) == cap


def test_backoff_jitter_within_bounds():
    for _ in range(100):
        d = backoff_delay(3, 1.0, 2.0, 100.0, jitter=0.2)
        # nominal is 4.0, jitter +/-20% => [3.2, 4.8]
        assert 3.2 - 1e-9 <= d <= 4.8 + 1e-9


def test_json_safe_passthrough_and_fallback():
    assert json_safe({"a": 1}) == {"a": 1}
    assert json_safe([1, 2, 3]) == [1, 2, 3]
    obj = object()
    assert json_safe(obj) == repr(obj)


def test_priority_ordering_in_ready_score():
    b = Broker.__new__(Broker)  # no Redis needed for the pure scorer
    seq = 12345
    critical = b._ready_score(Priority.CRITICAL.value, seq)
    normal = b._ready_score(Priority.NORMAL.value, seq)
    low = b._ready_score(Priority.LOW.value, seq)
    # ZPOPMIN pops the smallest score first -> higher priority must have a smaller score
    assert critical < normal < low


def test_ready_score_is_fifo_within_priority():
    b = Broker.__new__(Broker)
    earlier = b._ready_score(Priority.NORMAL.value, 1)
    later = b._ready_score(Priority.NORMAL.value, 2)
    assert earlier < later
    # A lower priority always outranks a higher seq within a higher priority class.
    assert b._ready_score(Priority.HIGH.value, 10**12) < b._ready_score(
        Priority.NORMAL.value, 1
    )


def test_ready_score_exact_in_double_precision():
    b = Broker.__new__(Broker)
    # Worst case: lowest priority and a sequence counter at its 10^13 budget.
    score = b._ready_score(255, 10**13 - 1)
    # Must remain an exactly representable integer in a float64 ZSET score.
    assert score < 2 ** 53
    assert float(score) == score


def test_priority_multiplier_constant():
    assert PRIORITY_MULTIPLIER == 10 ** 13


def test_cron_validation_and_next_run_is_in_future():
    assert is_valid_cron("*/5 * * * *")
    assert not is_valid_cron("not a cron")
    base = time.time()
    nxt = next_run_time(cron="* * * * *", base=base)
    assert nxt > base
    assert nxt - base <= 61  # within the next minute


def test_interval_next_run():
    base = 1000.0
    assert next_run_time(interval=30, base=base) == 1030.0
    with pytest.raises(ValueError):
        next_run_time(base=base)  # neither cron nor interval


def test_task_json_roundtrip():
    t = Task(name="add", args=[1, 2], priority=Priority.HIGH.value, max_retries=3)
    restored = Task.from_json(t.to_json())
    assert restored.id == t.id
    assert restored.name == "add"
    assert restored.args == [1, 2]
    assert restored.priority == Priority.HIGH.value
    assert restored.status == TaskStatus.PENDING


def test_schedule_json_roundtrip():
    s = Schedule(name="beat", task="heartbeat", cron="* * * * *", next_run=123.0)
    restored = Schedule.from_json(s.to_json())
    assert restored.name == "beat"
    assert restored.cron == "* * * * *"
    assert restored.next_run == 123.0


def test_priority_bounds_validation():
    with pytest.raises(Exception):
        Task(name="x", priority=999)


def test_registry_sync_and_async_detection():
    reg = TaskRegistry()

    @reg.task
    def sync_handler():
        return 1

    @reg.task(name="async.handler", max_retries=2)
    async def async_handler():
        return 2

    assert "sync_handler" in reg
    assert reg.get("sync_handler").is_async is False
    assert reg.get("async.handler").is_async is True
    assert reg.get("async.handler").max_retries == 2


def test_ms_conversion():
    assert ms(1.5) == 1500
    assert ms(0) == 0


def test_max_retries_precedence():
    settings = Settings(default_max_retries=7)
    handler = TaskDef(name="x", func=lambda: None, is_async=False, max_retries=3)
    handler_unset = TaskDef(name="x", func=lambda: None, is_async=False)
    # Per-task override beats the handler default beats the global setting.
    assert resolve_max_retries(Task(name="x", max_retries=1), handler, settings) == 1
    assert resolve_max_retries(Task(name="x", max_retries=0), handler, settings) == 0
    assert resolve_max_retries(Task(name="x"), handler, settings) == 3
    assert resolve_max_retries(Task(name="x"), handler_unset, settings) == 7
    assert resolve_max_retries(Task(name="x"), None, settings) == 7
