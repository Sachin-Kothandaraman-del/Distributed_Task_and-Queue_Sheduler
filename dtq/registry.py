"""Task handler registry. Handlers are registered by name and looked up by workers."""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class TaskDef:
    name: str
    func: Callable
    is_async: bool
    max_retries: Optional[int] = None
    timeout: Optional[float] = None
    priority: Optional[int] = None


class TaskRegistry:
    """Maps task names to callables. Both sync and async handlers are supported."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskDef] = {}

    def task(
        self,
        _func: Optional[Callable] = None,
        *,
        name: Optional[str] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
        priority: Optional[int] = None,
    ):
        """Decorator/registrar. Usage::

        @registry.task
        def add(a, b): ...

        @registry.task(name="email.send", max_retries=10, timeout=30)
        async def send_email(to): ...
        """

        def decorate(func: Callable) -> Callable:
            tname = name or func.__name__
            self._tasks[tname] = TaskDef(
                name=tname,
                func=func,
                is_async=inspect.iscoroutinefunction(func),
                max_retries=max_retries,
                timeout=timeout,
                priority=priority,
            )
            func.task_name = tname  # type: ignore[attr-defined]
            return func

        if _func is not None:  # used as a bare @registry.task
            return decorate(_func)
        return decorate

    def register(self, func: Callable, name: Optional[str] = None, **kwargs) -> Callable:
        return self.task(func, name=name, **kwargs)

    def get(self, name: str) -> Optional[TaskDef]:
        return self._tasks.get(name)

    def names(self) -> list[str]:
        return sorted(self._tasks)

    def __contains__(self, name: str) -> bool:
        return name in self._tasks


# Global default registry used by the decorator and the workers.
registry = TaskRegistry()
task = registry.task
