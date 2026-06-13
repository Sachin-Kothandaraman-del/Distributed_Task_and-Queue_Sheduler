"""Import task-handler modules listed in configuration so they register on startup."""
from __future__ import annotations

import importlib

from .config import Settings


def import_task_modules(settings: Settings) -> list[str]:
    imported: list[str] = []
    for mod in settings.import_modules:
        try:
            importlib.import_module(mod)
            imported.append(mod)
        except Exception as exc:  # noqa: BLE001 — surface but don't crash the process
            print(f"[dtq] warning: could not import task module '{mod}': {exc}")
    return imported
