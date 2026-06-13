"""``dtq`` command-line entry point: run the worker, scheduler, API, or all of them."""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from .bootstrap import import_task_modules
from .config import get_settings


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows event loop does not support add_signal_handler for SIGTERM.
            pass


async def _run_worker() -> None:
    from .worker import WorkerPool

    settings = get_settings()
    import_task_modules(settings)
    _maybe_start_metrics_server(settings)

    pool = WorkerPool(settings=settings)
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    await pool.start()
    print(f"[dtq] worker '{pool.worker_id}' started (concurrency={settings.worker_concurrency})")
    try:
        await stop.wait()
    finally:
        print("[dtq] worker shutting down…")
        await pool.stop()


async def _run_scheduler() -> None:
    from .scheduler import Scheduler

    settings = get_settings()
    import_task_modules(settings)

    scheduler = Scheduler(settings=settings)
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    serve = asyncio.create_task(scheduler.serve())
    print("[dtq] scheduler started (promote + reaper + cron)")
    try:
        await stop.wait()
    finally:
        print("[dtq] scheduler shutting down…")
        scheduler.stop()
        serve.cancel()
        try:
            await serve
        except asyncio.CancelledError:
            pass


def _run_api() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "dtq.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


async def _run_all() -> None:
    """Dev convenience: API + worker + scheduler in a single process/event loop."""
    import uvicorn

    from .scheduler import Scheduler
    from .worker import WorkerPool

    settings = get_settings()
    import_task_modules(settings)

    pool = WorkerPool(settings=settings)
    scheduler = Scheduler(settings=settings)

    config = uvicorn.Config(
        "dtq.api.app:app", host=settings.api_host, port=settings.api_port, log_level="info"
    )
    server = uvicorn.Server(config)

    await pool.start()
    print(f"[dtq] all-in-one: api on :{settings.api_port}, worker '{pool.worker_id}', scheduler")
    tasks = [
        asyncio.create_task(server.serve()),
        asyncio.create_task(scheduler.serve()),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        await pool.stop()


def _maybe_start_metrics_server(settings) -> None:
    if not (settings.metrics_enabled and settings.worker_metrics_port):
        return
    from prometheus_client import start_http_server

    try:
        start_http_server(settings.worker_metrics_port)
        print(f"[dtq] worker metrics on :{settings.worker_metrics_port}/metrics")
    except OSError as exc:
        print(f"[dtq] could not start metrics server: {exc}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dtq", description="Distributed Task Queue & Scheduler")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("worker", help="run a worker process")
    sub.add_parser("scheduler", help="run the scheduler (promote + reaper + cron)")
    sub.add_parser("api", help="run the FastAPI control plane")
    sub.add_parser("all", help="run api + worker + scheduler in one process (dev)")

    args = parser.parse_args(argv)

    if args.command == "api":
        _run_api()
        return 0

    runner = {
        "worker": _run_worker,
        "scheduler": _run_scheduler,
        "all": _run_all,
    }[args.command]
    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
