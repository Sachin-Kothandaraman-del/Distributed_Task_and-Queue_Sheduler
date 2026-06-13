"""FastAPI control plane: submit/inspect tasks, manage schedules, monitor health, and
expose Prometheus metrics.
"""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import FileResponse, Response

STATIC_DIR = Path(__file__).parent / "static"

from .. import cron, metrics
from ..bootstrap import import_task_modules
from ..broker import Broker
from ..config import Settings, get_settings
from ..models import Priority, Schedule, Task
from ..registry import registry
from ..scheduler import make_schedule
from ..util import ms


# --------------------------------------------------------------------- schemas
class SubmitTaskRequest(BaseModel):
    name: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=Priority.NORMAL.value, ge=0, le=255)
    delay: Optional[float] = Field(default=None, description="Seconds to delay execution")
    eta: Optional[float] = Field(default=None, description="Absolute epoch start time")
    max_retries: Optional[int] = Field(default=None, ge=0)
    timeout: Optional[float] = None


class ScheduleRequest(BaseModel):
    name: str
    task: str
    cron: Optional[str] = None
    interval: Optional[float] = None
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=Priority.NORMAL.value, ge=0, le=255)
    max_retries: Optional[int] = Field(default=None, ge=0)
    timeout: Optional[float] = None
    enabled: bool = True


# --------------------------------------------------------------------- app factory
def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    broker = Broker(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        import_task_modules(settings)
        await broker.connect()
        updater = asyncio.create_task(_metrics_updater(broker, settings))
        app.state.broker = broker
        try:
            yield
        finally:
            updater.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await updater
            await broker.close()

    app = FastAPI(
        title="Distributed Task Queue — Control Plane",
        version="0.1.0",
        description="Submit and inspect tasks, manage periodic schedules, monitor workers.",
        lifespan=lifespan,
    )

    def get_broker() -> Broker:
        return broker

    # ----------------------------------------------------------------- dashboard
    @app.get("/", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC_DIR / "index.html")

    # ----------------------------------------------------------------- health/meta
    @app.get("/healthz", tags=["meta"])
    async def healthz(b: Broker = Depends(get_broker)):
        try:
            await b.redis.ping()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}")
        return {"status": "ok"}

    @app.get("/metrics", tags=["meta"])
    async def prometheus_metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/tasks/registered", tags=["meta"])
    async def registered_tasks():
        return {"tasks": registry.names()}

    @app.get("/stats", tags=["monitoring"])
    async def stats(b: Broker = Depends(get_broker)):
        queue = await b.stats()
        workers = await b.list_workers()
        alive = [w for w in workers if w.get("alive")]
        return {
            "queues": queue,
            "workers": {"total": len(workers), "alive": len(alive)},
            "registered_tasks": registry.names(),
        }

    # ----------------------------------------------------------------- tasks
    @app.post("/tasks", status_code=201, tags=["tasks"])
    async def submit_task(req: SubmitTaskRequest, b: Broker = Depends(get_broker)):
        if req.name not in registry:
            # Allow submitting unknown tasks (a worker elsewhere may know them) but warn.
            pass
        task = await b.submit(
            req.name,
            args=req.args,
            kwargs=req.kwargs,
            priority=req.priority,
            delay=req.delay,
            eta=req.eta,
            max_retries=req.max_retries,
            timeout=req.timeout,
        )
        return {"id": task.id, "status": task.status, "name": task.name}

    @app.get("/tasks/recent", tags=["tasks"])
    async def recent_tasks(
        limit: int = Query(50, ge=1, le=200), b: Broker = Depends(get_broker)
    ) -> list[Task]:
        return await b.recent_tasks(limit)

    @app.get("/tasks/{task_id}", tags=["tasks"])
    async def get_task(task_id: str, b: Broker = Depends(get_broker)) -> Task:
        task = await b.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task

    @app.delete("/tasks/{task_id}", tags=["tasks"])
    async def cancel_task(task_id: str, b: Broker = Depends(get_broker)):
        ok = await b.cancel(task_id)
        if not ok:
            raise HTTPException(
                status_code=409,
                detail="task could not be cancelled (already running or finished)",
            )
        return {"id": task_id, "status": "cancelled"}

    # ----------------------------------------------------------------- dead-letter
    @app.get("/dlq", tags=["dead-letter"])
    async def list_dlq(
        limit: int = Query(50, ge=1, le=500), b: Broker = Depends(get_broker)
    ) -> list[Task]:
        return await b.list_dlq(0, limit - 1)

    @app.post("/dlq/{task_id}/requeue", tags=["dead-letter"])
    async def requeue_dlq(
        task_id: str,
        reset_attempts: bool = Query(True),
        b: Broker = Depends(get_broker),
    ):
        ok = await b.requeue_dlq(task_id, reset_attempts=reset_attempts)
        if not ok:
            raise HTTPException(status_code=404, detail="task not in dead-letter queue")
        return {"id": task_id, "status": "requeued"}

    # ----------------------------------------------------------------- workers
    @app.get("/workers", tags=["monitoring"])
    async def list_workers(b: Broker = Depends(get_broker)):
        return await b.list_workers()

    # ----------------------------------------------------------------- schedules
    @app.get("/schedules", tags=["schedules"])
    async def list_schedules(b: Broker = Depends(get_broker)) -> list[Schedule]:
        return await b.list_schedules()

    @app.post("/schedules", status_code=201, tags=["schedules"])
    async def create_schedule(req: ScheduleRequest, b: Broker = Depends(get_broker)) -> Schedule:
        if not req.cron and not req.interval:
            raise HTTPException(status_code=400, detail="provide either 'cron' or 'interval'")
        if req.cron and not cron.is_valid_cron(req.cron):
            raise HTTPException(status_code=400, detail=f"invalid cron expression: {req.cron}")
        sched = make_schedule(
            req.name,
            req.task,
            cron_expr=req.cron,
            interval=req.interval,
            args=req.args,
            kwargs=req.kwargs,
            priority=req.priority,
            max_retries=req.max_retries,
            timeout=req.timeout,
            enabled=req.enabled,
        )
        await b.put_schedule(sched)
        return sched

    @app.get("/schedules/{name}", tags=["schedules"])
    async def get_schedule(name: str, b: Broker = Depends(get_broker)) -> Schedule:
        sched = await b.get_schedule(name)
        if sched is None:
            raise HTTPException(status_code=404, detail="schedule not found")
        return sched

    @app.delete("/schedules/{name}", tags=["schedules"])
    async def delete_schedule(name: str, b: Broker = Depends(get_broker)):
        ok = await b.delete_schedule(name)
        if not ok:
            raise HTTPException(status_code=404, detail="schedule not found")
        return {"name": name, "status": "deleted"}

    return app


async def _metrics_updater(broker: Broker, settings: Settings) -> None:
    """Periodically reflect cluster-wide queue/worker state into Prometheus gauges."""
    while True:
        try:
            stats = await broker.stats()
            for queue, depth in stats.items():
                metrics.QUEUE_DEPTH.labels(queue).set(depth)
            workers = await broker.list_workers()
            metrics.WORKERS_ACTIVE.set(sum(1 for w in workers if w.get("alive")))
        except Exception:
            pass
        await asyncio.sleep(2.0)


# Importable ASGI app for `uvicorn dtq.api.app:app`.
app = create_app()
