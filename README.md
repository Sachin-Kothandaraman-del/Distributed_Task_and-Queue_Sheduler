# Distributed Task Queue & Job Scheduler

A fault-tolerant, Redis-backed distributed task queue with a broker, worker pool, and
scheduler. Built with **Python + asyncio + Redis + FastAPI + Prometheus + Docker**.

## Features

- **At-least-once delivery** — tasks are leased to workers with a visibility timeout and
  must be acknowledged. If a worker crashes, the task is automatically redelivered.
- **Exponential-backoff retries + dead-letter queue** — failed attempts are retried with
  jittered exponential backoff; once retries are exhausted, the task is dead-lettered for
  inspection / manual requeue.
- **Priority queues** — a single sorted set encodes `priority` and FIFO order, so the
  highest-priority task is always dequeued first (with FIFO within a priority).
- **Delayed & periodic (cron) jobs** — schedule a task for the future, or register a
  cron / interval schedule that fires repeatedly. Cron firing uses a Redis leader lock so
  each schedule fires once even with multiple scheduler replicas.
- **Worker autoscaling** — each worker process scales its own concurrency between a min and
  max based on backlog; horizontal scale is just more worker processes/containers.
- **FastAPI control plane** — submit/inspect/cancel tasks, manage schedules, browse the
  dead-letter queue, and monitor worker health.
- **Web dashboard** — a zero-dependency single-page UI served at `/`: live queue stats with
  a depth sparkline, a recent-task feed with per-task detail (args, result, error,
  traceback), a submit form, schedule management, DLQ requeue and worker liveness.
- **Prometheus metrics + Grafana** — queue depth, throughput, task duration/latency
  histograms, retries, dead-letters and worker health, with a ready-made dashboard.

## Architecture

```
                         ┌──────────────────────────┐
        submit task      │   FastAPI control plane   │   /metrics ─► Prometheus ─► Grafana
   ───────────────────►  │  (producer + monitoring)  │
                         └─────────────┬─────────────┘
                                       │ enqueue
                                       ▼
   ┌───────────────────────────────  REDIS  ───────────────────────────────┐
   │  ready (ZSET, priority+FIFO)   delayed (ZSET)   inflight (ZSET, lease) │
   │  dlq (LIST)   task:<id> (JSON)   schedules (HASH)   workers (HASH)     │
   └───▲──────────────▲───────────────────▲───────────────────▲────────────┘
       │ dequeue/ack  │ promote_due        │ reap_expired      │ heartbeat
       │              │                    │                   │
 ┌─────┴─────┐   ┌────┴───────────────────┴────┐         ┌─────┴───────────┐
 │  Worker   │   │         Scheduler           │         │  Worker         │
 │  pool     │   │  promote · reaper · cron    │   ...   │  pool           │
 │ (asyncio) │   │     (leader-elected cron)   │         │ (autoscaling)   │
 └───────────┘   └─────────────────────────────┘         └─────────────────┘
```

### Redis data model

| Key | Type | Purpose |
| --- | --- | --- |
| `dtq:ready` | ZSET | Ready tasks. score = `priority*1e13 + seq`; `ZPOPMIN` = highest priority, FIFO within a priority. |
| `dtq:seq` | STRING | Global `INCR` counter — the tie-free FIFO component of ready scores. |
| `dtq:delayed` | ZSET | Delayed + retry-scheduled tasks. score = `run_at_ms`. |
| `dtq:inflight` | ZSET | Leased tasks. score = `visibility_deadline_ms`; the reaper reclaims expired leases. |
| `dtq:dlq` | LIST | Dead-lettered task ids. |
| `dtq:task:<id>` | STRING | Full task record (JSON) with status / result. |
| `dtq:schedules` | HASH | Periodic schedule definitions. |
| `dtq:schedule:due` | ZSET | Due index for schedules (score = `next_run_ms`). |
| `dtq:workers` | HASH | Worker heartbeats for liveness/health. |
| `dtq:lock:scheduler` | STRING | Leader token so cron fires exactly once per tick. |

Atomic, race-free transitions (`dequeue`, `promote`, `reap`, `cancel`, lock refresh) are
implemented as server-side **Lua scripts** in [`dtq/lua.py`](dtq/lua.py).

## Quickstart (Docker — full stack)

```bash
git clone https://github.com/Sachin-Kothandaraman-del/Distributed_Task_and-Queue_Sheduler.git
cd Distributed_Task_and-Queue_Sheduler
docker compose up --build
```

This starts Redis, the API, the scheduler, two workers, Prometheus and Grafana.

- **Dashboard UI: http://localhost:8000**
- Control plane / OpenAPI docs: http://localhost:8000/docs
- Prometheus: http://localhost:9090
- Grafana (anonymous admin): http://localhost:3000 → dashboard **Distributed Task Queue**

Submit a task and watch it run:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "content-type: application/json" \
  -d '{"name":"email.send","kwargs":{"to":"a@example.com"},"priority":20}'

# inspect it
curl http://localhost:8000/tasks/<id>
```

## Quickstart (local, no Docker)

You need a running Redis. Then:

```bash
pip install -r requirements.txt
pip install -e .

# Terminal 1 — everything in one process (api + worker + scheduler)
python -m dtq all

# or run them separately:
python -m dtq api          # control plane on :8000
python -m dtq worker       # a worker process (metrics on :9100)
python -m dtq scheduler    # promote + reaper + cron

# Terminal 2 — enqueue example work and a cron schedule
python -m examples.producer
```

On Windows, [`scripts/quickstart.ps1`](scripts/quickstart.ps1) starts a Redis container and
the all-in-one process for you.

## API reference

| Method & path | Description |
| --- | --- |
| `POST /tasks` | Submit a task (`name`, `args`, `kwargs`, `priority`, `delay`/`eta`, `max_retries`, `timeout`). |
| `GET /tasks/{id}` | Fetch a task's status and result. |
| `DELETE /tasks/{id}` | Cancel a task that has not started yet. |
| `GET /tasks/recent` | The most recently enqueued tasks, newest first (dashboard feed). |
| `GET /tasks/registered` | List task handlers known to the control plane. |
| `GET /` | Web dashboard (single-file UI, no build step). |
| `GET /dlq` · `POST /dlq/{id}/requeue` | Browse / requeue dead-lettered tasks. |
| `GET /schedules` · `POST /schedules` · `GET/DELETE /schedules/{name}` | Manage periodic schedules. |
| `GET /workers` | Worker registry with liveness. |
| `GET /stats` | Queue depths + worker summary. |
| `GET /healthz` · `GET /metrics` | Health check · Prometheus exposition. |

Create a cron schedule:

```bash
curl -X POST http://localhost:8000/schedules \
  -H "content-type: application/json" \
  -d '{"name":"nightly-report","task":"heartbeat","cron":"0 2 * * *"}'
```

## Defining your own tasks

```python
from dtq import task

@task                                   # name defaults to the function name
def resize_image(path): ...

@task(name="email.send", max_retries=10, timeout=30)
async def send_email(to, subject): ...  # async handlers run on the event loop
                                        # sync handlers run in a thread pool
```

Tell the workers/API to import your module via `DTQ_IMPORT=myapp.tasks` (comma-separated).

## Throughput

A single sorted-set `ZPOPMIN` dequeue is one Redis round-trip; enqueues are pipelined
batches of the atomic enqueue script. Throughput scales horizontally by adding worker
processes/containers. Measured on a laptop against the Docker stack:

```bash
python -m examples.loadtest --count 100000 --concurrency 16
# enqueued 100,000 tasks in 1.72s = 58,051 tasks/sec (single producer process)
# two worker containers drained the 100K backlog in ~24s (~4K executions/sec
# with full at-least-once lease/ack accounting) — add workers to scale further
```

## Configuration

All settings are environment variables prefixed `DTQ_` (see [`.env.example`](.env.example)).
Key knobs: `DTQ_VISIBILITY_TIMEOUT`, `DTQ_DEFAULT_MAX_RETRIES`, `DTQ_RETRY_BACKOFF_*`,
`DTQ_WORKER_CONCURRENCY` / `DTQ_WORKER_MAX_CONCURRENCY`, `DTQ_AUTOSCALE_*`, `DTQ_IMPORT`.

## Tests

```bash
pip install -r requirements-dev.txt
pytest                       # unit tests always run; integration tests need Redis
# point integration tests at a Redis:
DTQ_TEST_REDIS_URL=redis://localhost:6379/15 pytest
```

## Project layout

```
dtq/
  broker.py      Redis broker (Lua-atomic dequeue/promote/reap, retries, DLQ, schedules)
  worker.py      Async worker pool with autoscaling, lease renewal, heartbeats
  scheduler.py   promote + reaper + leader-elected cron loops
  lua.py         Atomic server-side scripts
  models.py      Task / Schedule / enums (pydantic)
  registry.py    Task handler registry (@task)
  metrics.py     Prometheus metric definitions
  api/app.py     FastAPI control plane
  api/static/    Single-file web dashboard served at /
  cli.py         `dtq worker|scheduler|api|all`
  tasks.py       Example task handlers
monitoring/      Prometheus + Grafana provisioning and dashboard
examples/        producer + load test
tests/           unit + integration tests
```

## Design notes

- **Delivery guarantee.** At-least-once. Handlers should be idempotent — the reaper may
  redeliver a task whose worker died after starting but before acknowledging.
- **Long-running tasks.** Workers renew their lease (`renew_leases`) at ~⅓ of the visibility
  timeout so genuinely long tasks are not reaped prematurely.
- **Priority precision & FIFO.** Scores stay below `2^53`, so `priority*1e13 + seq` is
  exact in a ZSET's float64 score (priority is capped to 0–255). The FIFO component is a
  global `INCR` sequence rather than a timestamp, so concurrent enqueues in the same
  millisecond can never tie (ties would fall back to lexicographic member order).
- **Cron correctness.** `promote`/`reaper` are idempotent and safe to run on every replica;
  only the lock leader fires cron schedules, preventing duplicate periodic runs.
