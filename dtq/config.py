"""Runtime configuration loaded from environment (prefix ``DTQ_``) or .env."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DTQ_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"
    namespace: str = "dtq"

    # --- Delivery / reliability ---
    # Seconds a task may stay in-flight before the reaper requeues it (crash recovery).
    visibility_timeout: float = 30.0
    default_max_retries: int = 5
    retry_backoff_base: float = 0.5          # seconds
    retry_backoff_factor: float = 2.0
    retry_backoff_max: float = 300.0         # cap on a single retry delay
    retry_jitter: float = 0.2                # +/- fraction of the computed delay

    # --- Worker ---
    worker_concurrency: int = 8              # starting in-process concurrency
    worker_min_concurrency: int = 1
    worker_max_concurrency: int = 64
    worker_poll_interval: float = 0.05       # poll delay when queue is empty
    worker_max_poll_interval: float = 0.5    # adaptive backoff ceiling
    task_default_timeout: float = 300.0
    heartbeat_interval: float = 5.0
    worker_ttl: float = 15.0                 # worker is "dead" after this w/o heartbeat
    worker_metrics_port: int = 9100          # embedded Prometheus endpoint (0 disables)

    # --- Autoscaler (per-process concurrency) ---
    autoscale_enabled: bool = True
    autoscale_interval: float = 2.0
    autoscale_tasks_per_worker: int = 50     # target backlog handled by each slot
    autoscale_scale_step: int = 4            # max slots added/removed per tick

    # --- Scheduler / reaper ---
    scheduler_interval: float = 0.5
    reaper_interval: float = 1.0
    promote_batch: int = 200
    reaper_batch: int = 200

    # --- Results ---
    result_ttl: float = 3600.0               # seconds to retain terminal task records

    # --- API ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # --- Metrics ---
    metrics_enabled: bool = True

    # Comma-separated modules that register task handlers on import.
    import_: str = Field(default="dtq.tasks", validation_alias="DTQ_IMPORT")

    @property
    def import_modules(self) -> list[str]:
        return [m.strip() for m in self.import_.split(",") if m.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
