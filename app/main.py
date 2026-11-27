from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
import time
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.config import Settings, get_settings
from app.rate_limit.sliding_window import SlidingWindowRateLimiter
from app.rate_limit.token_bucket import DomainTokenBucketLimiter
from app.resilience.anomaly import DomainHealthTracker
from app.resilience.circuit_breaker import DomainCircuitBreaker
from app.routes.mock import router as mock_router
from app.routes.system import router as system_router
from app.routes.webhooks import router as webhooks_router
from app.storage import JobRepository, create_job_repository
from app.worker.dispatcher import DispatcherWorker

DASHBOARD_PATH = Path(__file__).parent / "ui" / "dashboard.html"


@dataclass
class AppContainer:
    settings: Settings
    repo: JobRepository
    worker: DispatcherWorker
    ingress_limiter: SlidingWindowRateLimiter
    mock_limiter: SlidingWindowRateLimiter
    started_at_ms: int
    flaky_counters: dict[str, int] = field(default_factory=dict)
    mock_receipts: deque = field(default_factory=lambda: deque(maxlen=50))


def create_app(
    settings: Settings | None = None,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    cfg = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        repo = create_job_repository(cfg)
        await repo.init()

        token_buckets = DomainTokenBucketLimiter(
            cfg.DOMAIN_BUCKET_CAPACITY,
            cfg.DOMAIN_BUCKET_REFILL_PER_SEC,
            cfg.DOMAIN_MAX_INFLIGHT,
        )
        circuit_breaker = DomainCircuitBreaker(
            cfg.CIRCUIT_FAILURE_THRESHOLD,
            cfg.CIRCUIT_COOLDOWN_MS,
        )
        health_tracker = DomainHealthTracker()
        ingress_limiter = SlidingWindowRateLimiter(
            cfg.INGRESS_WINDOW_MS,
            cfg.INGRESS_MAX_REQUESTS,
        )
        mock_limiter = SlidingWindowRateLimiter(
            cfg.MOCK_WINDOW_MS,
            cfg.MOCK_MAX_REQUESTS,
        )
        worker = DispatcherWorker(
            repo=repo,
            token_buckets=token_buckets,
            circuit_breaker=circuit_breaker,
            health_tracker=health_tracker,
            settings=cfg,
            http_client=http_client,
        )

        app.state.container = AppContainer(
            settings=cfg,
            repo=repo,
            worker=worker,
            ingress_limiter=ingress_limiter,
            mock_limiter=mock_limiter,
            started_at_ms=int(time.time() * 1000),
        )
        await worker.start()
        try:
            yield
        finally:
            await worker.stop()
            await repo.close()

    app = FastAPI(
        title="PulseHook — Distributed Webhook Dispatcher",
        version="2.0.0",
        description="Non-blocking distributed webhook engine with FastAPI, asyncio, SQLite WAL / PostgreSQL SKIP LOCKED, Token Bucket, Circuit Breaker, and Exponential Backoff.",
        lifespan=lifespan,
    )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard():
        return HTMLResponse(content=DASHBOARD_PATH.read_text(encoding="utf-8"))

    app.include_router(webhooks_router)
    app.include_router(mock_router)
    app.include_router(system_router)
    return app


app = create_app()
