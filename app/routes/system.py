from __future__ import annotations

import time
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["system"])


@router.get("/health")
async def health_check(request: Request):
    container = request.app.state.container
    db_healthy = await container.repo.ping()
    worker_stats = container.worker.get_stats()
    healthy = db_healthy and worker_stats["running"]
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "ok" if healthy else "degraded",
            "storageEngine": container.repo.engine,
            "dbHealthy": db_healthy,
            "workerRunning": worker_stats["running"],
            "uptimeSec": round((time.time() * 1000 - container.started_at_ms) / 1000),
        },
    )


@router.get("/api/metrics")
async def get_metrics(request: Request):
    container = request.app.state.container
    counts = await container.repo.get_status_counts()
    return {
        "timestamp": int(time.time() * 1000),
        "uptimeSec": round((time.time() * 1000 - container.started_at_ms) / 1000),
        "storageEngine": container.repo.engine,
        "jobs": counts.model_dump(),
        "worker": container.worker.get_stats(),
        "domains": [d.model_dump() for d in container.worker.get_domain_metrics()],
    }
