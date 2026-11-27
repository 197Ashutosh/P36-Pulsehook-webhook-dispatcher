from __future__ import annotations

import math
import time
import uuid
from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status

from app.models import DispatchRequest, DispatchResponse, JobDetailResponse, JobStatus
from app.security.signer import validate_destination_url

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


@router.post("/dispatch", status_code=status.HTTP_202_ACCEPTED, response_model=DispatchResponse)
async def dispatch_webhook(
    body: DispatchRequest,
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    container = request.app.state.container
    client_ip = request.client.host if request.client else "global"
    rate = container.ingress_limiter.consume(f"ingress:{client_ip}")

    response.headers["X-RateLimit-Limit"] = str(rate.limit)
    response.headers["X-RateLimit-Remaining"] = str(rate.remaining)
    response.headers["X-RateLimit-Reset"] = str(math.ceil(rate.reset_at_ms / 1000))

    if not rate.allowed:
        retry_sec = max(1, math.ceil(rate.retry_after_ms / 1000))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Too Many Requests",
                "message": "Ingestion sliding-window rate limit exceeded",
                "retryAfterMs": rate.retry_after_ms,
            },
            headers={"Retry-After": str(retry_sec)},
        )

    try:
        destination = validate_destination_url(
            body.destinationUrl, container.settings.ALLOW_PRIVATE_URLS
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Invalid destinationUrl", "message": str(exc)},
        ) from exc

    idem = (
        idempotency_key.strip()
        if idempotency_key and idempotency_key.strip()
        else f"{body.eventId}::{destination.normalized_url}"
    )

    job, deduplicated = await container.repo.enqueue(
        job_id=f"job_{uuid.uuid4().hex[:16]}",
        event_id=body.eventId,
        idempotency_key=idem,
        destination_url=destination.normalized_url,
        destination_domain=destination.domain,
        payload=body.payload,
        max_retries=(
            body.maxRetries if body.maxRetries is not None else container.settings.MAX_RETRIES
        ),
        now_ms=int(time.time() * 1000),
    )

    if not deduplicated:
        container.worker.nudge()

    return DispatchResponse(
        status="queued",
        jobId=job.id,
        eventId=job.eventId,
        deduplicated=deduplicated,
        currentJobStatus=job.status,
    )


@router.get("/jobs")
async def list_jobs(
    request: Request,
    job_status: JobStatus | None = Query(default=None, alias="status"),
    domain: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    container = request.app.state.container
    jobs = await container.repo.list_jobs(
        status=job_status, domain=domain, limit=limit, offset=offset
    )
    return {"count": len(jobs), "jobs": jobs}


@router.get("/jobs/{job_id}", response_model=JobDetailResponse)
async def get_job(job_id: str, request: Request):
    container = request.app.state.container
    record = await container.repo.get_job_with_attempts(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail={"error": "Job not found", "jobId": job_id})
    job, attempts = record
    return JobDetailResponse(job=job, attempts=attempts)


@router.post("/jobs/{job_id}/replay", status_code=status.HTTP_202_ACCEPTED)
async def replay_job(job_id: str, request: Request):
    container = request.app.state.container
    replayed = await container.repo.replay_job(job_id, int(time.time() * 1000))
    if replayed is None:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "Cannot replay job",
                "message": "Job does not exist or is currently processing",
            },
        )
    container.worker.nudge()
    return {"status": "queued", "jobId": replayed.id, "replayed": True}
