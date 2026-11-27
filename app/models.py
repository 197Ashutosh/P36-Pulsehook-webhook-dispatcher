from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field

JobStatus = Literal["queued", "processing", "retrying", "success", "failed"]
AttemptStatus = Literal["success", "retrying", "failed"]
CircuitState = Literal["CLOSED", "OPEN", "HALF_OPEN"]


class DispatchRequest(BaseModel):
    destinationUrl: str = Field(..., min_length=1, description="Target webhook endpoint URL")
    eventId: str = Field(..., min_length=1, max_length=128, description="Unique upstream event ID")
    payload: dict[str, Any] = Field(..., description="JSON payload to deliver")
    maxRetries: int | None = Field(default=None, ge=0, le=5)


class DispatchResponse(BaseModel):
    status: Literal["queued"] = "queued"
    jobId: str
    eventId: str
    deduplicated: bool
    currentJobStatus: JobStatus


class WebhookJob(BaseModel):
    id: str
    eventId: str
    idempotencyKey: str
    destinationUrl: str
    destinationDomain: str
    payload: dict[str, Any]
    status: JobStatus
    attemptCount: int
    maxRetries: int
    nextRunAt: int
    lockedUntil: int | None = None
    lastStatusCode: int | None = None
    lastError: str | None = None
    createdAt: int
    updatedAt: int
    completedAt: int | None = None


class DeliveryAttempt(BaseModel):
    id: int
    jobId: str
    attemptNumber: int
    status: AttemptStatus
    responseCode: int | None = None
    durationMs: int
    errorMessage: str | None = None
    responseSnippet: str | None = None
    createdAt: int


class JobDetailResponse(BaseModel):
    job: WebhookJob
    attempts: list[DeliveryAttempt]


class StatusCounts(BaseModel):
    queued: int = 0
    processing: int = 0
    retrying: int = 0
    success: int = 0
    failed: int = 0
    total: int = 0


class DomainRateMetrics(BaseModel):
    domain: str
    availableTokens: float
    capacity: float
    refillPerSec: float
    inflight: int
    maxInflight: int
    deferredCount: int
    circuitState: CircuitState
    consecutiveFailures: int
    nextProbeAt: int | None = None
    ewmaLatencyMs: float | None = None
    healthScore: int = 100
    diagnostic: str = "Healthy"
