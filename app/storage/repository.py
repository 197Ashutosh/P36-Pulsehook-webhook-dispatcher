from __future__ import annotations

from abc import ABC, abstractmethod
from app.models import (
    DeliveryAttempt,
    JobStatus,
    StatusCounts,
    WebhookJob,
)


class JobRepository(ABC):
    engine: str

    @abstractmethod
    async def init(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def ping(self) -> bool: ...

    @abstractmethod
    async def enqueue(
        self,
        *,
        job_id: str,
        event_id: str,
        idempotency_key: str,
        destination_url: str,
        destination_domain: str,
        payload: dict,
        max_retries: int,
        now_ms: int,
    ) -> tuple[WebhookJob, bool]: ...

    @abstractmethod
    async def claim_ready_jobs(self, limit: int, now_ms: int, lease_ms: int) -> list[WebhookJob]: ...

    @abstractmethod
    async def reclaim_expired_leases(self, now_ms: int) -> int: ...

    @abstractmethod
    async def defer_job(self, job_id: str, next_run_at: int, now_ms: int) -> None: ...

    @abstractmethod
    async def record_attempt_success(
        self,
        *,
        job_id: str,
        attempt_number: int,
        response_code: int,
        duration_ms: int,
        response_snippet: str | None,
        now_ms: int,
    ) -> None: ...

    @abstractmethod
    async def record_attempt_failure(
        self,
        *,
        job_id: str,
        attempt_number: int,
        will_retry: bool,
        next_run_at: int | None,
        response_code: int | None,
        duration_ms: int,
        error_message: str,
        response_snippet: str | None,
        now_ms: int,
    ) -> None: ...

    @abstractmethod
    async def get_job_with_attempts(
        self, job_id: str
    ) -> tuple[WebhookJob, list[DeliveryAttempt]] | None: ...

    @abstractmethod
    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        domain: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WebhookJob]: ...

    @abstractmethod
    async def replay_job(self, job_id: str, now_ms: int) -> WebhookJob | None: ...

    @abstractmethod
    async def get_status_counts(self) -> StatusCounts: ...
