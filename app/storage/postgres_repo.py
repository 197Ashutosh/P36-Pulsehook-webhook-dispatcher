from __future__ import annotations

import json
import asyncpg

from app.models import DeliveryAttempt, JobStatus, StatusCounts, WebhookJob
from app.storage.repository import JobRepository


def _record_to_job(row: asyncpg.Record) -> WebhookJob:
    payload_val = row["payload_json"]
    payload = json.loads(payload_val) if isinstance(payload_val, str) else dict(payload_val)
    return WebhookJob(
        id=row["id"],
        eventId=row["event_id"],
        idempotencyKey=row["idempotency_key"],
        destinationUrl=row["destination_url"],
        destinationDomain=row["destination_domain"],
        payload=payload,
        status=row["status"],
        attemptCount=int(row["attempt_count"]),
        maxRetries=int(row["max_retries"]),
        nextRunAt=int(row["next_run_at"]),
        lockedUntil=int(row["locked_until"]) if row["locked_until"] is not None else None,
        lastStatusCode=row["last_status_code"],
        lastError=row["last_error"],
        createdAt=int(row["created_at"]),
        updatedAt=int(row["updated_at"]),
        completedAt=int(row["completed_at"]) if row["completed_at"] is not None else None,
    )


def _record_to_attempt(row: asyncpg.Record) -> DeliveryAttempt:
    return DeliveryAttempt(
        id=int(row["id"]),
        jobId=row["job_id"],
        attemptNumber=int(row["attempt_number"]),
        status=row["status"],
        responseCode=row["response_code"],
        durationMs=int(row["duration_ms"]),
        errorMessage=row["error_message"],
        responseSnippet=row["response_snippet"],
        createdAt=int(row["created_at"]),
    )


class PostgresJobRepository(JobRepository):
    engine = "postgres"

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def init(self) -> None:
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=12)
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS webhook_jobs (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    destination_url TEXT NOT NULL,
                    destination_domain TEXT NOT NULL,
                    payload_json JSONB NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    next_run_at BIGINT NOT NULL,
                    locked_until BIGINT,
                    last_status_code INTEGER,
                    last_error TEXT,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    completed_at BIGINT
                );

                CREATE INDEX IF NOT EXISTS idx_pg_jobs_ready
                    ON webhook_jobs (status, next_run_at, created_at);

                CREATE INDEX IF NOT EXISTS idx_pg_jobs_lease
                    ON webhook_jobs (status, locked_until);

                CREATE INDEX IF NOT EXISTS idx_pg_jobs_domain
                    ON webhook_jobs (destination_domain, status);

                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    id BIGSERIAL PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES webhook_jobs(id) ON DELETE CASCADE,
                    attempt_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    response_code INTEGER,
                    duration_ms INTEGER NOT NULL,
                    error_message TEXT,
                    response_snippet TEXT,
                    created_at BIGINT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_pg_attempts_job
                    ON delivery_attempts (job_id, attempt_number);
                """
            )

    def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("PostgresJobRepository is not initialized")
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> bool:
        try:
            async with self._get_pool().acquire() as conn:
                val = await conn.fetchval("SELECT 1")
                return val == 1
        except Exception:
            return False

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
    ) -> tuple[WebhookJob, bool]:
        async with self._get_pool().acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO webhook_jobs (
                        id, event_id, idempotency_key, destination_url, destination_domain,
                        payload_json, status, attempt_count, max_retries, next_run_at,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'queued', 0, $7, $8, $9, $10)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING *
                    """,
                    job_id,
                    event_id,
                    idempotency_key,
                    destination_url,
                    destination_domain,
                    json.dumps(payload),
                    max_retries,
                    now_ms,
                    now_ms,
                    now_ms,
                )
                if inserted is not None:
                    return _record_to_job(inserted), False

                existing = await conn.fetchrow(
                    "SELECT * FROM webhook_jobs WHERE idempotency_key = $1", idempotency_key
                )
                assert existing is not None
                return _record_to_job(existing), True

    async def claim_ready_jobs(self, limit: int, now_ms: int, lease_ms: int) -> list[WebhookJob]:
        async with self._get_pool().acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    WITH candidate AS (
                        SELECT id FROM webhook_jobs
                        WHERE status IN ('queued', 'retrying') AND next_run_at <= $1
                        ORDER BY next_run_at ASC, created_at ASC
                        LIMIT $2
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE webhook_jobs j
                    SET status = 'processing', locked_until = $3, updated_at = $1
                    FROM candidate
                    WHERE j.id = candidate.id
                    RETURNING j.*
                    """,
                    now_ms,
                    limit,
                    now_ms + lease_ms,
                )
                return [_record_to_job(r) for r in rows]

    async def reclaim_expired_leases(self, now_ms: int) -> int:
        async with self._get_pool().acquire() as conn:
            status = await conn.execute(
                """
                UPDATE webhook_jobs
                SET status = CASE WHEN attempt_count > 0 THEN 'retrying' ELSE 'queued' END,
                    locked_until = NULL,
                    next_run_at = $1,
                    updated_at = $1
                WHERE status = 'processing' AND locked_until IS NOT NULL AND locked_until < $1
                """,
                now_ms,
            )
            return int(status.split()[-1])

    async def defer_job(self, job_id: str, next_run_at: int, now_ms: int) -> None:
        async with self._get_pool().acquire() as conn:
            await conn.execute(
                """
                UPDATE webhook_jobs
                SET status = CASE WHEN attempt_count > 0 THEN 'retrying' ELSE 'queued' END,
                    locked_until = NULL,
                    next_run_at = $1,
                    updated_at = $2
                WHERE id = $3
                """,
                next_run_at,
                now_ms,
                job_id,
            )

    async def record_attempt_success(
        self,
        *,
        job_id: str,
        attempt_number: int,
        response_code: int,
        duration_ms: int,
        response_snippet: str | None,
        now_ms: int,
    ) -> None:
        async with self._get_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO delivery_attempts (
                        job_id, attempt_number, status, response_code, duration_ms,
                        error_message, response_snippet, created_at
                    ) VALUES ($1, $2, 'success', $3, $4, NULL, $5, $6)
                    """,
                    job_id,
                    attempt_number,
                    response_code,
                    duration_ms,
                    response_snippet,
                    now_ms,
                )
                await conn.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = 'success',
                        attempt_count = $1,
                        locked_until = NULL,
                        last_status_code = $2,
                        last_error = NULL,
                        updated_at = $3,
                        completed_at = $3
                    WHERE id = $4
                    """,
                    attempt_number,
                    response_code,
                    now_ms,
                    job_id,
                )

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
    ) -> None:
        next_status = "retrying" if will_retry else "failed"
        completed_at = None if will_retry else now_ms
        async with self._get_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO delivery_attempts (
                        job_id, attempt_number, status, response_code, duration_ms,
                        error_message, response_snippet, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    job_id,
                    attempt_number,
                    next_status,
                    response_code,
                    duration_ms,
                    error_message,
                    response_snippet,
                    now_ms,
                )
                await conn.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = $1,
                        attempt_count = $2,
                        next_run_at = COALESCE($3, next_run_at),
                        locked_until = NULL,
                        last_status_code = $4,
                        last_error = $5,
                        updated_at = $6,
                        completed_at = $7
                    WHERE id = $8
                    """,
                    next_status,
                    attempt_number,
                    next_run_at,
                    response_code,
                    error_message,
                    now_ms,
                    completed_at,
                    job_id,
                )

    async def get_job_with_attempts(
        self, job_id: str
    ) -> tuple[WebhookJob, list[DeliveryAttempt]] | None:
        async with self._get_pool().acquire() as conn:
            job_row = await conn.fetchrow("SELECT * FROM webhook_jobs WHERE id = $1", job_id)
            if job_row is None:
                return None
            attempt_rows = await conn.fetch(
                "SELECT * FROM delivery_attempts WHERE job_id = $1 ORDER BY id ASC",
                job_id,
            )
            return _record_to_job(job_row), [_record_to_attempt(r) for r in attempt_rows]

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        domain: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WebhookJob]:
        clauses: list[str] = []
        params: list = []
        if status:
            params.append(status)
            clauses.append(f"status = ${len(params)}")
        if domain:
            params.append(domain)
            clauses.append(f"destination_domain = ${len(params)}")

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([min(max(limit, 1), 200), max(offset, 0)])
        async with self._get_pool().acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM webhook_jobs {where} ORDER BY created_at DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}",
                *params,
            )
            return [_record_to_job(r) for r in rows]

    async def replay_job(self, job_id: str, now_ms: int) -> WebhookJob | None:
        async with self._get_pool().acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE webhook_jobs
                SET status = 'queued',
                    attempt_count = 0,
                    next_run_at = $1,
                    locked_until = NULL,
                    last_error = NULL,
                    updated_at = $1,
                    completed_at = NULL
                WHERE id = $2 AND status <> 'processing'
                RETURNING *
                """,
                now_ms,
                job_id,
            )
            return _record_to_job(row) if row is not None else None

    async def get_status_counts(self) -> StatusCounts:
        counts = StatusCounts()
        async with self._get_pool().acquire() as conn:
            rows = await conn.fetch("SELECT status, COUNT(*) AS cnt FROM webhook_jobs GROUP BY status")
        for row in rows:
            setattr(counts, row["status"], int(row["cnt"]))
            counts.total += int(row["cnt"])
        return counts
