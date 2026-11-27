from __future__ import annotations

import asyncio
import json
from pathlib import Path
import aiosqlite

from app.models import (
    DeliveryAttempt,
    JobStatus,
    StatusCounts,
    WebhookJob,
)
from app.storage.repository import JobRepository


def _row_to_job(row: aiosqlite.Row) -> WebhookJob:
    return WebhookJob(
        id=row["id"],
        eventId=row["event_id"],
        idempotencyKey=row["idempotency_key"],
        destinationUrl=row["destination_url"],
        destinationDomain=row["destination_domain"],
        payload=json.loads(row["payload_json"]),
        status=row["status"],
        attemptCount=row["attempt_count"],
        maxRetries=row["max_retries"],
        nextRunAt=row["next_run_at"],
        lockedUntil=row["locked_until"],
        lastStatusCode=row["last_status_code"],
        lastError=row["last_error"],
        createdAt=row["created_at"],
        updatedAt=row["updated_at"],
        completedAt=row["completed_at"],
    )


def _row_to_attempt(row: aiosqlite.Row) -> DeliveryAttempt:
    return DeliveryAttempt(
        id=row["id"],
        jobId=row["job_id"],
        attemptNumber=row["attempt_number"],
        status=row["status"],
        responseCode=row["response_code"],
        durationMs=row["duration_ms"],
        errorMessage=row["error_message"],
        responseSnippet=row["response_snippet"],
        createdAt=row["created_at"],
    )


class SqliteJobRepository(JobRepository):
    engine = "sqlite"

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def init(self) -> None:
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        db = await aiosqlite.connect(self.db_path, isolation_level=None)
        db.row_factory = aiosqlite.Row
        await db.executescript(
            """
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS webhook_jobs (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                destination_url TEXT NOT NULL,
                destination_domain TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                next_run_at INTEGER NOT NULL,
                locked_until INTEGER,
                last_status_code INTEGER,
                last_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_ready
                ON webhook_jobs (status, next_run_at, created_at);

            CREATE INDEX IF NOT EXISTS idx_jobs_lease
                ON webhook_jobs (status, locked_until);

            CREATE INDEX IF NOT EXISTS idx_jobs_domain
                ON webhook_jobs (destination_domain, status);

            CREATE TABLE IF NOT EXISTS delivery_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                response_code INTEGER,
                duration_ms INTEGER NOT NULL,
                error_message TEXT,
                response_snippet TEXT,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (job_id) REFERENCES webhook_jobs(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_attempts_job
                ON delivery_attempts (job_id, attempt_number);
            """
        )
        self._db = db

    def _conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("SqliteJobRepository is not initialized")
        return self._db

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def ping(self) -> bool:
        try:
            async with self._conn().execute("SELECT 1") as cur:
                row = await cur.fetchone()
                return row is not None and row[0] == 1
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
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute(
                    "SELECT * FROM webhook_jobs WHERE idempotency_key = ?", (idempotency_key,)
                ) as cur:
                    existing = await cur.fetchone()
                    if existing is not None:
                        await db.execute("COMMIT")
                        return _row_to_job(existing), True

                await db.execute(
                    """
                    INSERT INTO webhook_jobs (
                        id, event_id, idempotency_key, destination_url, destination_domain,
                        payload_json, status, attempt_count, max_retries, next_run_at,
                        locked_until, last_status_code, last_error, created_at, updated_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, NULL, NULL, NULL, ?, ?, NULL)
                    """,
                    (
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
                    ),
                )
                async with db.execute("SELECT * FROM webhook_jobs WHERE id = ?", (job_id,)) as cur:
                    created = await cur.fetchone()
                await db.execute("COMMIT")
                assert created is not None
                return _row_to_job(created), False
            except Exception:
                await db.execute("ROLLBACK")
                raise

    async def claim_ready_jobs(self, limit: int, now_ms: int, lease_ms: int) -> list[WebhookJob]:
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute(
                    """
                    SELECT * FROM webhook_jobs
                    WHERE status IN ('queued', 'retrying') AND next_run_at <= ?
                    ORDER BY next_run_at ASC, created_at ASC
                    LIMIT ?
                    """,
                    (now_ms, limit),
                ) as cur:
                    rows = await cur.fetchall()

                if not rows:
                    await db.execute("COMMIT")
                    return []

                locked_until = now_ms + lease_ms
                ids = [r["id"] for r in rows]
                placeholders = ",".join("?" for _ in ids)
                await db.execute(
                    f"""
                    UPDATE webhook_jobs
                    SET status = 'processing', locked_until = ?, updated_at = ?
                    WHERE id IN ({placeholders})
                    """,
                    [locked_until, now_ms, *ids],
                )
                async with db.execute(
                    f"SELECT * FROM webhook_jobs WHERE id IN ({placeholders}) ORDER BY next_run_at ASC, created_at ASC",
                    ids,
                ) as cur:
                    updated_rows = await cur.fetchall()
                await db.execute("COMMIT")
                return [_row_to_job(r) for r in updated_rows]
            except Exception:
                await db.execute("ROLLBACK")
                raise

    async def reclaim_expired_leases(self, now_ms: int) -> int:
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                cur = await db.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = CASE WHEN attempt_count > 0 THEN 'retrying' ELSE 'queued' END,
                        locked_until = NULL,
                        next_run_at = ?,
                        updated_at = ?
                    WHERE status = 'processing' AND locked_until IS NOT NULL AND locked_until < ?
                    """,
                    (now_ms, now_ms, now_ms),
                )
                count = cur.rowcount or 0
                await db.execute("COMMIT")
                return count
            except Exception:
                await db.execute("ROLLBACK")
                raise

    async def defer_job(self, job_id: str, next_run_at: int, now_ms: int) -> None:
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = CASE WHEN attempt_count > 0 THEN 'retrying' ELSE 'queued' END,
                        locked_until = NULL,
                        next_run_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (next_run_at, now_ms, job_id),
                )
                await db.execute("COMMIT")
            except Exception:
                await db.execute("ROLLBACK")
                raise

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
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    """
                    INSERT INTO delivery_attempts (
                        job_id, attempt_number, status, response_code, duration_ms,
                        error_message, response_snippet, created_at
                    ) VALUES (?, ?, 'success', ?, ?, NULL, ?, ?)
                    """,
                    (job_id, attempt_number, response_code, duration_ms, response_snippet, now_ms),
                )
                await db.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = 'success',
                        attempt_count = ?,
                        locked_until = NULL,
                        last_status_code = ?,
                        last_error = NULL,
                        updated_at = ?,
                        completed_at = ?
                    WHERE id = ?
                    """,
                    (attempt_number, response_code, now_ms, now_ms, job_id),
                )
                await db.execute("COMMIT")
            except Exception:
                await db.execute("ROLLBACK")
                raise

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
        db = self._conn()
        next_status = "retrying" if will_retry else "failed"
        completed_at = None if will_retry else now_ms

        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                await db.execute(
                    """
                    INSERT INTO delivery_attempts (
                        job_id, attempt_number, status, response_code, duration_ms,
                        error_message, response_snippet, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        attempt_number,
                        next_status,
                        response_code,
                        duration_ms,
                        error_message,
                        response_snippet,
                        now_ms,
                    ),
                )
                await db.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = ?,
                        attempt_count = ?,
                        next_run_at = COALESCE(?, next_run_at),
                        locked_until = NULL,
                        last_status_code = ?,
                        last_error = ?,
                        updated_at = ?,
                        completed_at = ?
                    WHERE id = ?
                    """,
                    (
                        next_status,
                        attempt_number,
                        next_run_at,
                        response_code,
                        error_message,
                        now_ms,
                        completed_at,
                        job_id,
                    ),
                )
                await db.execute("COMMIT")
            except Exception:
                await db.execute("ROLLBACK")
                raise

    async def get_job_with_attempts(
        self, job_id: str
    ) -> tuple[WebhookJob, list[DeliveryAttempt]] | None:
        db = self._conn()
        async with db.execute("SELECT * FROM webhook_jobs WHERE id = ?", (job_id,)) as cur:
            job_row = await cur.fetchone()
        if job_row is None:
            return None

        async with db.execute(
            "SELECT * FROM delivery_attempts WHERE job_id = ? ORDER BY id ASC",
            (job_id,),
        ) as cur:
            attempt_rows = await cur.fetchall()

        return _row_to_job(job_row), [_row_to_attempt(r) for r in attempt_rows]

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        domain: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WebhookJob]:
        db = self._conn()
        clauses: list[str] = []
        params: list = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if domain:
            clauses.append("destination_domain = ?")
            params.append(domain)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        lim = min(max(limit, 1), 200)
        off = max(offset, 0)
        params.extend([lim, off])

        async with db.execute(
            f"SELECT * FROM webhook_jobs {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params,
        ) as cur:
            rows = await cur.fetchall()
        return [_row_to_job(r) for r in rows]

    async def replay_job(self, job_id: str, now_ms: int) -> WebhookJob | None:
        db = self._conn()
        async with self._write_lock:
            await db.execute("BEGIN IMMEDIATE")
            try:
                async with db.execute("SELECT * FROM webhook_jobs WHERE id = ?", (job_id,)) as cur:
                    existing = await cur.fetchone()
                if existing is None or existing["status"] == "processing":
                    await db.execute("COMMIT")
                    return None

                await db.execute(
                    """
                    UPDATE webhook_jobs
                    SET status = 'queued',
                        attempt_count = 0,
                        next_run_at = ?,
                        locked_until = NULL,
                        last_error = NULL,
                        updated_at = ?,
                        completed_at = NULL
                    WHERE id = ?
                    """,
                    (now_ms, now_ms, job_id),
                )
                async with db.execute("SELECT * FROM webhook_jobs WHERE id = ?", (job_id,)) as cur:
                    updated = await cur.fetchone()
                await db.execute("COMMIT")
                return _row_to_job(updated) if updated else None
            except Exception:
                await db.execute("ROLLBACK")
                raise

    async def get_status_counts(self) -> StatusCounts:
        db = self._conn()
        counts = StatusCounts()
        async with db.execute("SELECT status, COUNT(*) AS cnt FROM webhook_jobs GROUP BY status") as cur:
            rows = await cur.fetchall()
        for row in rows:
            st = row["status"]
            cnt = int(row["cnt"])
            setattr(counts, st, cnt)
            counts.total += cnt
        return counts
