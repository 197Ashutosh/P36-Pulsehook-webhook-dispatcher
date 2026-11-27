from __future__ import annotations

import asyncio
import json
import random
import time
from email.utils import parsedate_to_datetime
import httpx

from app.config import Settings
from app.models import DomainRateMetrics, WebhookJob
from app.rate_limit.token_bucket import DomainTokenBucketLimiter
from app.resilience.anomaly import DomainHealthTracker
from app.resilience.circuit_breaker import DomainCircuitBreaker
from app.security.signer import sign_webhook_payload
from app.storage.repository import JobRepository


def _parse_retry_after_ms(header_val: str | None) -> int | None:
    if not header_val:
        return None
    try:
        secs = float(header_val)
        if secs >= 0:
            return min(round(secs * 1000), 60_000)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(header_val)
        delta_ms = int((dt.timestamp() - time.time()) * 1000)
        return min(max(0, delta_ms), 60_000)
    except Exception:
        return None


def _is_retryable_status(status_code: int) -> bool:
    return status_code >= 500 or status_code in (429, 408)


class DispatcherWorker:
    def __init__(
        self,
        *,
        repo: JobRepository,
        token_buckets: DomainTokenBucketLimiter,
        circuit_breaker: DomainCircuitBreaker,
        health_tracker: DomainHealthTracker,
        settings: Settings,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.repo = repo
        self.token_buckets = token_buckets
        self.circuit_breaker = circuit_breaker
        self.health_tracker = health_tracker
        self.settings = settings
        self._external_client = http_client
        self._client: httpx.AsyncClient | None = http_client

        self._running = False
        self._in_flight: set[asyncio.Task] = set()
        self._wakeup = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._reaper_task: asyncio.Task | None = None

        self.total_dispatched = 0
        self.total_succeeded = 0
        self.total_failed = 0
        self.total_deferred = 0
        self.leases_reclaimed = 0

    async def start(self) -> None:
        if self._running:
            return
        self._running = True

        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.REQUEST_TIMEOUT_MS / 1000.0),
                limits=httpx.Limits(max_connections=64, max_keepalive_connections=20),
                follow_redirects=False,
            )

        reclaimed = await self.repo.reclaim_expired_leases(int(time.time() * 1000))
        self.leases_reclaimed += reclaimed

        self._loop_task = asyncio.create_task(self._poll_loop(), name="dispatcher-poll-loop")
        self._reaper_task = asyncio.create_task(self._reaper_loop(), name="dispatcher-lease-reaper")
        self.nudge()

    async def stop(self) -> None:
        self._running = False
        self._wakeup.set()

        for task in (self._loop_task, self._reaper_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._in_flight:
            await asyncio.gather(*list(self._in_flight), return_exceptions=True)

        if self._client is not None and self._external_client is None:
            await self._client.aclose()
            self._client = None

    def nudge(self) -> None:
        if self._running:
            self._wakeup.set()

    async def _poll_loop(self) -> None:
        poll_sec = self.settings.WORKER_POLL_INTERVAL_MS / 1000.0
        while self._running:
            try:
                await self._drain_ready_batch()
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._wakeup.wait(), timeout=poll_sec)
            except asyncio.TimeoutError:
                pass
            self._wakeup.clear()

    async def _reaper_loop(self) -> None:
        interval_sec = max(5.0, (self.settings.WORKER_LEASE_MS / 2) / 1000.0)
        while self._running:
            await asyncio.sleep(interval_sec)
            if not self._running:
                break
            try:
                reclaimed = await self.repo.reclaim_expired_leases(int(time.time() * 1000))
                if reclaimed > 0:
                    self.leases_reclaimed += reclaimed
                    self.nudge()
            except Exception:
                pass

    async def _drain_ready_batch(self) -> None:
        available_slots = self.settings.WORKER_CONCURRENCY - len(self._in_flight)
        if available_slots <= 0:
            return

        batch_limit = min(available_slots, self.settings.WORKER_BATCH_SIZE)
        now_ms = int(time.time() * 1000)
        jobs = await self.repo.claim_ready_jobs(batch_limit, now_ms, self.settings.WORKER_LEASE_MS)
        for job in jobs:
            task = asyncio.create_task(self._process_job(job))
            self._in_flight.add(task)
            task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._in_flight.discard(task)
        if self._running:
            self.nudge()

    def _compute_backoff_ms(self, attempt_number: int, retry_after_ms: int | None) -> int:
        exp_delay = self.settings.RETRY_BASE_DELAY_MS * (2 ** (attempt_number - 1))
        jitter_span = exp_delay * self.settings.RETRY_JITTER_RATIO
        offset = random.uniform(-jitter_span, jitter_span) if jitter_span > 0 else 0.0
        with_jitter = max(100, round(exp_delay + offset))
        if retry_after_ms is not None:
            return max(with_jitter, retry_after_ms)
        return with_jitter

    async def _process_job(self, job: WebhookJob) -> None:
        now_ms = int(time.time() * 1000)
        domain = job.destinationDomain

        # --- Circuit Breaker & Rate Limit Checks ---
        circuit = self.circuit_breaker.can_execute(domain, now_ms)
        if not circuit.allowed:
            self.total_deferred += 1
            await self.repo.defer_job(job.id, now_ms + circuit.retry_after_ms, now_ms)
            return

        bucket = self.token_buckets.try_acquire(domain, now_ms)
        if not bucket.allowed:
            self.total_deferred += 1
            await self.repo.defer_job(job.id, now_ms + bucket.retry_after_ms, now_ms)
            return

        attempt_number = job.attemptCount + 1
        started_at = int(time.time() * 1000)
        self.total_dispatched += 1

        try:
            assert self._client is not None
            raw_body = json.dumps(job.payload, separators=(",", ":"))
            ts_sec = started_at // 1000
            signature = sign_webhook_payload(self.settings.SIGNING_SECRET, ts_sec, raw_body)

            resp = await self._client.post(
                job.destinationUrl,
                content=raw_body.encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "PulseHook-FastAPI/1.0",
                    "X-Webhook-Id": job.id,
                    "X-Webhook-Event-Id": job.eventId,
                    "X-Webhook-Attempt": str(attempt_number),
                    "X-Webhook-Timestamp": str(ts_sec),
                    "X-Webhook-Signature": signature,
                },
                timeout=self.settings.REQUEST_TIMEOUT_MS / 1000.0,
            )
            duration_ms = max(1, int(time.time() * 1000) - started_at)
            snippet = resp.text[:256] if resp.text else None

            if 200 <= resp.status_code < 300:
                self.circuit_breaker.record_success(domain)
                self.health_tracker.record(domain, duration_ms, True)
                self.total_succeeded += 1
                await self.repo.record_attempt_success(
                    job_id=job.id,
                    attempt_number=attempt_number,
                    response_code=resp.status_code,
                    duration_ms=duration_ms,
                    response_snippet=snippet,
                    now_ms=int(time.time() * 1000),
                )
                return

            retryable = _is_retryable_status(resp.status_code)
            finish_ms = int(time.time() * 1000)
            if resp.status_code >= 500 or resp.status_code == 408:
                self.circuit_breaker.record_failure(domain, finish_ms)
                self.health_tracker.record(domain, duration_ms, False)
            elif resp.status_code == 429:
                self.health_tracker.record(domain, duration_ms, False)
            else:
                self.circuit_breaker.record_success(domain)
                self.health_tracker.record(domain, duration_ms, False)

            can_retry = retryable and job.attemptCount < job.maxRetries
            retry_after_ms = _parse_retry_after_ms(resp.headers.get("retry-after"))
            next_run_at = (
                finish_ms + self._compute_backoff_ms(attempt_number, retry_after_ms)
                if can_retry
                else None
            )
            if not can_retry:
                self.total_failed += 1

            await self.repo.record_attempt_failure(
                job_id=job.id,
                attempt_number=attempt_number,
                will_retry=can_retry,
                next_run_at=next_run_at,
                response_code=resp.status_code,
                duration_ms=duration_ms,
                error_message=(
                    f"Downstream returned HTTP {resp.status_code}"
                    if retryable
                    else f"Permanent client error HTTP {resp.status_code} (not retried)"
                ),
                response_snippet=snippet,
                now_ms=finish_ms,
            )
        except Exception as exc:
            finish_ms = int(time.time() * 1000)
            duration_ms = max(1, finish_ms - started_at)
            self.circuit_breaker.record_failure(domain, finish_ms)
            self.health_tracker.record(domain, duration_ms, False)

            can_retry = job.attemptCount < job.maxRetries
            next_run_at = (
                finish_ms + self._compute_backoff_ms(attempt_number, None) if can_retry else None
            )
            if not can_retry:
                self.total_failed += 1

            detail_msg = str(exc).strip() or f"request exceeded {self.settings.REQUEST_TIMEOUT_MS}ms timeout"
            await self.repo.record_attempt_failure(
                job_id=job.id,
                attempt_number=attempt_number,
                will_retry=can_retry,
                next_run_at=next_run_at,
                response_code=None,
                duration_ms=duration_ms,
                error_message=f"{type(exc).__name__}: {detail_msg}",
                response_snippet=None,
                now_ms=finish_ms,
            )
        finally:
            self.token_buckets.release(domain)

    def get_stats(self) -> dict:
        return {
            "running": self._running,
            "activeInFlight": len(self._in_flight),
            "maxConcurrency": self.settings.WORKER_CONCURRENCY,
            "totalDispatched": self.total_dispatched,
            "totalSucceeded": self.total_succeeded,
            "totalFailed": self.total_failed,
            "totalDeferred": self.total_deferred,
            "leasesReclaimed": self.leases_reclaimed,
        }

    def get_domain_metrics(self) -> list[DomainRateMetrics]:
        now_ms = int(time.time() * 1000)
        buckets = self.token_buckets.snapshot(now_ms)
        circuits = self.circuit_breaker.snapshot()
        metrics: list[DomainRateMetrics] = []
        for b in buckets:
            c = circuits.get(b["domain"], {})
            c_state = c.get("state", "CLOSED")
            ewma, health_score, diagnostic = self.health_tracker.diagnose(b["domain"], c_state)
            metrics.append(
                DomainRateMetrics(
                    domain=b["domain"],
                    availableTokens=b["availableTokens"],
                    capacity=b["capacity"],
                    refillPerSec=b["refillPerSec"],
                    inflight=b["inflight"],
                    maxInflight=b["maxInflight"],
                    deferredCount=b["deferredCount"],
                    circuitState=c_state,
                    consecutiveFailures=c.get("consecutiveFailures", 0),
                    nextProbeAt=c.get("nextProbeAt"),
                    ewmaLatencyMs=ewma,
                    healthScore=health_score,
                    diagnostic=diagnostic,
                )
            )
        return metrics
