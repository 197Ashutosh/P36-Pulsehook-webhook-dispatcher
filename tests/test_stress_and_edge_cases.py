import asyncio
import time
import httpx
import pytest

from app.config import Settings
from app.storage.sqlite_repo import SqliteJobRepository
from tests.test_integration import run_test_server, wait_until


@pytest.mark.asyncio
async def test_50_concurrent_burst_ingestion_and_full_delivery():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=25,
        WORKER_CONCURRENCY=16,
        WORKER_BATCH_SIZE=25,
        DOMAIN_BUCKET_CAPACITY=20.0,
        DOMAIN_BUCKET_REFILL_PER_SEC=40.0,
        DOMAIN_MAX_INFLIGHT=10,
        INGRESS_MAX_REQUESTS=200,
        MOCK_MAX_REQUESTS=200,
        ALLOW_PRIVATE_URLS=True,
    )
    async with run_test_server(settings) as (base, client):
        t0 = time.perf_counter()
        responses = await asyncio.gather(
            *[
                client.post(
                    f"{base}/api/webhooks/dispatch",
                    json={
                        "destinationUrl": f"{base}/api/mock/receiver?mode=ok&bucket=stress50",
                        "eventId": f"evt_stress_{i}",
                        "payload": {"seq": i},
                    },
                )
                for i in range(50)
            ]
        )
        ingest_ms = (time.perf_counter() - t0) * 1000
        assert all(r.status_code == 202 for r in responses)
        assert ingest_ms < 2000

        async def all_50_done():
            m = (await client.get(f"{base}/api/metrics")).json()
            return m["jobs"]["success"] == 50

        await wait_until(all_50_done, timeout_sec=10.0)

        receipts = (await client.get(f"{base}/api/mock/receipts")).json()["receipts"]
        assert len(receipts) > 0
        assert all(r["signatureValid"] is True for r in receipts)


@pytest.mark.asyncio
async def test_non_head_of_line_blocking_across_domains():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=25,
        DOMAIN_BUCKET_CAPACITY=2.0,
        DOMAIN_BUCKET_REFILL_PER_SEC=2.0,
        DOMAIN_MAX_INFLIGHT=1,
        MOCK_MAX_REQUESTS=100,
        ALLOW_PRIVATE_URLS=True,
    )
    async with run_test_server(settings) as (base, client):
        port = base.rsplit(":", 1)[1]
        domain_a = f"http://127.0.0.1:{port}"
        domain_b = f"http://localhost:{port}"

        for i in range(4):
            r = await client.post(
                f"{base}/api/webhooks/dispatch",
                json={
                    "destinationUrl": f"{domain_a}/api/mock/receiver?mode=slow&delayMs=1200&bucket=holA",
                    "eventId": f"evt_holA_{i}",
                    "payload": {"domain": "A", "i": i},
                },
            )
            assert r.status_code == 202

        t_b = time.perf_counter()
        b_res = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{domain_b}/api/mock/receiver?mode=ok&bucket=holB",
                "eventId": "evt_holB_fast",
                "payload": {"domain": "B"},
            },
        )
        b_id = b_res.json()["jobId"]

        async def b_done():
            r = await client.get(f"{base}/api/webhooks/jobs/{b_id}")
            return r.json()["job"]["status"] == "success"

        await wait_until(b_done, timeout_sec=1.5)
        b_elapsed_ms = (time.perf_counter() - t_b) * 1000
        assert b_elapsed_ms < 800


@pytest.mark.asyncio
async def test_error_handling_validation_ssrf_404_409_and_ingress_429():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        INGRESS_WINDOW_MS=5000,
        INGRESS_MAX_REQUESTS=5,
        ALLOW_PRIVATE_URLS=False,
    )
    async with run_test_server(settings) as (base, client):
        bad_proto = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={"destinationUrl": "ftp://example.com/hook", "eventId": "e1", "payload": {}},
        )
        assert bad_proto.status_code == 400

        ssrf_meta = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={"destinationUrl": "http://169.254.169.254/latest", "eventId": "e2", "payload": {}},
        )
        assert ssrf_meta.status_code == 400

        ssrf_priv = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={"destinationUrl": "http://127.0.0.1:9999/hook", "eventId": "e3", "payload": {}},
        )
        assert ssrf_priv.status_code == 400

        assert (await client.get(f"{base}/api/webhooks/jobs/job_missing")).status_code == 404
        assert (await client.post(f"{base}/api/webhooks/jobs/job_missing/replay")).status_code == 409

        for i in range(2):
            ok = await client.post(
                f"{base}/api/webhooks/dispatch",
                json={"destinationUrl": "https://example.com/hook", "eventId": f"ok_{i}", "payload": {}},
            )
            assert ok.status_code == 202

        rate_limited = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={"destinationUrl": "https://example.com/hook", "eventId": "over_limit", "payload": {}},
        )
        assert rate_limited.status_code == 429
        assert "Retry-After" in rate_limited.headers


@pytest.mark.asyncio
async def test_429_does_not_open_circuit_and_400_fails_fast():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=30,
        RETRY_BASE_DELAY_MS=100,
        RETRY_JITTER_RATIO=0.0,
        MAX_RETRIES=2,
        CIRCUIT_FAILURE_THRESHOLD=2,
        ALLOW_PRIVATE_URLS=True,
        MOCK_MAX_REQUESTS=100,
    )
    async with run_test_server(settings) as (base, client):
        r400 = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{base}/api/mock/receiver?mode=badrequest",
                "eventId": "evt_400_ff",
                "payload": {"a": 1},
            },
        )
        id400 = r400.json()["jobId"]
        await wait_until(
            lambda: _job_is(client, base, id400, "failed"), timeout_sec=3.0
        )
        detail400 = (await client.get(f"{base}/api/webhooks/jobs/{id400}")).json()
        assert len(detail400["attempts"]) == 1

        r429 = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{base}/api/mock/receiver?mode=ratelimit",
                "eventId": "evt_429_cb",
                "payload": {"a": 2},
            },
        )
        id429 = r429.json()["jobId"]
        await wait_until(
            lambda: _job_is(client, base, id429, "failed"), timeout_sec=8.0
        )
        metrics = (await client.get(f"{base}/api/metrics")).json()
        assert metrics["domains"][0]["circuitState"] == "CLOSED"


@pytest.mark.asyncio
async def test_stale_lease_reclamation_after_worker_crash():
    repo = SqliteJobRepository(":memory:")
    await repo.init()
    try:
        now = 1_000_000
        await repo.enqueue(
            job_id="job_crashed_1",
            event_id="evt_crash_1",
            idempotency_key="evt_crash_1::https://example.com",
            destination_url="https://example.com/hook",
            destination_domain="example.com",
            payload={"x": 1},
            max_retries=3,
            now_ms=now,
        )
        claimed = await repo.claim_ready_jobs(10, now, lease_ms=500)
        assert len(claimed) == 1
        assert claimed[0].status == "processing"

        assert await repo.reclaim_expired_leases(now + 400) == 0
        assert await repo.reclaim_expired_leases(now + 600) == 1

        detail = await repo.get_job_with_attempts("job_crashed_1")
        assert detail is not None
        assert detail[0].status == "queued"
        assert detail[0].lockedUntil is None
    finally:
        await repo.close()


async def _job_is(client: httpx.AsyncClient, base: str, job_id: str, status: str) -> bool:
    r = await client.get(f"{base}/api/webhooks/jobs/{job_id}")
    return r.json()["job"]["status"] == status
