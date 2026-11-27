import asyncio
import socket
import time
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn

from app.config import Settings
from app.main import create_app


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@asynccontextmanager
async def run_test_server(settings: Settings):
    port = _free_port()
    app = create_app(settings)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    base_url = f"http://127.0.0.1:{port}"

    async with httpx.AsyncClient() as client:
        for _ in range(50):
            try:
                r = await client.get(f"{base_url}/health")
                if r.status_code == 200:
                    break
            except Exception:
                await asyncio.sleep(0.05)
        try:
            yield base_url, client
        finally:
            server.should_exit = True
            await task


async def wait_until(predicate, timeout_sec: float = 6.0):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Condition not met within {timeout_sec}s")


@pytest.mark.asyncio
async def test_non_blocking_ingestion_and_async_delivery():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=40,
        ALLOW_PRIVATE_URLS=True,
        MOCK_MAX_REQUESTS=100,
    )
    async with run_test_server(settings) as (base, client):
        t0 = time.perf_counter()
        res = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{base}/api/mock/receiver?mode=slow&delayMs=600",
                "eventId": "evt_slow_1",
                "payload": {"event": "user.signup", "userId": "usr_999"},
            },
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert res.status_code == 202
        assert elapsed_ms < 200
        job_id = res.json()["jobId"]
        assert res.json()["status"] == "queued"

        async def is_done():
            r = await client.get(f"{base}/api/webhooks/jobs/{job_id}")
            return r.json()["job"]["status"] == "success"

        await wait_until(is_done)
        detail = (await client.get(f"{base}/api/webhooks/jobs/{job_id}")).json()
        assert detail["job"]["lastStatusCode"] == 200
        assert len(detail["attempts"]) == 1
        assert detail["attempts"][0]["durationMs"] >= 500


@pytest.mark.asyncio
async def test_idempotency_deduplication():
    settings = Settings(ENV="test", SQLITE_PATH=":memory:", ALLOW_PRIVATE_URLS=True, MOCK_MAX_REQUESTS=100)
    async with run_test_server(settings) as (base, client):
        body = {
            "destinationUrl": f"{base}/api/mock/receiver?mode=ok",
            "eventId": "evt_dup_99",
            "payload": {"order": 99},
        }
        r1 = await client.post(f"{base}/api/webhooks/dispatch", json=body)
        r2 = await client.post(f"{base}/api/webhooks/dispatch", json=body)
        assert r1.status_code == 202 and r2.status_code == 202
        assert r1.json()["deduplicated"] is False
        assert r2.json()["deduplicated"] is True
        assert r2.json()["jobId"] == r1.json()["jobId"]


@pytest.mark.asyncio
async def test_exponential_backoff_retries_flaky_and_dlq_and_replay():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=40,
        RETRY_BASE_DELAY_MS=120,
        RETRY_JITTER_RATIO=0.0,
        MAX_RETRIES=3,
        CIRCUIT_FAILURE_THRESHOLD=10,
        ALLOW_PRIVATE_URLS=True,
        MOCK_MAX_REQUESTS=100,
    )
    async with run_test_server(settings) as (base, client):
        flaky = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{base}/api/mock/receiver?mode=flaky&failCount=2",
                "eventId": "evt_flaky_1",
                "payload": {"k": "v"},
            },
        )
        flaky_id = flaky.json()["jobId"]

        await wait_until(lambda: _check_status(client, base, flaky_id, "success"))
        flaky_detail = (await client.get(f"{base}/api/webhooks/jobs/{flaky_id}")).json()
        assert [a["status"] for a in flaky_detail["attempts"]] == ["retrying", "retrying", "success"]

        outage = await client.post(
            f"{base}/api/webhooks/dispatch",
            json={
                "destinationUrl": f"{base}/api/mock/receiver?mode=fail500",
                "eventId": "evt_outage_1",
                "payload": {"k": "v"},
            },
        )
        outage_id = outage.json()["jobId"]
        await wait_until(lambda: _check_status(client, base, outage_id, "failed"), timeout_sec=8.0)

        outage_detail = (await client.get(f"{base}/api/webhooks/jobs/{outage_id}")).json()
        assert [a["status"] for a in outage_detail["attempts"]] == [
            "retrying",
            "retrying",
            "retrying",
            "failed",
        ]

        replay = await client.post(f"{base}/api/webhooks/jobs/{outage_id}/replay")
        assert replay.status_code == 202


@pytest.mark.asyncio
async def test_rate_limiting_token_bucket_and_sliding_window_and_ui():
    settings = Settings(
        ENV="test",
        SQLITE_PATH=":memory:",
        WORKER_POLL_INTERVAL_MS=40,
        DOMAIN_BUCKET_CAPACITY=2.0,
        DOMAIN_BUCKET_REFILL_PER_SEC=8.0,
        DOMAIN_MAX_INFLIGHT=2,
        MOCK_WINDOW_MS=5000,
        MOCK_MAX_REQUESTS=5,
        ALLOW_PRIVATE_URLS=True,
    )
    async with run_test_server(settings) as (base, client):
        dash = await client.get(f"{base}/")
        assert dash.status_code == 200
        assert "PulseHook" in dash.text

        burst = await asyncio.gather(
            *[
                client.post(
                    f"{base}/api/webhooks/dispatch",
                    json={
                        "destinationUrl": f"{base}/api/mock/receiver?mode=ok&bucket=burst",
                        "eventId": f"evt_b_{i}",
                        "payload": {"idx": i},
                    },
                )
                for i in range(8)
            ]
        )
        assert all(r.status_code == 202 for r in burst)

        async def burst_done():
            m = (await client.get(f"{base}/api/metrics")).json()
            return m["worker"]["totalDeferred"] > 0 and m["jobs"]["success"] >= 5

        await wait_until(burst_done)

        direct = await asyncio.gather(
            *[
                client.post(f"{base}/api/mock/receiver?mode=ok&bucket=direct", json={"ping": True})
                for _ in range(7)
            ]
        )
        codes = [r.status_code for r in direct]
        assert codes.count(200) == 5
        assert codes.count(429) == 2


async def _check_status(client: httpx.AsyncClient, base: str, job_id: str, expected: str) -> bool:
    r = await client.get(f"{base}/api/webhooks/jobs/{job_id}")
    return r.json()["job"]["status"] == expected
