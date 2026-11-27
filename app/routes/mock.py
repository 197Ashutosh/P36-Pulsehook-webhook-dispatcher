from __future__ import annotations

import asyncio
import json
import math
import time
from collections import deque
from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import JSONResponse

from app.security.signer import verify_webhook_signature

router = APIRouter(prefix="/api/mock", tags=["mock"])


@router.post("/receiver")
async def mock_receiver(
    request: Request,
    response: Response,
    mode: str = Query(default="ok"),
    delayMs: int = Query(default=10000, ge=50, le=30000),
    failCount: int = Query(default=2, ge=1, le=5),
    bucket: str = Query(default="global"),
):
    container = request.app.state.container
    rate = container.mock_limiter.consume(f"mock:{bucket}")

    headers = {
        "X-RateLimit-Limit": str(rate.limit),
        "X-RateLimit-Remaining": str(rate.remaining),
        "X-RateLimit-Reset": str(math.ceil(rate.reset_at_ms / 1000)),
    }

    if not rate.allowed:
        retry_sec = max(1, math.ceil(rate.retry_after_ms / 1000))
        headers["Retry-After"] = str(retry_sec)
        return JSONResponse(
            status_code=429,
            headers=headers,
            content={
                "ok": False,
                "error": "Mock Receiver Sliding-Window Rate Limit Exceeded",
                "retryAfterSec": retry_sec,
            },
        )

    for k, v in headers.items():
        response.headers[k] = v

    raw_bytes = await request.body()
    raw_text = raw_bytes.decode("utf-8", errors="replace")
    try:
        parsed_body = json.loads(raw_text) if raw_text else {}
    except json.JSONDecodeError:
        parsed_body = {"raw": raw_text}

    webhook_id = request.headers.get("x-webhook-id")
    event_id = request.headers.get("x-webhook-event-id")
    attempt_hdr = request.headers.get("x-webhook-attempt")
    attempt = int(attempt_hdr) if attempt_hdr and attempt_hdr.isdigit() else None
    sig_hdr = request.headers.get("x-webhook-signature")
    sig_valid = (
        verify_webhook_signature(container.settings.SIGNING_SECRET, sig_hdr, raw_text)
        if sig_hdr
        else False
    )

    receipts: deque = container.mock_receipts

    def record(status_code: int) -> None:
        receipts.appendleft(
            {
                "receivedAt": int(time.time() * 1000),
                "webhookId": webhook_id,
                "eventId": event_id,
                "attempt": attempt,
                "mode": mode,
                "statusCode": status_code,
                "signatureValid": sig_valid,
                "payload": parsed_body,
            }
        )

    if mode == "slow":
        await asyncio.sleep(delayMs / 1000.0)
        record(200)
        return JSONResponse(
            status_code=200,
            headers=headers,
            content={"ok": True, "mode": mode, "delayedMs": delayMs, "signatureValid": sig_valid},
        )

    if mode == "fail500":
        record(503)
        return JSONResponse(
            status_code=503,
            headers=headers,
            content={"ok": False, "mode": mode, "message": "Simulated downstream 503 outage"},
        )

    if mode == "ratelimit":
        headers["Retry-After"] = "2"
        record(429)
        return JSONResponse(
            status_code=429,
            headers=headers,
            content={"ok": False, "mode": mode, "message": "Simulated 429 rate limit"},
        )

    if mode == "badrequest":
        record(400)
        return JSONResponse(
            status_code=400,
            headers=headers,
            content={"ok": False, "mode": mode, "message": "Simulated permanent 400 error"},
        )

    if mode == "flaky":
        key = webhook_id or event_id or "default"
        seen = container.flaky_counters.get(key, 0) + 1
        container.flaky_counters[key] = seen
        if seen <= failCount:
            record(503)
            return JSONResponse(
                status_code=503,
                headers=headers,
                content={
                    "ok": False,
                    "mode": mode,
                    "attemptSeen": seen,
                    "willSucceedAfter": failCount,
                },
            )
        container.flaky_counters.pop(key, None)

    record(200)
    return {
        "ok": True,
        "mode": mode,
        "receivedAt": int(time.time() * 1000),
        "signatureValid": sig_valid,
        "webhookId": webhook_id,
        "eventId": event_id,
    }


@router.get("/receipts")
async def list_mock_receipts(request: Request):
    receipts = list(request.app.state.container.mock_receipts)
    return {"count": len(receipts), "receipts": receipts}
