from __future__ import annotations

import math
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class TokenAcquireResult:
    allowed: bool
    retry_after_ms: int
    reason: str | None = None


@dataclass
class _BucketState:
    tokens: float
    last_refill_ms: int
    inflight: int = 0
    deferred_count: int = 0


class DomainTokenBucketLimiter:
    def __init__(self, capacity: float, refill_per_sec: float, max_inflight: int) -> None:
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.max_inflight = max_inflight
        self._buckets: dict[str, _BucketState] = {}

    def _get_or_create(self, domain: str, now_ms: int) -> _BucketState:
        state = self._buckets.get(domain)
        if state is None:
            state = _BucketState(tokens=self.capacity, last_refill_ms=now_ms)
            self._buckets[domain] = state
            return state

        elapsed_sec = max(0.0, (now_ms - state.last_refill_ms) / 1000.0)
        if elapsed_sec > 0.0:
            state.tokens = min(self.capacity, state.tokens + elapsed_sec * self.refill_per_sec)
            state.last_refill_ms = now_ms
        return state

    def try_acquire(self, domain: str, now_ms: int | None = None) -> TokenAcquireResult:
        ts = now_ms if now_ms is not None else int(time.time() * 1000)
        state = self._get_or_create(domain, ts)

        if state.inflight >= self.max_inflight:
            state.deferred_count += 1
            return TokenAcquireResult(allowed=False, retry_after_ms=150, reason="concurrency_limit")

        if state.tokens < 1.0:
            state.deferred_count += 1
            missing = 1.0 - state.tokens
            wait_ms = max(50, math.ceil((missing / self.refill_per_sec) * 1000))
            return TokenAcquireResult(allowed=False, retry_after_ms=wait_ms, reason="rate_limit")

        state.tokens -= 1.0
        state.inflight += 1
        return TokenAcquireResult(allowed=True, retry_after_ms=0)

    def release(self, domain: str) -> None:
        state = self._buckets.get(domain)
        if state is not None:
            state.inflight = max(0, state.inflight - 1)

    def snapshot(self, now_ms: int | None = None) -> list[dict]:
        ts = now_ms if now_ms is not None else int(time.time() * 1000)
        result: list[dict] = []
        for domain in list(self._buckets.keys()):
            state = self._get_or_create(domain, ts)
            result.append(
                {
                    "domain": domain,
                    "availableTokens": round(state.tokens, 2),
                    "capacity": self.capacity,
                    "refillPerSec": self.refill_per_sec,
                    "inflight": state.inflight,
                    "maxInflight": self.max_inflight,
                    "deferredCount": state.deferred_count,
                }
            )
        return result
