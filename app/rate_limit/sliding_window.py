from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class SlidingWindowResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_ms: int
    reset_at_ms: int


class SlidingWindowRateLimiter:
    def __init__(self, window_ms: int, max_requests: int) -> None:
        self.window_ms = window_ms
        self.max_requests = max_requests
        self._logs: dict[str, deque[int]] = {}
        self._last_prune_at = 0

    def consume(self, key: str, now_ms: int | None = None) -> SlidingWindowResult:
        ts = now_ms if now_ms is not None else int(time.time() * 1000)
        self._maybe_prune(ts)

        cutoff = ts - self.window_ms
        bucket = self._logs.setdefault(key, deque())
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()

        if len(bucket) >= self.max_requests:
            oldest = bucket[0]
            reset_at = oldest + self.window_ms
            retry_after = max(1, reset_at - ts)
            return SlidingWindowResult(
                allowed=False,
                limit=self.max_requests,
                remaining=0,
                retry_after_ms=retry_after,
                reset_at_ms=reset_at,
            )

        bucket.append(ts)
        reset_at = bucket[0] + self.window_ms
        return SlidingWindowResult(
            allowed=True,
            limit=self.max_requests,
            remaining=max(0, self.max_requests - len(bucket)),
            retry_after_ms=0,
            reset_at_ms=reset_at,
        )

    def _maybe_prune(self, now_ms: int) -> None:
        if now_ms - self._last_prune_at < self.window_ms:
            return
        self._last_prune_at = now_ms
        cutoff = now_ms - self.window_ms
        stale_keys = [k for k, q in self._logs.items() if not q or q[-1] <= cutoff]
        for k in stale_keys:
            del self._logs[k]
