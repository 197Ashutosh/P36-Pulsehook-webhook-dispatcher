from __future__ import annotations

import time
from dataclasses import dataclass
from app.models import CircuitState


@dataclass(frozen=True)
class CircuitCheckResult:
    allowed: bool
    state: CircuitState
    retry_after_ms: int


@dataclass
class _BreakerEntry:
    state: CircuitState = "CLOSED"
    consecutive_failures: int = 0
    next_probe_at: int | None = None
    probe_in_flight: bool = False


class DomainCircuitBreaker:
    def __init__(self, failure_threshold: int, cooldown_ms: int) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_ms = cooldown_ms
        self._entries: dict[str, _BreakerEntry] = {}

    def _get_or_create(self, domain: str) -> _BreakerEntry:
        entry = self._entries.get(domain)
        if entry is None:
            entry = _BreakerEntry()
            self._entries[domain] = entry
        return entry

    def can_execute(self, domain: str, now_ms: int | None = None) -> CircuitCheckResult:
        ts = now_ms if now_ms is not None else int(time.time() * 1000)
        entry = self._get_or_create(domain)

        if entry.state == "CLOSED":
            return CircuitCheckResult(allowed=True, state="CLOSED", retry_after_ms=0)

        if entry.state == "OPEN":
            next_probe = entry.next_probe_at or (ts + self.cooldown_ms)
            if ts < next_probe:
                return CircuitCheckResult(
                    allowed=False,
                    state="OPEN",
                    retry_after_ms=max(50, next_probe - ts),
                )
            entry.state = "HALF_OPEN"
            entry.probe_in_flight = True
            return CircuitCheckResult(allowed=True, state="HALF_OPEN", retry_after_ms=0)

        if entry.probe_in_flight:
            return CircuitCheckResult(allowed=False, state="HALF_OPEN", retry_after_ms=500)

        entry.probe_in_flight = True
        return CircuitCheckResult(allowed=True, state="HALF_OPEN", retry_after_ms=0)

    def record_success(self, domain: str) -> None:
        entry = self._get_or_create(domain)
        entry.state = "CLOSED"
        entry.consecutive_failures = 0
        entry.next_probe_at = None
        entry.probe_in_flight = False

    def record_failure(self, domain: str, now_ms: int | None = None) -> None:
        ts = now_ms if now_ms is not None else int(time.time() * 1000)
        entry = self._get_or_create(domain)
        entry.consecutive_failures += 1
        entry.probe_in_flight = False

        if entry.state == "HALF_OPEN" or entry.consecutive_failures >= self.failure_threshold:
            entry.state = "OPEN"
            entry.next_probe_at = ts + self.cooldown_ms

    def snapshot(self) -> dict[str, dict]:
        return {
            domain: {
                "state": entry.state,
                "consecutiveFailures": entry.consecutive_failures,
                "nextProbeAt": entry.next_probe_at,
            }
            for domain, entry in self._entries.items()
        }
