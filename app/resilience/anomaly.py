from __future__ import annotations

import math
from dataclasses import dataclass
from app.models import CircuitState


@dataclass
class _DomainStats:
    ewma_latency_ms: float | None = None
    ewma_var_ms2: float = 0.0
    ewma_success_rate: float = 1.0
    samples: int = 0
    last_z_score: float = 0.0


class DomainHealthTracker:
    def __init__(self, alpha: float = 0.25) -> None:
        self.alpha = alpha
        self._stats: dict[str, _DomainStats] = {}

    def record(self, domain: str, duration_ms: int, succeeded: bool) -> None:
        st = self._stats.setdefault(domain, _DomainStats())
        x = float(duration_ms)
        st.samples += 1

        if st.ewma_latency_ms is None:
            st.ewma_latency_ms = x
            st.ewma_var_ms2 = 0.0
            st.last_z_score = 0.0
        else:
            std = math.sqrt(max(st.ewma_var_ms2, 25.0))
            st.last_z_score = (x - st.ewma_latency_ms) / std if st.samples >= 4 else 0.0
            diff = x - st.ewma_latency_ms
            st.ewma_latency_ms = (1.0 - self.alpha) * st.ewma_latency_ms + self.alpha * x
            st.ewma_var_ms2 = (1.0 - self.alpha) * (st.ewma_var_ms2 + self.alpha * diff * diff)

        outcome = 1.0 if succeeded else 0.0
        st.ewma_success_rate = (1.0 - self.alpha) * st.ewma_success_rate + self.alpha * outcome

    def diagnose(self, domain: str, circuit_state: CircuitState) -> tuple[float | None, int, str]:
        st = self._stats.get(domain)
        if st is None:
            return None, 100, "Healthy"

        ewma = round(st.ewma_latency_ms, 1) if st.ewma_latency_ms is not None else None
        score = int(round(st.ewma_success_rate * 100))

        if circuit_state == "OPEN":
            return ewma, min(score, 20), "Circuit OPEN — shedding load to protect workers"
        if st.ewma_success_rate < 0.6:
            return ewma, score, "Degraded — elevated failure rate detected"
        if st.last_z_score >= 2.5 and st.ewma_latency_ms and st.ewma_latency_ms > 200:
            return ewma, max(35, score - 15), f"Latency anomaly (+{st.last_z_score:.1f}σ above baseline)"
        return ewma, score, "Healthy"
