import pytest

from app.rate_limit.sliding_window import SlidingWindowRateLimiter
from app.rate_limit.token_bucket import DomainTokenBucketLimiter
from app.resilience.anomaly import DomainHealthTracker
from app.resilience.circuit_breaker import DomainCircuitBreaker
from app.security.signer import (
    sign_webhook_payload,
    validate_destination_url,
    verify_webhook_signature,
)


def test_token_bucket_per_domain_isolation_and_refill():
    limiter = DomainTokenBucketLimiter(capacity=2.0, refill_per_sec=2.0, max_inflight=5)
    t0 = 1_000_000

    assert limiter.try_acquire("api.alpha.com", t0).allowed is True
    assert limiter.try_acquire("api.alpha.com", t0).allowed is True

    blocked = limiter.try_acquire("api.alpha.com", t0)
    assert blocked.allowed is False
    assert blocked.reason == "rate_limit"
    assert blocked.retry_after_ms >= 450

    assert limiter.try_acquire("api.beta.com", t0).allowed is True

    assert limiter.try_acquire("api.alpha.com", t0 + 500).allowed is True


def test_token_bucket_concurrency_cap():
    limiter = DomainTokenBucketLimiter(capacity=10.0, refill_per_sec=10.0, max_inflight=2)
    t0 = 1_000_000

    assert limiter.try_acquire("slow.test", t0).allowed is True
    assert limiter.try_acquire("slow.test", t0).allowed is True

    blocked = limiter.try_acquire("slow.test", t0)
    assert blocked.allowed is False
    assert blocked.reason == "concurrency_limit"

    limiter.release("slow.test")
    assert limiter.try_acquire("slow.test", t0).allowed is True


def test_sliding_window_limiter():
    limiter = SlidingWindowRateLimiter(window_ms=1000, max_requests=3)
    t0 = 50_000

    assert limiter.consume("ip1", t0).allowed is True
    assert limiter.consume("ip1", t0 + 200).allowed is True
    assert limiter.consume("ip1", t0 + 400).allowed is True

    denied = limiter.consume("ip1", t0 + 500)
    assert denied.allowed is False
    assert denied.remaining == 0
    assert denied.retry_after_ms == 500

    assert limiter.consume("ip1", t0 + 1001).allowed is True


def test_circuit_breaker_lifecycle():
    breaker = DomainCircuitBreaker(failure_threshold=3, cooldown_ms=5000)
    t0 = 10_000

    breaker.record_failure("down.test", t0)
    breaker.record_failure("down.test", t0 + 10)
    assert breaker.can_execute("down.test", t0 + 20).allowed is True

    breaker.record_failure("down.test", t0 + 30)
    open_res = breaker.can_execute("down.test", t0 + 100)
    assert open_res.allowed is False
    assert open_res.state == "OPEN"

    probe = breaker.can_execute("down.test", t0 + 5030)
    assert probe.allowed is True
    assert probe.state == "HALF_OPEN"
    assert breaker.can_execute("down.test", t0 + 5031).allowed is False

    breaker.record_success("down.test")
    assert breaker.can_execute("down.test", t0 + 5040).state == "CLOSED"


def test_hmac_signing_and_ssrf_guard():
    secret = "super_secret_key_123"
    body = '{"event":"user.signup","userId":"usr_999"}'
    ts = 1_700_000_000

    sig = sign_webhook_payload(secret, ts, body)
    assert verify_webhook_signature(secret, sig, body, 300, ts + 10) is True
    assert verify_webhook_signature(secret, sig, '{"tampered":true}', 300, ts + 10) is False
    assert verify_webhook_signature(secret, sig, body, 300, ts + 400) is False

    with pytest.raises(ValueError):
        validate_destination_url("http://169.254.169.254/latest/meta-data", True)
    with pytest.raises(ValueError):
        validate_destination_url("http://127.0.0.1:3000/hook", False)

    ok = validate_destination_url("https://api.client.com/webhooks", False)
    assert ok.domain == "api.client.com"


def test_anomaly_health_tracker():
    tracker = DomainHealthTracker()
    for _ in range(5):
        tracker.record("api.ok.com", 45, True)
    ewma, score, diag = tracker.diagnose("api.ok.com", "CLOSED")
    assert ewma is not None and ewma < 60
    assert score >= 95
    assert diag == "Healthy"

    tracker.record("api.ok.com", 1200, True)
    _, score_spike, diag_spike = tracker.diagnose("api.ok.com", "CLOSED")
    assert "Latency anomaly" in diag_spike
    assert score_spike < 100
