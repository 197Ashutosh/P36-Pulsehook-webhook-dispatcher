from __future__ import annotations

import hashlib
import hmac
import ipaddress
import time
from dataclasses import dataclass
from urllib.parse import urlparse

BLOCKED_METADATA_HOSTS = {
    "169.254.169.254",
    "metadata.google.internal",
    "metadata.goog",
}


def sign_webhook_payload(secret: str, timestamp_sec: int, raw_body: str) -> str:
    signed_payload = f"{timestamp_sec}.{raw_body}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp_sec},v1={digest}"


def verify_webhook_signature(
    secret: str,
    signature_header: str,
    raw_body: str,
    tolerance_sec: int = 300,
    now_sec: int | None = None,
) -> bool:
    current_sec = now_sec if now_sec is not None else int(time.time())
    parts: dict[str, str] = {}
    for item in signature_header.split(","):
        if "=" in item:
            k, v = item.split("=", 1)
            parts[k.strip()] = v.strip()

    ts_raw = parts.get("t", "")
    sig = parts.get("v1", "")
    if not ts_raw or not sig:
        return False
    try:
        ts = int(ts_raw)
    except ValueError:
        return False

    if abs(current_sec - ts) > tolerance_sec:
        return False

    expected = hmac.new(
        secret.encode("utf-8"), f"{ts}.{raw_body}".encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig)


def _is_private_host(hostname: str) -> bool:
    clean = hostname.strip("[]").lower()
    if clean in {"localhost", "::1"}:
        return True
    try:
        ip = ipaddress.ip_address(clean)
        return (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
        )
    except ValueError:
        return False


@dataclass(frozen=True)
class ValidatedDestination:
    normalized_url: str
    domain: str


def validate_destination_url(raw_url: str, allow_private_urls: bool) -> ValidatedDestination:
    parsed = urlparse(raw_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("destinationUrl must be a valid http or https URL")

    hostname = (parsed.hostname or "").lower()
    if hostname in BLOCKED_METADATA_HOSTS:
        raise ValueError("destinationUrl points to a blocked cloud metadata host")

    if not allow_private_urls and _is_private_host(hostname):
        raise ValueError("destinationUrl cannot target private or loopback network addresses")

    return ValidatedDestination(
        normalized_url=parsed.geturl(),
        domain=parsed.netloc.lower(),
    )
