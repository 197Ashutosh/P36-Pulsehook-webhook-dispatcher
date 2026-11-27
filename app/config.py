from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    PORT: int = 3000
    HOST: str = "0.0.0.0"
    ENV: str = "development"

    SQLITE_PATH: str = "./data/webhooks.db"
    DATABASE_URL: str = ""

    WORKER_POLL_INTERVAL_MS: int = 150
    WORKER_CONCURRENCY: int = 16
    WORKER_BATCH_SIZE: int = 25
    WORKER_LEASE_MS: int = 30000
    MAX_RETRIES: int = 3
    RETRY_BASE_DELAY_MS: int = 2000
    RETRY_JITTER_RATIO: float = 0.15
    REQUEST_TIMEOUT_MS: int = 15000

    DOMAIN_BUCKET_CAPACITY: float = 5.0
    DOMAIN_BUCKET_REFILL_PER_SEC: float = 2.0
    DOMAIN_MAX_INFLIGHT: int = 3

    INGRESS_WINDOW_MS: int = 10000
    INGRESS_MAX_REQUESTS: int = 200
    MOCK_WINDOW_MS: int = 10000
    MOCK_MAX_REQUESTS: int = 30

    CIRCUIT_FAILURE_THRESHOLD: int = 5
    CIRCUIT_COOLDOWN_MS: int = 15000

    SIGNING_SECRET: str = "pulsehook_dev_signing_secret_change_in_prod"
    ALLOW_PRIVATE_URLS: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
