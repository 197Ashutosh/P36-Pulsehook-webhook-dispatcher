from app.config import Settings
from app.storage.postgres_repo import PostgresJobRepository
from app.storage.repository import JobRepository
from app.storage.sqlite_repo import SqliteJobRepository


def create_job_repository(settings: Settings) -> JobRepository:
    if settings.DATABASE_URL.strip():
        return PostgresJobRepository(settings.DATABASE_URL.strip())
    return SqliteJobRepository(settings.SQLITE_PATH)


__all__ = ["JobRepository", "SqliteJobRepository", "PostgresJobRepository", "create_job_repository"]
