from functools import lru_cache

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "TaskMesh"
    app_version: str = "0.1.0"
    debug: bool = False
    database_url: str = Field(
        default="sqlite:///./taskmesh.db",
        validation_alias=AliasChoices("TASKMESH_DATABASE_URL", "DATABASE_URL"),
    )
    celery_broker_url: str = "redis://localhost:6379/0"

    # How long a worker may hold a RUNNING claim before it is considered
    # abandoned (crashed worker, killed process) and eligible for lease
    # recovery. See app/services/recovery.py.
    task_lease_seconds: int = 300
    # How often the Celery Beat schedule triggers a recovery scan.
    task_recovery_interval_seconds: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="TASKMESH_",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
