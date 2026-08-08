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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="TASKMESH_",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
