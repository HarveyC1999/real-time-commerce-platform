from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_order_topic: str = "commerce.orders.v1"
    kafka_consumer_group: str = "order-storage-consumer"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "commerce"
    postgres_user: str = "commerce"
    postgres_password: str = "commerce"


@lru_cache
def get_settings() -> Settings:
    """Return the cached application settings."""

    return Settings()