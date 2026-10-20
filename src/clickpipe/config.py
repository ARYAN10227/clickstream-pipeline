"""Typed runtime settings loaded from environment variables and an optional .env file."""

from functools import lru_cache
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline settings; each field maps to the upper-case env var of the same name."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Kafka / Redpanda
    kafka_bootstrap: str = "127.0.0.1:19092"
    clicks_topic: str = "clicks"
    dlq_topic: str = "clicks-dlq"
    consumer_group: str = "clickpipe"

    # ClickHouse
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8123
    clickhouse_user: str = "clickpipe"
    clickhouse_password: str = "clickpipe_dev"
    clickhouse_db: str = "analytics"

    # Batching and data-quality thresholds
    batch_max_size: int = 5000
    batch_max_wait_ms: int = 500
    late_threshold_s: int = 300
    future_skew_s: int = 60

    # Insert retries (exponential backoff with jitter)
    insert_max_retries: int = 8
    retry_base_s: float = 0.2
    retry_max_s: float = 5.0

    # Observability and enrichment
    metrics_port: int = 9108
    geoip_db_path: str | None = None
    grafana_admin_password: str = "change_me"
    grafana_ch_password: str = "grafana_dev"

    @field_validator("geoip_db_path", mode="before")
    @classmethod
    def _empty_path_is_unset(cls, value: Any) -> Any:
        """Treat an empty GEOIP_DB_PATH as unset."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    def consumer_config(self) -> dict[str, Any]:
        """Return confluent-kafka Consumer config; offsets are committed manually."""
        return {
            "bootstrap.servers": self.kafka_bootstrap,
            "group.id": self.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
        }

    def producer_config(self) -> dict[str, Any]:
        """Return confluent-kafka Producer config with idempotent, fully acked writes."""
        return {
            "bootstrap.servers": self.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "compression.type": "lz4",
        }


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide Settings instance, built once on first call."""
    return Settings()
