"""Click event schema (SPEC "Event schema") and mapping to analytics.clicks rows."""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

import orjson
from pydantic import BaseModel, ConfigDict, Field

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# analytics.clicks columns in table order, minus ingested_at (filled by ClickHouse).
CLICK_COLUMNS: tuple[str, ...] = (
    "event_id",
    "short_code",
    "event_time",
    "produced_at",
    "referrer_domain",
    "device",
    "browser",
    "country",
    "schema_version",
    "kafka_partition",
    "kafka_offset",
)


class ClickEvent(BaseModel):
    """One redirect click, as produced to the `clicks` topic (schema_version 1)."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    event_id: UUID
    short_code: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,16}$")]
    event_time: int  # epoch ms, click time
    produced_at: int  # epoch ms, set right before send
    ip: str | None = None  # GeoIP lookup only, never stored
    user_agent: Annotated[str | None, Field(max_length=512)] = None
    referrer: str | None = None
    schema_version: Literal[1]


def parse_event(raw: bytes) -> ClickEvent:
    """Decode and validate one Kafka message value; raises on malformed or invalid input."""
    return ClickEvent.model_validate(orjson.loads(raw))


def ms_to_datetime(ms: int) -> datetime:
    """Convert epoch milliseconds to an exact timezone-aware UTC datetime."""
    return EPOCH + timedelta(milliseconds=ms)


def to_row(event: ClickEvent, partition: int, offset: int) -> tuple[Any, ...]:
    """Map an event plus its Kafka position to a row in CLICK_COLUMNS order."""
    return (
        event.event_id,
        event.short_code,
        ms_to_datetime(event.event_time),
        ms_to_datetime(event.produced_at),
        "",  # referrer_domain (enrichment comes later)
        "unknown",  # device
        "unknown",  # browser
        "ZZ",  # country
        event.schema_version,
        partition,
        offset,
    )
