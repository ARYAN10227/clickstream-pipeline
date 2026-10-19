"""Click event schema (SPEC "Event schema") and mapping to analytics.clicks rows."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

import orjson
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from clickpipe.enrich import country, parse_ua, referrer_domain

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


class DlqReason(StrEnum):
    """Why a record was routed to the dead-letter topic (SPEC MVP feature 5)."""

    BAD_JSON = "bad_json"
    SCHEMA_INVALID = "schema_invalid"
    MISSING_FIELD = "missing_field"
    LATE_EVENT = "late_event"
    FUTURE_EVENT = "future_event"


@dataclass(frozen=True)
class Valid:
    """A record that passed every data-quality rule."""

    event: ClickEvent


@dataclass(frozen=True)
class Invalid:
    """A record that failed a data-quality rule; `error` is safe to log and store."""

    reason: DlqReason
    error: str


MAX_ERROR_LEN = 256
REDACTED = "<redacted>"


def _safe_error(text: str, payload: object) -> str:
    """Strip the event's ip from an error message and cap its length."""
    ip = payload.get("ip") if isinstance(payload, dict) else None
    if isinstance(ip, str) and ip:
        text = text.replace(ip, REDACTED)
    if len(text) > MAX_ERROR_LEN:
        text = text[: MAX_ERROR_LEN - 3] + "..."
    return text


def _describe(exc: ValidationError) -> str:
    """Summarise validation errors by field and message, never echoing input values."""
    parts = []
    for err in exc.errors(include_url=False, include_input=False):
        field = ".".join(str(part) for part in err["loc"]) or "<root>"
        parts.append(f"{field}: {err['msg']}")
    return "; ".join(parts)


def validate(raw: bytes, now_ms: int, late_threshold_s: int, future_skew_s: int) -> Valid | Invalid:
    """Apply the data-quality rules to one raw message value; never raises.

    Events exactly at the late or future threshold are accepted.
    """
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        return Invalid(DlqReason.BAD_JSON, _safe_error(f"bad JSON: {exc}", None))
    if not isinstance(payload, dict):
        return Invalid(
            DlqReason.BAD_JSON, f"top-level JSON value is {type(payload).__name__}, not an object"
        )

    try:
        event = ClickEvent.model_validate(payload)
    except ValidationError as exc:
        missing = any(err["type"] == "missing" for err in exc.errors())
        reason = DlqReason.MISSING_FIELD if missing else DlqReason.SCHEMA_INVALID
        return Invalid(reason, _safe_error(_describe(exc), payload))

    lag_ms = now_ms - event.event_time
    if lag_ms > late_threshold_s * 1000:
        return Invalid(
            DlqReason.LATE_EVENT,
            f"event_time is {lag_ms} ms old, limit is {late_threshold_s * 1000} ms",
        )
    if -lag_ms > future_skew_s * 1000:
        return Invalid(
            DlqReason.FUTURE_EVENT,
            f"event_time is {-lag_ms} ms in the future, limit is {future_skew_s * 1000} ms",
        )
    return Valid(event)


def ms_to_datetime(ms: int) -> datetime:
    """Convert epoch milliseconds to an exact timezone-aware UTC datetime."""
    return EPOCH + timedelta(milliseconds=ms)


def to_row(event: ClickEvent, partition: int, offset: int) -> tuple[Any, ...]:
    """Map an event plus its Kafka position to an enriched row in CLICK_COLUMNS order.

    The ip is only used for the country lookup and is not part of the row.
    """
    device, browser = parse_ua(event.user_agent)
    return (
        event.event_id,
        event.short_code,
        ms_to_datetime(event.event_time),
        ms_to_datetime(event.produced_at),
        referrer_domain(event.referrer),
        device,
        browser,
        country(event.ip),
        event.schema_version,
        partition,
        offset,
    )
