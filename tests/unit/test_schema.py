import uuid
from datetime import UTC, datetime

import orjson
import pytest
from pydantic import ValidationError

from clickpipe.schema import CLICK_COLUMNS, ClickEvent, parse_event, to_row


def _event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "short_code": "abc123",
        "event_time": 1700000000123,
        "produced_at": 1700000000456,
        "ip": "203.0.113.7",
        "user_agent": "Mozilla/5.0",
        "referrer": "https://example.com/page",
        "schema_version": 1,
    }
    event.update(overrides)
    return event


def _raw(**overrides: object) -> bytes:
    return orjson.dumps(_event(**overrides))


def test_valid_event_parses() -> None:
    data = _event()
    event = parse_event(orjson.dumps(data))

    assert isinstance(event, ClickEvent)
    assert event.event_id == uuid.UUID(str(data["event_id"]))
    assert event.short_code == "abc123"
    assert event.event_time == 1700000000123
    assert event.schema_version == 1


def test_to_row_matches_click_columns() -> None:
    event = parse_event(_raw())
    row = to_row(event, partition=2, offset=99)

    assert len(row) == len(CLICK_COLUMNS)
    by_column = dict(zip(CLICK_COLUMNS, row, strict=True))
    assert by_column["event_id"] == event.event_id
    assert by_column["short_code"] == "abc123"
    assert by_column["referrer_domain"] == ""
    assert by_column["device"] == "unknown"
    assert by_column["browser"] == "unknown"
    assert by_column["country"] == "ZZ"
    assert by_column["schema_version"] == 1
    assert by_column["kafka_partition"] == 2
    assert by_column["kafka_offset"] == 99
    assert "ingested_at" not in CLICK_COLUMNS
    assert "ip" not in CLICK_COLUMNS


def test_ms_to_datetime_is_exact() -> None:
    row = dict(zip(CLICK_COLUMNS, to_row(parse_event(_raw()), 0, 0), strict=True))

    assert row["event_time"] == datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)
    assert row["produced_at"] == datetime(2023, 11, 14, 22, 13, 20, 456000, tzinfo=UTC)
    assert row["event_time"].tzinfo is UTC


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(_raw(short_code="a" * 17), id="short_code-17-chars"),
        pytest.param(_raw(schema_version=2), id="schema_version-2"),
        pytest.param(
            orjson.dumps({k: v for k, v in _event().items() if k != "event_id"}),
            id="missing-event_id",
        ),
        pytest.param(b'{"event_id": "abc", ', id="malformed-json"),
    ],
)
def test_invalid_input_raises(raw: bytes) -> None:
    with pytest.raises((ValidationError, orjson.JSONDecodeError)):
        parse_event(raw)


def test_sixteen_char_short_code_is_accepted() -> None:
    assert parse_event(_raw(short_code="A" * 8 + "z_-9" * 2)).short_code == "AAAAAAAAz_-9z_-9"
