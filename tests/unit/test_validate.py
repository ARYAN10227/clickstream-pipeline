import uuid

import orjson
import pytest

from clickpipe.schema import MAX_ERROR_LEN, DlqReason, Invalid, Valid, validate

NOW_MS = 1_700_000_000_000
LATE_S = 300
FUTURE_S = 60
IP = "198.51.100.23"


def _event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "short_code": "abc123",
        "event_time": NOW_MS,
        "produced_at": NOW_MS,
        "ip": IP,
        "user_agent": "Mozilla/5.0",
        "referrer": None,
        "schema_version": 1,
    }
    event.update(overrides)
    return event


def _without(field: str) -> bytes:
    return orjson.dumps({k: v for k, v in _event().items() if k != field})


def _check(raw: bytes) -> Valid | Invalid:
    return validate(raw, late_threshold_s=LATE_S, future_skew_s=FUTURE_S)


def _reason(result: Valid | Invalid) -> DlqReason | None:
    return result.reason if isinstance(result, Invalid) else None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(b'{"event_id": ', DlqReason.BAD_JSON, id="bad_json-truncated"),
        pytest.param(b"[1, 2, 3]", DlqReason.BAD_JSON, id="bad_json-not-an-object"),
        pytest.param(_without("event_id"), DlqReason.MISSING_FIELD, id="missing_field"),
        pytest.param(
            orjson.dumps(_event(event_id="not-a-uuid")), DlqReason.SCHEMA_INVALID, id="schema"
        ),
        pytest.param(
            orjson.dumps(_event(event_time=NOW_MS - LATE_S * 1000 - 5000)),
            DlqReason.LATE_EVENT,
            id="late_event",
        ),
        pytest.param(
            orjson.dumps(_event(event_time=NOW_MS + FUTURE_S * 1000 + 5000)),
            DlqReason.FUTURE_EVENT,
            id="future_event",
        ),
    ],
)
def test_each_reason_code(raw: bytes, expected: DlqReason) -> None:
    assert _reason(_check(raw)) is expected


@pytest.mark.parametrize(
    "field", ["event_id", "short_code", "event_time", "produced_at", "schema_version"]
)
def test_each_required_field_missing(field: str) -> None:
    result = _check(_without(field))
    assert _reason(result) is DlqReason.MISSING_FIELD
    assert field in result.error  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param({"short_code": "a" * 16}, None, id="short_code-16-valid"),
        pytest.param({"short_code": "a" * 17}, DlqReason.SCHEMA_INVALID, id="short_code-17"),
        pytest.param({"schema_version": 2}, DlqReason.SCHEMA_INVALID, id="schema_version-2"),
        pytest.param({"event_time": NOW_MS - LATE_S * 1000}, None, id="late-exactly-threshold"),
        pytest.param(
            {"event_time": NOW_MS - LATE_S * 1000 - 1}, DlqReason.LATE_EVENT, id="late-plus-1ms"
        ),
        pytest.param({"event_time": NOW_MS + FUTURE_S * 1000}, None, id="future-exactly-skew"),
        pytest.param(
            {"event_time": NOW_MS + FUTURE_S * 1000 + 1},
            DlqReason.FUTURE_EVENT,
            id="future-plus-1ms",
        ),
    ],
)
def test_rules_and_boundaries(overrides: dict[str, object], expected: DlqReason | None) -> None:
    result = _check(orjson.dumps(_event(**overrides)))
    assert _reason(result) is expected
    if expected is None:
        assert isinstance(result, Valid)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(orjson.dumps(_event(short_code=IP)), id="ip-as-short_code"),
        pytest.param(orjson.dumps(_event(event_id=IP, event_time=IP)), id="ip-in-several-fields"),
        pytest.param(orjson.dumps(_event(user_agent="x" * 5000, short_code="!" * 40)), id="long"),
        pytest.param(
            orjson.dumps({f"extra_{i}": IP for i in range(50)} | {"ip": IP}), id="many-missing"
        ),
    ],
)
def test_error_text_is_short_and_never_contains_ip(raw: bytes) -> None:
    result = _check(raw)
    assert isinstance(result, Invalid)
    assert len(result.error) <= MAX_ERROR_LEN
    assert IP not in result.error


def test_classification_ignores_consumer_clock() -> None:
    # An on-time event consumed an hour late (outage, backlog) is still valid ...
    hour_old = _event(event_time=NOW_MS - 3_600_000, produced_at=NOW_MS - 3_600_000)
    assert isinstance(_check(orjson.dumps(hour_old)), Valid)
    # ... while an event produced long after its click is late, whenever it is consumed.
    produced_late = _event(event_time=NOW_MS - 3_600_000, produced_at=NOW_MS)
    assert _reason(_check(orjson.dumps(produced_late))) is DlqReason.LATE_EVENT
