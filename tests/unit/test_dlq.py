from collections.abc import Callable
from typing import Any

import pytest
from confluent_kafka import KafkaError

from clickpipe.config import Settings
from clickpipe.dlq import DlqFlushError, DlqProducer
from clickpipe.schema import DlqReason


class FakeMessage:
    def __init__(self, headers: list[tuple[str, bytes]] | None = None) -> None:
        self._headers = headers

    def value(self) -> bytes:
        return b'{"event_id": '

    def key(self) -> bytes:
        return b"abc123"

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def topic(self) -> str:
        return "clicks"

    def partition(self) -> int:
        return 4

    def offset(self) -> int:
        return 1234


class FakeProducer:
    """Queues produced records; flush() reports them with the given error and remaining count."""

    def __init__(self, deliver_error: KafkaError | None = None, remaining: int = 0) -> None:
        self.records: list[dict[str, Any]] = []
        self.deliver_error, self.remaining = deliver_error, remaining
        self._callbacks: list[Callable[[KafkaError | None, Any], None]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.records.append({"topic": topic, **kwargs})
        self._callbacks.append(kwargs["on_delivery"])

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for callback in self._callbacks:
            callback(self.deliver_error, None)
        self._callbacks.clear()
        return self.remaining


def _dlq(producer: FakeProducer) -> DlqProducer:
    return DlqProducer(Settings(), producer=producer)  # type: ignore[arg-type]


def test_send_produces_original_value_and_key_to_dlq_topic() -> None:
    producer = FakeProducer()
    _dlq(producer).send(FakeMessage(), DlqReason.BAD_JSON, "bad JSON: unexpected end")

    [record] = producer.records
    assert record["topic"] == "clicks-dlq"
    assert record["value"] == b'{"event_id": '
    assert record["key"] == b"abc123"


def test_dlq_headers_present_with_correct_values() -> None:
    producer = FakeProducer()
    _dlq(producer).send(FakeMessage(), DlqReason.LATE_EVENT, "event_time is 400000 ms old")

    headers = dict(producer.records[0]["headers"])
    assert headers["dlq_reason"] == b"late_event"
    assert headers["error"] == b"event_time is 400000 ms old"
    assert headers["source_topic"] == b"clicks"
    assert headers["source_partition"] == b"4"
    assert headers["source_offset"] == b"1234"
    assert headers["failed_at"].isdigit() and len(headers["failed_at"]) == 13
    assert all(isinstance(v, bytes) for v in headers.values())


def test_original_headers_are_preserved() -> None:
    producer = FakeProducer()
    _dlq(producer).send(FakeMessage([("run_id", b"bench-7")]), DlqReason.SCHEMA_INVALID, "x")

    headers = producer.records[0]["headers"]
    assert headers[0] == ("run_id", b"bench-7")
    assert len(headers) == 7


def test_flush_raises_when_messages_remain_queued() -> None:
    dlq = _dlq(FakeProducer(remaining=2))
    dlq.send(FakeMessage(), DlqReason.BAD_JSON, "x")
    with pytest.raises(DlqFlushError, match="2 DLQ message"):
        dlq.flush(timeout_s=0.1)


def test_flush_raises_after_failed_delivery_then_resets() -> None:
    producer = FakeProducer(deliver_error=KafkaError(KafkaError._MSG_TIMED_OUT))
    dlq = _dlq(producer)
    dlq.send(FakeMessage(), DlqReason.BAD_JSON, "x")
    with pytest.raises(DlqFlushError, match="delivery failure"):
        dlq.flush()

    producer.deliver_error = None
    dlq.flush()  # failure record was reset; nothing new failed


def test_flush_succeeds_when_everything_delivered() -> None:
    dlq = _dlq(FakeProducer())
    dlq.send(FakeMessage(), DlqReason.MISSING_FIELD, "event_id: Field required")
    dlq.flush()
    dlq.close()
