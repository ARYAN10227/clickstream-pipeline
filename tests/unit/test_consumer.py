import threading
import time
import uuid
from typing import Any

import orjson
import pytest
from confluent_kafka import KafkaError, KafkaException

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer, process_batch
from clickpipe.dlq import DlqFlushError
from clickpipe.schema import CLICK_COLUMNS, DlqReason

KAFKA_PARTITION = CLICK_COLUMNS.index("kafka_partition")
KAFKA_OFFSET = CLICK_COLUMNS.index("kafka_offset")
NOW_MS = 1_700_000_000_000


def _value(**overrides: object) -> bytes:
    event: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "short_code": "abc123",
        "event_time": NOW_MS,
        "produced_at": NOW_MS,
        "schema_version": 1,
    }
    return orjson.dumps(event | overrides)


def _live_value() -> bytes:
    """A valid event stamped with the real current time (run() uses the real clock)."""
    now = time.time_ns() // 1_000_000
    return _value(event_time=now, produced_at=now)


class FakeMessage:
    def __init__(
        self,
        value: bytes | None = None,
        partition: int = 0,
        offset: int = 0,
        error: KafkaError | None = None,
    ) -> None:
        self._value, self._partition, self._offset, self._error = value, partition, offset, error

    def value(self) -> bytes | None:
        return self._value

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def topic(self) -> str:
        return "clicks"

    def error(self) -> KafkaError | None:
        return self._error


class FakeConsumer:
    """Returns the given batches one per consume() call, then sets stop_event."""

    def __init__(self, batches: list[list[FakeMessage]], stop: threading.Event, calls: list[str]):
        self.batches, self.stop, self.calls = list(batches), stop, calls

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        batch = self.batches.pop(0) if self.batches else []
        if not self.batches:
            self.stop.set()
        return batch

    def commit(self, asynchronous: bool = True) -> None:
        assert asynchronous is False
        self.calls.append("commit")

    def close(self) -> None:
        self.calls.append("close")


class FakeSink:
    def __init__(self, calls: list[str], fail: bool = False) -> None:
        self.calls, self.fail = calls, fail
        self.rows: list[tuple[Any, ...]] = []
        self.tokens: list[str | None] = []

    def insert_rows(self, rows: list[tuple[Any, ...]], dedup_token: str | None = None) -> int:
        self.calls.append("insert")
        self.tokens.append(dedup_token)
        if self.fail:
            raise ConnectionError("ClickHouse is down")
        self.rows.extend(rows)
        return len(rows)


class FakeDlq:
    def __init__(self, calls: list[str], fail_flush: bool = False) -> None:
        self.calls, self.fail_flush = calls, fail_flush
        self.sent: list[tuple[Any, DlqReason, str]] = []

    def send(self, message: Any, reason: DlqReason, error: str) -> None:
        self.calls.append("dlq.send")
        self.sent.append((message, reason, error))

    def flush(self, timeout_s: float = 10.0) -> None:
        self.calls.append("dlq.flush")
        if self.fail_flush:
            raise DlqFlushError("1 DLQ message(s) still queued")


def _consumer(
    batches: list[list[FakeMessage]], sink_fails: bool = False, flush_fails: bool = False
) -> tuple[ClickConsumer, threading.Event, list[str], FakeSink, FakeDlq]:
    calls: list[str] = []
    stop = threading.Event()
    sink, dlq = FakeSink(calls, fail=sink_fails), FakeDlq(calls, fail_flush=flush_fails)
    consumer = ClickConsumer(
        Settings(clicks_topic="clicks"),
        sink,  # type: ignore[arg-type]
        dlq,  # type: ignore[arg-type]
        consumer=FakeConsumer(batches, stop, calls),  # type: ignore[arg-type]
    )
    return consumer, stop, calls, sink, dlq


def _process(messages: list[FakeMessage]) -> tuple[list[Any], list[Any]]:
    return process_batch(messages, NOW_MS, late_threshold_s=300, future_skew_s=60)  # type: ignore[arg-type]


def test_valid_messages_become_rows_with_partition_and_offset() -> None:
    rows, invalid = _process([FakeMessage(_value(), 2, 10), FakeMessage(_value(), 5, 77)])
    assert invalid == []
    assert [(r[KAFKA_PARTITION], r[KAFKA_OFFSET]) for r in rows] == [(2, 10), (5, 77)]
    assert all(len(r) == len(CLICK_COLUMNS) for r in rows)


def test_invalid_records_get_reason_codes() -> None:
    bad_json = FakeMessage(b'{"event_id": ', partition=1, offset=3)
    late = FakeMessage(_value(event_time=NOW_MS - 301_000))
    rows, invalid = _process([bad_json, late, FakeMessage(_value())])
    assert len(rows) == 1
    assert [(m, reason) for m, reason, _ in invalid] == [
        (bad_json, DlqReason.BAD_JSON),
        (late, DlqReason.LATE_EVENT),
    ]
    assert all(isinstance(error, str) and error for _, _, error in invalid)


def test_error_messages_are_skipped() -> None:
    eof = FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF))
    broken = FakeMessage(error=KafkaError(KafkaError._TRANSPORT))
    rows, invalid = _process([eof, broken, FakeMessage(_value())])
    assert len(rows) == 1
    assert invalid == []


def test_run_passes_dedup_token_from_valid_messages_only() -> None:
    batch = [
        FakeMessage(_live_value(), partition=1, offset=7),
        FakeMessage(b"not json", partition=1, offset=8),  # invalid: excluded from the token
        FakeMessage(_live_value(), partition=0, offset=3),
        FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF), partition=2, offset=99),
    ]
    consumer, stop, _, sink, _ = _consumer([batch])
    consumer.run(stop)
    [token] = sink.tokens
    assert token.startswith("clicks|p0:3-3|p1:7-7|h:")  # invalid p1:8 and the EOF event excluded


def test_run_order_is_insert_dlq_send_dlq_flush_commit() -> None:
    batch = [FakeMessage(_live_value(), offset=0), FakeMessage(b"not json", offset=1)]
    consumer, stop, calls, sink, dlq = _consumer([batch])
    consumer.run(stop)
    assert calls == ["insert", "dlq.send", "dlq.flush", "commit"]
    assert len(sink.rows) == 1
    assert dlq.sent[0][1] is DlqReason.BAD_JSON


def test_run_does_not_commit_when_dlq_flush_fails() -> None:
    batch = [FakeMessage(_live_value()), FakeMessage(b"not json")]
    consumer, stop, calls, _, _ = _consumer([batch], flush_fails=True)
    with pytest.raises(DlqFlushError):
        consumer.run(stop)
    assert calls == ["insert", "dlq.send", "dlq.flush"]


def test_run_does_not_send_dlq_or_commit_when_insert_fails() -> None:
    batch = [FakeMessage(_live_value()), FakeMessage(b"not json")]
    consumer, stop, calls, _, dlq = _consumer([batch], sink_fails=True)
    with pytest.raises(ConnectionError, match="ClickHouse is down"):
        consumer.run(stop)
    assert calls == ["insert"]
    assert dlq.sent == []


def test_run_only_invalid_batch_skips_insert_then_commits() -> None:
    batch = [FakeMessage(b"not json", offset=0), FakeMessage(_value(schema_version=2), offset=1)]
    consumer, stop, calls, _, dlq = _consumer([batch])
    consumer.run(stop)
    assert calls == ["dlq.send", "dlq.send", "dlq.flush", "commit"]
    assert [reason for _, reason, _ in dlq.sent] == [DlqReason.BAD_JSON, DlqReason.SCHEMA_INVALID]


def test_run_only_valid_batch_skips_dlq() -> None:
    consumer, stop, calls, _, _ = _consumer([[FakeMessage(_live_value())]])
    consumer.run(stop)
    assert calls == ["insert", "commit"]


def test_run_empty_poll_does_nothing() -> None:
    consumer, stop, calls, _, _ = _consumer([[], []])
    consumer.run(stop)
    assert calls == []


def test_close_closes_kafka_consumer() -> None:
    consumer, _, calls, _, _ = _consumer([])
    consumer.close()
    assert calls == ["close"]


class StopDuringInsertSink(FakeSink):
    """Simulates SIGTERM arriving while a batch is being inserted."""

    def __init__(self, calls: list[str], stop: threading.Event) -> None:
        super().__init__(calls)
        self.stop = stop

    def insert_rows(self, rows: list[tuple[Any, ...]], dedup_token: str | None = None) -> int:
        self.stop.set()
        return super().insert_rows(rows, dedup_token)


def test_stop_during_processing_finishes_current_batch_then_exits() -> None:
    calls: list[str] = []
    stop = threading.Event()
    batches = [[FakeMessage(_live_value(), offset=0)], [FakeMessage(_live_value(), offset=1)]]
    kafka = FakeConsumer(batches, threading.Event(), calls)  # never sets the real stop itself
    consumer = ClickConsumer(
        Settings(clicks_topic="clicks"),
        StopDuringInsertSink(calls, stop),  # type: ignore[arg-type]
        FakeDlq(calls),  # type: ignore[arg-type]
        consumer=kafka,  # type: ignore[arg-type]
    )
    consumer.run(stop)
    assert calls == ["insert", "commit"]  # first batch finished and committed
    assert len(kafka.batches) == 1  # second batch never polled


class FlakyCommitConsumer(FakeConsumer):
    def __init__(self, batches: list[list[FakeMessage]], stop: threading.Event, calls: list[str]):
        super().__init__(batches, stop, calls)
        self.fail_next_commit = True

    def commit(self, asynchronous: bool = True) -> None:
        if self.fail_next_commit:
            self.fail_next_commit = False
            self.calls.append("commit-failed")
            raise KafkaException(KafkaError(KafkaError.REBALANCE_IN_PROGRESS))
        super().commit(asynchronous)


def test_commit_failure_is_logged_and_loop_continues(caplog: pytest.LogCaptureFixture) -> None:
    calls: list[str] = []
    stop = threading.Event()
    batches = [[FakeMessage(_live_value(), offset=0)], [FakeMessage(_live_value(), offset=1)]]
    consumer = ClickConsumer(
        Settings(clicks_topic="clicks"),
        FakeSink(calls),  # type: ignore[arg-type]
        FakeDlq(calls),  # type: ignore[arg-type]
        consumer=FlakyCommitConsumer(batches, stop, calls),  # type: ignore[arg-type]
    )
    with caplog.at_level("WARNING", logger="clickpipe.consumer"):
        consumer.run(stop)
    assert calls == ["insert", "commit-failed", "insert", "commit"]
    assert "offset commit failed, batch will be redelivered" in caplog.text
