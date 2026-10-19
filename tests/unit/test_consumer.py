import threading
import uuid
from typing import Any

import orjson
import pytest
from confluent_kafka import KafkaError

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer, process_batch
from clickpipe.schema import CLICK_COLUMNS

KAFKA_PARTITION = CLICK_COLUMNS.index("kafka_partition")
KAFKA_OFFSET = CLICK_COLUMNS.index("kafka_offset")


def _valid_value() -> bytes:
    return orjson.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "short_code": "abc123",
            "event_time": 1700000000123,
            "produced_at": 1700000000456,
            "schema_version": 1,
        }
    )


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

    def insert_rows(self, rows: list[tuple[Any, ...]], settings: Any = None) -> int:
        self.calls.append("insert")
        if self.fail:
            raise ConnectionError("ClickHouse is down")
        self.rows.extend(rows)
        return len(rows)


def _run(batches: list[list[FakeMessage]], fail: bool = False) -> tuple[list[str], FakeSink]:
    calls: list[str] = []
    stop = threading.Event()
    sink = FakeSink(calls, fail=fail)
    consumer = ClickConsumer(Settings(), sink, consumer=FakeConsumer(batches, stop, calls))  # type: ignore[arg-type]
    consumer.run(stop)
    return calls, sink


def test_valid_messages_become_rows_with_partition_and_offset() -> None:
    rows, invalid = process_batch(
        [FakeMessage(_valid_value(), partition=2, offset=10), FakeMessage(_valid_value(), 5, 77)]  # type: ignore[list-item]
    )
    assert invalid == []
    assert [(r[KAFKA_PARTITION], r[KAFKA_OFFSET]) for r in rows] == [(2, 10), (5, 77)]
    assert all(len(r) == len(CLICK_COLUMNS) for r in rows)


def test_invalid_json_goes_to_invalid_list() -> None:
    bad = FakeMessage(b'{"event_id": ', partition=1, offset=3)
    rows, invalid = process_batch([bad, FakeMessage(_valid_value())])  # type: ignore[list-item]
    assert len(rows) == 1
    assert len(invalid) == 1
    assert invalid[0][0] is bad
    assert invalid[0][1].startswith("bad JSON")


def test_schema_violation_goes_to_invalid_list() -> None:
    value = orjson.loads(_valid_value()) | {"short_code": "x" * 17}
    rows, invalid = process_batch([FakeMessage(orjson.dumps(value))])  # type: ignore[list-item]
    assert rows == []
    assert invalid[0][1].startswith("invalid short_code")


def test_error_messages_are_skipped() -> None:
    eof = FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF))
    broken = FakeMessage(error=KafkaError(KafkaError._TRANSPORT))
    rows, invalid = process_batch([eof, broken, FakeMessage(_valid_value())])  # type: ignore[list-item]
    assert len(rows) == 1
    assert invalid == []


def test_run_commits_after_insert() -> None:
    calls, sink = _run(
        [[FakeMessage(_valid_value(), offset=0)], [FakeMessage(_valid_value(), offset=1)]]
    )
    assert calls == ["insert", "commit", "insert", "commit"]
    assert len(sink.rows) == 2


def test_run_does_not_commit_when_insert_fails() -> None:
    calls: list[str] = []
    stop = threading.Event()
    consumer = ClickConsumer(
        Settings(),
        FakeSink(calls, fail=True),  # type: ignore[arg-type]
        consumer=FakeConsumer([[FakeMessage(_valid_value())]], stop, calls),  # type: ignore[arg-type]
    )
    with pytest.raises(ConnectionError, match="ClickHouse is down"):
        consumer.run(stop)
    assert calls == ["insert"]


def test_run_empty_poll_calls_neither_insert_nor_commit() -> None:
    calls, _ = _run([[], []])
    assert calls == []


def test_close_closes_kafka_consumer() -> None:
    calls: list[str] = []
    consumer = ClickConsumer(
        Settings(), FakeSink(calls), consumer=FakeConsumer([], threading.Event(), calls)
    )  # type: ignore[arg-type]
    consumer.close()
    assert calls == ["close"]
