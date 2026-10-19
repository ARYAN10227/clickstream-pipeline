import random
import threading
import time
import uuid
from typing import Any

import orjson
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer
from clickpipe.schema import CLICK_COLUMNS
from clickpipe.sink import ClickHouseSink, is_transient

ROW = tuple(range(len(CLICK_COLUMNS)))


class FlakyClient:
    """insert() raises the queued errors in order, then succeeds."""

    def __init__(self, errors: list[Exception], always: Exception | None = None) -> None:
        self.errors, self.always = list(errors), always
        self.attempts = 0

    def insert(self, table: str, rows: Any, **kwargs: Any) -> None:
        self.attempts += 1
        if self.always is not None:
            raise self.always
        if self.errors:
            raise self.errors.pop(0)

    def close(self) -> None:
        pass


def _sink(client: FlakyClient, sleeps: list[float], retries: int = 8) -> ClickHouseSink:
    settings = Settings(insert_max_retries=retries, retry_base_s=0.2, retry_max_s=5.0)
    return ClickHouseSink(
        settings,
        client=client,  # type: ignore[arg-type]
        sleep=sleeps.append,
        rng=random.Random(0),
    )


def test_transient_failures_are_retried_with_growing_delays() -> None:
    client = FlakyClient([OperationalError("connection refused"), ConnectionResetError()])
    sleeps: list[float] = []

    assert _sink(client, sleeps).insert_rows([ROW], dedup_token="clicks|p0:1-1") == 1

    assert client.attempts == 3
    assert len(sleeps) == 2
    assert 0.1 <= sleeps[0] <= 0.2  # 0.2 * 2**0 * jitter(0.5..1.0)
    assert 0.2 <= sleeps[1] <= 0.4  # 0.2 * 2**1 * jitter
    assert sleeps[1] > sleeps[0]


def test_gives_up_after_insert_max_retries_attempts() -> None:
    client = FlakyClient([], always=OperationalError("connection refused"))
    sleeps: list[float] = []

    with pytest.raises(OperationalError):
        _sink(client, sleeps, retries=8).insert_rows([ROW])

    assert client.attempts == 8
    assert len(sleeps) == 7
    assert max(sleeps) <= 5.0  # capped at RETRY_MAX_S


def test_non_transient_error_is_raised_immediately() -> None:
    unknown_column = DatabaseError("Code: 16. DB::Exception: No such column", code=16)
    client = FlakyClient([], always=unknown_column)
    sleeps: list[float] = []

    with pytest.raises(DatabaseError):
        _sink(client, sleeps).insert_rows([ROW])

    assert client.attempts == 1
    assert sleeps == []


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (OperationalError("refused"), True),
        (TimeoutError(), True),
        (DatabaseError("Code: 241. Memory limit exceeded", code=241), True),
        (DatabaseError("Code: 60. Unknown table", code=60), False),
        (ValueError("bad row"), False),
    ],
)
def test_is_transient(exc: Exception, expected: bool) -> None:
    assert is_transient(exc) is expected


class OneBatchConsumer:
    def __init__(self, batch: list[Any], stop: threading.Event) -> None:
        self.batch, self.stop, self.commits = batch, stop, 0

    def consume(self, num_messages: int, timeout: float) -> list[Any]:
        self.stop.set()
        return self.batch

    def commit(self, asynchronous: bool = True) -> None:
        self.commits += 1


class ValidMessage:
    def __init__(self) -> None:
        now = time.time_ns() // 1_000_000
        self._value = orjson.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "short_code": "abc123",
                "event_time": now,
                "produced_at": now,
                "schema_version": 1,
            }
        )

    def value(self) -> bytes:
        return self._value

    def error(self) -> None:
        return None

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def topic(self) -> str:
        return "clicks"


def test_consumer_never_commits_when_insert_finally_fails() -> None:
    client = FlakyClient([], always=OperationalError("connection refused"))
    sleeps: list[float] = []
    stop = threading.Event()
    kafka = OneBatchConsumer([ValidMessage()], stop)
    consumer = ClickConsumer(
        Settings(clicks_topic="clicks"),
        _sink(client, sleeps, retries=3),
        dlq=None,  # type: ignore[arg-type]  # not reached: no invalid records
        consumer=kafka,  # type: ignore[arg-type]
    )

    with pytest.raises(OperationalError):
        consumer.run(stop)

    assert client.attempts == 3
    assert kafka.commits == 0
