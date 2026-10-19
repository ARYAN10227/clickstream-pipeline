import itertools
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from helpers import load_tool

from clickpipe.producer import send_events

bench = load_tool("bench")


class FakeClock:
    """Monotonic clock that only moves when slept on (plus a tiny cost per produce)."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeProducer:
    def __init__(self, clock: FakeClock, cost_s: float = 1e-6) -> None:
        self.clock, self.cost_s = clock, cost_s
        self.produced = 0
        self.callbacks: list[Callable[..., None]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.produced += 1
        self.clock.now += self.cost_s
        self.callbacks.append(kwargs["on_delivery"])

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for callback in self.callbacks:
            callback(None, None)
        self.callbacks.clear()
        return 0


def _events() -> Iterator[dict[str, Any]]:
    for i in itertools.count():
        yield {"event_id": str(i), "short_code": "abc"}


def _run(rate: float, duration: float, events: Iterator[dict[str, Any]] | None = None):  # type: ignore[no-untyped-def]
    clock = FakeClock()
    producer = FakeProducer(clock)
    result = send_events(
        producer,  # type: ignore[arg-type]
        "clicks",
        events if events is not None else _events(),
        rate=rate,
        duration_s=duration,
        clock=clock,
        sleep=clock.sleep,
    )
    return result, producer


@pytest.mark.parametrize(("rate", "duration", "expected"), [(1000, 3, 3000), (20_000, 5, 100_000)])
def test_duration_mode_sends_exactly_rate_times_duration(
    rate: float, duration: float, expected: int
) -> None:
    result, producer = _run(rate, duration)

    assert result.sent == producer.produced == expected
    assert result.delivery_errors == 0
    # Event i is due at i / rate, so sending spans (expected - 1) / rate seconds.
    assert result.elapsed_s == pytest.approx((expected - 1) / rate, abs=0.02)
    assert result.rate == pytest.approx(rate, rel=0.01)
    assert result.rate >= rate * 0.999


def test_count_mode_stops_when_events_run_out() -> None:
    result, producer = _run(1000, 3, events=itertools.islice(_events(), 1234))
    assert result.sent == producer.produced == 1234


def test_procs_split_sums_to_total() -> None:
    assert bench.split_counts(6_000_000, 2) == [3_000_000, 3_000_000]
    assert sum(bench.split_counts(100_001, 2)) == 100_001
    assert bench.split_counts(100_001, 2) == [50_001, 50_000]
    assert sum(bench.split_counts(10, 3)) == 10


def test_two_procs_each_send_their_half_at_half_rate() -> None:
    rate, duration = 20_000, 5
    total = round(rate * duration)
    halves = bench.split_counts(total, 2)
    sent = []
    for count in halves:
        result, _ = _run(rate * count / total, None, events=itertools.islice(_events(), count))  # type: ignore[arg-type]
        sent.append(result.sent)
        assert result.elapsed_s == pytest.approx(duration, abs=0.02)
    assert sent == halves == [50_000, 50_000]
    assert sum(sent) == total
