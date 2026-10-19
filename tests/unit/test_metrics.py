import threading
import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import TopicPartition
from prometheus_client import CollectorRegistry
from test_consumer import FakeConsumer, FakeDlq, FakeMessage, FakeSink, _live_value

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer
from clickpipe.metrics import Metrics, sample


def _metrics() -> Metrics:
    return Metrics(CollectorRegistry())


def test_consumer_records_batch_metrics() -> None:
    metrics = _metrics()
    calls: list[str] = []
    stop = threading.Event()
    batch = [
        FakeMessage(_live_value(), offset=0),
        FakeMessage(_live_value(), offset=1),
        FakeMessage(b"not json", offset=2),
    ]
    consumer = ClickConsumer(
        Settings(clicks_topic="clicks"),
        FakeSink(calls),  # type: ignore[arg-type]
        FakeDlq(calls),  # type: ignore[arg-type]
        consumer=FakeConsumer([batch], stop, calls),  # type: ignore[arg-type]
        metrics=metrics,
    )
    consumer.run(stop)

    assert sample(metrics, "clickpipe_events_consumed_total") == 3
    assert sample(metrics, "clickpipe_events_inserted_total") == 2
    assert sample(metrics, "clickpipe_events_dlq_total", {"reason": "bad_json"}) == 1
    assert sample(metrics, "clickpipe_batch_size_count") == 1
    assert sample(metrics, "clickpipe_batch_size_sum") == 3
    assert sample(metrics, "clickpipe_insert_duration_seconds_count") == 1
    assert sample(metrics, "clickpipe_e2e_latency_seconds_count") == 2


def test_e2e_latency_buckets() -> None:
    metrics = _metrics()
    now = 1_700_000_010.0
    produced = [datetime.fromtimestamp(now - lag, UTC) for lag in (0.04, 0.3, 1.2, 9.0)]
    metrics.observe_e2e_latency(produced, now_s=now)

    def le(bound: str) -> float | None:
        return sample(metrics, "clickpipe_e2e_latency_seconds_bucket", {"le": bound})

    assert (le("0.05"), le("0.5"), le("1.5"), le("5.0"), le("+Inf")) == (1, 2, 3, 3, 4)


class LagConsumer:
    """Two assigned partitions: positions 90 and 40, high watermarks 100 and 40."""

    def __init__(self) -> None:
        self.watermark_calls = 0

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition("clicks", 0), TopicPartition("clicks", 1)]

    def position(self, parts: list[TopicPartition]) -> list[TopicPartition]:
        return [TopicPartition("clicks", 0, 90), TopicPartition("clicks", 1, 40)]

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float) -> tuple[int, int]:
        self.watermark_calls += 1
        return (0, 100) if tp.partition == 0 else (0, 40)


def test_lag_gauge_refreshes_at_most_every_5_seconds() -> None:
    metrics, kafka = _metrics(), LagConsumer()

    assert metrics.maybe_refresh_lag(kafka, now_s=100.0) is True  # type: ignore[arg-type]
    assert sample(metrics, "clickpipe_consumer_lag", {"partition": "0"}) == 10
    assert sample(metrics, "clickpipe_consumer_lag", {"partition": "1"}) == 0
    assert kafka.watermark_calls == 2

    assert metrics.maybe_refresh_lag(kafka, now_s=104.9) is False  # type: ignore[arg-type]
    assert kafka.watermark_calls == 2  # no broker calls inside the 5 s window

    assert metrics.maybe_refresh_lag(kafka, now_s=105.0) is True  # type: ignore[arg-type]
    assert kafka.watermark_calls == 4


def test_lag_refresh_never_raises() -> None:
    class Broken:
        def assignment(self) -> Any:
            raise RuntimeError("broker gone")

    metrics = _metrics()
    assert metrics.maybe_refresh_lag(Broken(), now_s=time.monotonic()) is True  # type: ignore[arg-type]
    assert sample(metrics, "clickpipe_consumer_lag", {"partition": "0"}) is None
