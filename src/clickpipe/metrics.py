"""Prometheus metrics for the consumer.

All collectors live on one registry: the process default in production (served by
start_http_server), or a fresh CollectorRegistry in tests.
"""

import logging
import time
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer
from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

log = logging.getLogger(__name__)

LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5)
BATCH_SIZE_BUCKETS = (1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000)
INSERT_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
LAG_REFRESH_S = 5.0


class Metrics:
    """The consumer's Prometheus collectors plus the rate-limited lag refresh."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.registry = registry
        self.events_consumed = Counter(
            "clickpipe_events_consumed",
            "Kafka messages consumed (valid and invalid, excluding Kafka error events)",
            registry=registry,
        )
        self.events_inserted = Counter(
            "clickpipe_events_inserted",
            "Rows inserted into ClickHouse (before deduplication)",
            registry=registry,
        )
        self.events_dlq = Counter(
            "clickpipe_events_dlq",
            "Records delivered to the dead-letter topic",
            ["reason"],
            registry=registry,
        )
        self.batch_size = Histogram(
            "clickpipe_batch_size",
            "Messages per consumed batch",
            buckets=BATCH_SIZE_BUCKETS,
            registry=registry,
        )
        self.insert_duration = Histogram(
            "clickpipe_insert_duration_seconds",
            "Time spent in one batch insert, including retries",
            buckets=INSERT_DURATION_BUCKETS,
            registry=registry,
        )
        self.e2e_latency = Histogram(
            "clickpipe_e2e_latency_seconds",
            "Time from produced_at to a successful ClickHouse insert, per event",
            buckets=LATENCY_BUCKETS,
            registry=registry,
        )
        self.consumer_lag = Gauge(
            "clickpipe_consumer_lag",
            "Messages between this consumer's position and the high watermark",
            ["partition"],
            registry=registry,
        )
        self._next_lag_refresh = 0.0

    def observe_e2e_latency(
        self, produced_at: Sequence[datetime], now_s: float | None = None
    ) -> None:
        """Record now - produced_at for each inserted event."""
        now_s = time.time() if now_s is None else now_s
        for ts in produced_at:
            self.e2e_latency.observe(max(0.0, now_s - ts.timestamp()))

    def maybe_refresh_lag(self, consumer: Consumer, now_s: float | None = None) -> bool:
        """Refresh the lag gauge from watermarks vs position, at most every LAG_REFRESH_S.

        Must be called from the thread that owns `consumer`. Never raises: metrics must
        not break the pipeline. Returns True if a refresh was attempted.
        """
        now_s = time.monotonic() if now_s is None else now_s
        if now_s < self._next_lag_refresh:
            return False
        self._next_lag_refresh = now_s + LAG_REFRESH_S
        try:
            assigned = consumer.assignment()
            lags: dict[str, int] = {}
            for tp in consumer.position(assigned) if assigned else []:
                _low, high = consumer.get_watermark_offsets(tp, timeout=2)
                if tp.offset >= 0:  # no position yet (nothing consumed) -> skip
                    lags[str(tp.partition)] = max(0, high - tp.offset)
        except Exception as exc:
            log.debug("lag refresh skipped: %s", type(exc).__name__)
            return True
        self.consumer_lag.clear()  # drop partitions that are no longer assigned
        for partition, lag in lags.items():
            self.consumer_lag.labels(partition=partition).set(lag)
        return True


def sample(metrics: Metrics, name: str, labels: dict[str, Any] | None = None) -> float | None:
    """Current value of one sample (for tests and debugging)."""
    return metrics.registry.get_sample_value(name, labels or {})
