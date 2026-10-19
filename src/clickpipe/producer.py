"""Rate-controlled producing of click events, shared by the load generator and tests."""

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import orjson
from confluent_kafka import KafkaError, Message, Producer

TICK_S = 0.01


@dataclass
class LoadResult:
    """Outcome of one send_events() run."""

    sent: int
    elapsed_s: float
    delivery_errors: int

    @property
    def rate(self) -> float:
        """Average events per second over the whole run, including the final flush."""
        return self.sent / self.elapsed_s if self.elapsed_s > 0 else 0.0


def now_ms() -> int:
    """Current wall-clock time in epoch milliseconds."""
    return time.time_ns() // 1_000_000


def send_events(
    producer: Producer,
    topic: str,
    events: Iterable[dict[str, Any]],
    rate: float,
    duration_s: float | None = None,
    headers: list[tuple[str, bytes]] | None = None,
) -> LoadResult:
    """Produce events at `rate`/s until `events` is exhausted or `duration_s` passes, then flush.

    Events are pulled lazily, so a generator can stamp event_time when each event is due.
    produced_at is set right before produce(). Sending is paced in 10 ms ticks; each event
    is keyed by its short_code. Messages still undelivered after flush count as errors.
    """
    if rate <= 0:
        raise ValueError("rate must be > 0")
    errors = 0

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    source = iter(events)
    sent = 0
    exhausted = False
    start = time.monotonic()
    next_tick = start
    while not exhausted:
        elapsed = time.monotonic() - start
        if duration_s is not None and elapsed >= duration_s:
            break
        target = int(rate * elapsed) + 1  # events due by now (send the first one immediately)
        while sent < target:
            event = next(source, None)
            if event is None:
                exhausted = True
                break
            event["produced_at"] = now_ms()
            value = orjson.dumps(event)
            while True:
                try:
                    producer.produce(
                        topic,
                        key=event["short_code"],
                        value=value,
                        headers=headers,
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.05)  # local queue full: serve callbacks, then retry
            sent += 1
        producer.poll(0)
        next_tick += TICK_S
        delay = next_tick - time.monotonic()
        if delay > 0 and not exhausted:
            time.sleep(delay)
    remaining = producer.flush(30)
    elapsed = time.monotonic() - start
    return LoadResult(sent=sent, elapsed_s=elapsed, delivery_errors=errors + remaining)
