"""Rate-controlled producing of click events, shared by the load generator and tests."""

import time
from collections.abc import Callable, Iterable
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


def produce_with_retry(
    producer: Producer,
    topic: str,
    key: str | bytes | None,
    value: bytes,
    headers: list[tuple[str, bytes]] | None,
    on_delivery: Callable[[KafkaError | None, Message], None],
) -> None:
    """produce() one message, waiting for queue space whenever the local queue is full."""
    while True:
        try:
            producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
            return
        except BufferError:
            producer.poll(0.05)  # local queue full: serve delivery callbacks, then retry


def send_events(
    producer: Producer,
    topic: str,
    events: Iterable[dict[str, Any]],
    rate: float,
    duration_s: float | None = None,
    headers: list[tuple[str, bytes]] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> LoadResult:
    """Produce events at `rate`/s, then flush.

    With duration_s, exactly round(rate * duration_s) events are sent (or fewer if `events`
    runs out first); otherwise until `events` is exhausted. Event i is due at i / rate
    seconds after the start, checked in 10 ms ticks, so the final partial tick still sends
    its remaining events. LoadResult.elapsed_s is the sending time (first to last produce),
    excluding the final flush. Events are pulled lazily, so a generator can stamp
    event_time when each event is due; produced_at is set right before produce(). Each
    event is keyed by its short_code. Messages still undelivered after flush count as
    delivery errors. `clock` and `sleep` are injectable for tests.
    """
    if rate <= 0:
        raise ValueError("rate must be > 0")
    total = None if duration_s is None else round(rate * duration_s)
    errors = 0

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    source = iter(events)
    sent = 0
    exhausted = False
    start = clock()
    last_produce = start
    next_tick = start
    while not exhausted and (total is None or sent < total):
        due = int(rate * (clock() - start)) + 1  # events 0..due-1 are due (first one at t=0)
        if total is not None:
            due = min(due, total)
        while sent < due:
            event = next(source, None)
            if event is None:
                exhausted = True
                break
            event["produced_at"] = now_ms()
            value = orjson.dumps(event)
            produce_with_retry(producer, topic, event["short_code"], value, headers, on_delivery)
            sent += 1
            last_produce = clock()
        producer.poll(0)
        if exhausted or (total is not None and sent >= total):
            break
        next_tick += TICK_S
        delay = next_tick - clock()
        if delay > 0:
            sleep(delay)
    remaining = producer.flush(30)
    return LoadResult(sent=sent, elapsed_s=last_produce - start, delivery_errors=errors + remaining)
