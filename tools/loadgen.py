"""Rate-controlled click event load generator for the `clicks` topic.

Usage: uv run python tools/loadgen.py --count 1000 --rate 1000
       uv run python tools/loadgen.py --duration 300 --rate 20000 --run-id bench-1
"""

import argparse
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import orjson
from confluent_kafka import KafkaError, Message, Producer

from clickpipe.config import get_settings
from clickpipe.fake import make_event

TICK_S = 0.01


@dataclass
class LoadResult:
    sent: int
    elapsed_s: float
    delivery_errors: int

    @property
    def rate(self) -> float:
        return self.sent / self.elapsed_s if self.elapsed_s > 0 else 0.0


def now_ms() -> int:
    """Current wall-clock time in epoch milliseconds."""
    return time.time_ns() // 1_000_000


def send_events(
    producer: Producer,
    topic: str,
    next_event: Callable[[int], dict[str, Any]],
    rate: float,
    count: int | None = None,
    duration_s: float | None = None,
    headers: list[tuple[str, bytes]] | None = None,
) -> LoadResult:
    """Produce events at `rate`/s until `count` events or `duration_s` seconds, then flush.

    `next_event(now_ms)` returns one event dict; its produced_at is set right before
    produce(). Sending is paced in 10 ms ticks.
    """
    if (count is None) == (duration_s is None):
        raise ValueError("pass exactly one of count or duration_s")
    errors = 0

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    sent = 0
    start = time.monotonic()
    next_tick = start
    while True:
        elapsed = time.monotonic() - start
        if duration_s is not None and elapsed >= duration_s:
            break
        target = int(rate * elapsed) + 1  # events due by now (send the first one immediately)
        if count is not None:
            target = min(target, count)
        while sent < target:
            event = next_event(now_ms())
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
        if count is not None and sent >= count:
            break
        next_tick += TICK_S
        delay = next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    remaining = producer.flush(30)
    elapsed = time.monotonic() - start
    return LoadResult(sent=sent, elapsed_s=elapsed, delivery_errors=errors + remaining)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=float, default=1000, help="events per second")
    stop = parser.add_mutually_exclusive_group(required=True)
    stop.add_argument("--duration", type=float, help="seconds to run")
    stop.add_argument("--count", type=int, help="number of events to send")
    parser.add_argument("--codes", type=int, default=1000, help="size of the short_code pool")
    parser.add_argument("--code-prefix", default="", help="prefix for every short_code")
    parser.add_argument("--run-id", help="sent as Kafka header run_id on every message")
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap, help="Kafka bootstrap")
    parser.add_argument("--seed", type=int, default=None, help="random seed (default: random)")
    args = parser.parse_args(argv)
    if args.rate <= 0:
        parser.error("--rate must be > 0")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the load generator and print a summary; exit code 1 on delivery errors."""
    args = parse_args(argv)
    settings = get_settings()
    config = settings.producer_config() | {"bootstrap.servers": args.bootstrap}
    producer = Producer(config)
    rng = random.Random(args.seed)
    headers = [("run_id", args.run_id.encode())] if args.run_id else None

    def next_event(ts_ms: int) -> dict[str, Any]:
        return make_event(rng, ts_ms, code_prefix=args.code_prefix, n_codes=args.codes)

    result = send_events(
        producer,
        settings.clicks_topic,
        next_event,
        rate=args.rate,
        count=args.count,
        duration_s=args.duration,
        headers=headers,
    )
    print(f"events sent:     {result.sent}")
    print(f"elapsed:         {result.elapsed_s:.2f} s")
    print(f"actual rate:     {result.rate:,.0f} events/s (target {args.rate:,.0f})")
    print(f"delivery errors: {result.delivery_errors}")
    return 1 if result.delivery_errors else 0


if __name__ == "__main__":
    sys.exit(main())
