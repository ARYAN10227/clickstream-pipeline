"""Rate-controlled click event load generator for the `clicks` topic.

Usage: uv run python tools/loadgen.py --count 1000 --rate 1000
       uv run python tools/loadgen.py --duration 300 --rate 20000 --run-id bench-1
"""

import argparse
import itertools
import random
import sys
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Producer

from clickpipe.config import get_settings
from clickpipe.fake import make_event
from clickpipe.producer import now_ms, send_events


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

    def events() -> Iterator[dict[str, Any]]:
        while True:  # event_time is stamped when each event is actually due
            yield make_event(rng, now_ms(), code_prefix=args.code_prefix, n_codes=args.codes)

    stream = itertools.islice(events(), args.count) if args.count is not None else events()
    result = send_events(
        producer,
        settings.clicks_topic,
        stream,
        rate=args.rate,
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
