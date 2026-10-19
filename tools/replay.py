"""Replay generator: unique valid events plus injected duplicates, bad and late records.

Usage: uv run python tools/replay.py --n 1000000 --dup 0.05 --bad 0.01 --late 0.01

Every message carries the Kafka header run_id, and every short_code starts with a prefix
derived from run_id, so a run's rows can be counted in ClickHouse afterwards:
    SELECT uniqExact(event_id) FROM analytics.clicks WHERE startsWith(short_code, '<prefix>')
should equal n_unique_valid. A manifest is written to docs/results/replay-<run_id>.json.
"""

import argparse
import hashlib
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
from confluent_kafka import KafkaError, Message, Producer

from clickpipe.config import Settings, get_settings
from clickpipe.fake import make_event
from clickpipe.producer import now_ms, produce_with_retry

RESULTS_DIR = Path(__file__).resolve().parents[1] / "docs" / "results"
REQUIRED_FIELDS = ("event_id", "short_code", "event_time", "produced_at", "schema_version")
BAD_REASONS = ("bad_json", "missing_field", "schema_invalid")
DUP_SOON_WINDOW = 100  # "soon" duplicates land within this many messages of the original


@dataclass(frozen=True)
class PlanItem:
    """One message to send. kind: valid | late | bad_json | missing_field | schema_invalid | dup."""

    kind: str
    key: bytes
    value: bytes
    dup_of: int | None = None  # for kind == "dup": index (in Plan.items) of the original


@dataclass
class Plan:
    items: list[PlanItem]
    counts: dict[str, Any] = field(default_factory=dict)


def make_prefix(run_id: str) -> str:
    """'r' + 5 lowercase hex chars derived from run_id (valid short_code characters)."""
    return "r" + hashlib.sha1(run_id.encode()).hexdigest()[:5]


def _split(total: int, parts: int) -> list[int]:
    """Split total into `parts` near-equal integer counts (earlier parts get the remainder)."""
    base, extra = divmod(total, parts)
    return [base + (1 if i < extra else 0) for i in range(parts)]


def _bad_record(reason: str, rng: random.Random, prefix: str, now: int, i: int) -> bytes:
    event = make_event(rng, now, code_prefix=prefix)
    if reason == "bad_json":
        return orjson.dumps(event)[: rng.randrange(5, 40)]  # truncated JSON
    if reason == "missing_field":
        del event[REQUIRED_FIELDS[i % len(REQUIRED_FIELDS)]]
        return orjson.dumps(event)
    variants = (
        {"short_code": prefix + "x" * 20},  # too long for ^[A-Za-z0-9_-]{1,16}$
        {"schema_version": 2},
        {"event_id": "not-a-uuid"},
        {"event_time": "yesterday"},
    )
    return orjson.dumps(event | variants[i % len(variants)])


def build_plan(
    n: int,
    dup: float,
    bad: float,
    late: float,
    seed: int,
    prefix: str,
    now_ms: int,
    late_threshold_s: int = 300,
) -> Plan:
    """Return the ordered messages for a replay run plus the expected counts. Pure."""
    rng = random.Random(seed)
    n_dup, n_bad, n_late = round(dup * n), round(bad * n), round(late * n)

    base: list[PlanItem] = []
    for _ in range(n):
        event = make_event(rng, now_ms, code_prefix=prefix)
        base.append(PlanItem("valid", event["short_code"].encode(), orjson.dumps(event)))
    late_ms = now_ms - 2 * late_threshold_s * 1000
    for _ in range(n_late):
        event = make_event(rng, late_ms, code_prefix=prefix)
        event["produced_at"] = now_ms  # clicked long ago, produced now -> late_event
        base.append(PlanItem("late", event["short_code"].encode(), orjson.dumps(event)))
    bad_counts = dict(zip(BAD_REASONS, _split(n_bad, len(BAD_REASONS)), strict=True))
    for reason, count in bad_counts.items():
        for i in range(count):
            base.append(PlanItem(reason, b"bad", _bad_record(reason, rng, prefix, now_ms, i)))
    rng.shuffle(base)

    # Duplicates: each copies a random earlier valid message and is inserted after it,
    # half of them soon after (within DUP_SOON_WINDOW messages), half anywhere later.
    valid_positions = [i for i, item in enumerate(base) if item.kind == "valid"]
    inserts: dict[int, list[int]] = {}  # base position after which to insert -> originals
    for k in range(n_dup):
        original = rng.choice(valid_positions)
        if k % 2 == 0:
            after = min(len(base) - 1, original + rng.randint(0, DUP_SOON_WINDOW))
        else:
            after = rng.randint(original, len(base) - 1)
        inserts.setdefault(after, []).append(original)

    items: list[PlanItem] = []
    final_index: dict[int, int] = {}  # base position -> index in items
    for pos, item in enumerate(base):
        final_index[pos] = len(items)
        items.append(item)
        for original in inserts.get(pos, ()):
            src = base[original]
            items.append(PlanItem("dup", src.key, src.value, dup_of=final_index[original]))

    counts = {
        "n_unique_valid": n,
        "duplicates_sent": n_dup,
        "bad_by_reason": bad_counts,
        "late": n_late,
        "produced_total": len(items),
    }
    return Plan(items=items, counts=counts)


def _stamp_produced_at(value: bytes) -> bytes:
    """Re-serialize an event with produced_at set to now (valid and late events only)."""
    event = orjson.loads(value)
    event["produced_at"] = now_ms()
    return orjson.dumps(event)


def send_plan(
    producer: Producer, topic: str, plan: Plan, run_id: str
) -> tuple[int, int, Counter[str]]:
    """Send every plan item as fast as possible; returns (sent, delivery_errors, kinds)."""
    errors = 0

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        nonlocal errors
        if err is not None:
            errors += 1

    headers = [("run_id", run_id.encode())]
    sent_values: dict[int, bytes] = {}  # index -> exact bytes sent (originals of duplicates)
    needed = {item.dup_of for item in plan.items if item.dup_of is not None}
    kinds: Counter[str] = Counter()
    for index, item in enumerate(plan.items):
        if item.kind == "dup":
            value = sent_values[item.dup_of]  # type: ignore[index]  # byte-identical copy
        elif item.kind in ("valid", "late"):
            value = _stamp_produced_at(item.value)
        else:
            value = item.value
        if index in needed:
            sent_values[index] = value
        produce_with_retry(producer, topic, item.key, value, headers, on_delivery)
        kinds[item.kind] += 1
        if index % 1000 == 0:
            producer.poll(0)
    remaining = producer.flush(60)
    return sum(kinds.values()), errors + remaining, kinds


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=1_000_000, help="unique valid events")
    parser.add_argument("--dup", type=float, default=0.05, help="duplicates, as a fraction of n")
    parser.add_argument("--bad", type=float, default=0.01, help="invalid records, fraction of n")
    parser.add_argument("--late", type=float, default=0.01, help="late events, fraction of n")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--run-id", default=datetime.now(UTC).strftime("%Y%m%dT%H%M%S"), help="Kafka header"
    )
    parser.add_argument("--bootstrap", default=settings.kafka_bootstrap, help="Kafka bootstrap")
    args = parser.parse_args(argv)
    if args.n < 1 or min(args.dup, args.bad, args.late) < 0:
        parser.error("--n must be >= 1 and fractions must be >= 0")
    return args


@dataclass
class ReplayResult:
    """Outcome of run_replay()."""

    manifest: dict[str, Any]
    manifest_path: Path
    kinds: Counter[str]
    delivery_errors: int
    elapsed_s: float


def run_replay(
    n: int,
    dup: float,
    bad: float,
    late: float,
    seed: int,
    run_id: str,
    bootstrap: str | None = None,
    results_dir: Path = RESULTS_DIR,
    settings: Settings | None = None,
) -> ReplayResult:
    """Plan and send a replay run, write replay-<run_id>.json to results_dir, return it."""
    settings = settings or get_settings()
    prefix = make_prefix(run_id)
    started = datetime.now(UTC)
    plan = build_plan(
        n, dup, bad, late, seed, prefix, now_ms(), late_threshold_s=settings.late_threshold_s
    )
    config = settings.producer_config() | {
        "bootstrap.servers": bootstrap or settings.kafka_bootstrap
    }
    _, errors, kinds = send_plan(Producer(config), settings.clicks_topic, plan, run_id)
    finished = datetime.now(UTC)

    manifest = {
        "run_id": run_id,
        "prefix": prefix,
        "seed": seed,
        **plan.counts,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": finished.isoformat(timespec="seconds"),
    }
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / f"replay-{run_id}.json"
    path.write_bytes(orjson.dumps(manifest, option=orjson.OPT_INDENT_2) + b"\n")
    return ReplayResult(manifest, path, kinds, errors, (finished - started).total_seconds())


def display_path(path: Path) -> str:
    """Path relative to the repository root when possible."""
    try:
        return str(path.relative_to(RESULTS_DIR.parents[1]))
    except ValueError:
        return str(path)


def main(argv: list[str] | None = None) -> int:
    """Plan, send and write the manifest; exit code 1 on delivery errors."""
    args = parse_args(argv)
    result = run_replay(
        args.n, args.dup, args.bad, args.late, args.seed, args.run_id, bootstrap=args.bootstrap
    )
    kinds, elapsed, sent = result.kinds, result.elapsed_s, sum(result.kinds.values())
    print(f"run_id:           {args.run_id}  (prefix {result.manifest['prefix']})")
    print(f"sent:             {sent} in {elapsed:.1f} s ({sent / max(elapsed, 1e-9):,.0f} msg/s)")
    print(f"  valid:          {kinds['valid']}")
    print(f"  duplicates:     {kinds['dup']}")
    print(f"  late:           {kinds['late']}")
    for reason in BAD_REASONS:
        print(f"  {reason + ':':<16}{kinds[reason]}")
    print(f"delivery errors:  {result.delivery_errors}")
    print(f"manifest:         {display_path(result.manifest_path)}")
    return 1 if result.delivery_errors else 0


if __name__ == "__main__":
    sys.exit(main())
