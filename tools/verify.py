"""Verify a replay run end to end: ClickHouse rows and DLQ records vs the replay manifest.

Usage: uv run python tools/verify.py --manifest docs/results/replay-<run_id>.json

1. waits until the consumer group has committed everything on the clicks topic (lag 0)
2. counts the run's rows in ClickHouse (short_code prefix)
3. counts the run's DLQ records (run_id header) by dlq_reason
Prints an expected-vs-actual table, writes it to docs/results/verify-<run_id>.md and
exits 1 if any check fails.
"""

import argparse
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import clickhouse_connect
import orjson
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from clickpipe.config import Settings, get_settings

RESULTS_DIR = Path(__file__).resolve().parents[1] / "docs" / "results"
DLQ_REASONS = ("bad_json", "missing_field", "schema_invalid", "late_event")
PROGRESS_EVERY_S = 5


@dataclass(frozen=True)
class Check:
    name: str
    expected: int
    actual: int

    @property
    def passed(self) -> bool:
        return self.expected == self.actual


@dataclass(frozen=True)
class Actual:
    """Measured results for one run."""

    raw_count: int
    final_count: int
    unique_event_ids: int
    dlq_by_reason: dict[str, int]


# ---------------------------------------------------------------- pure, unit-tested logic


def expected_dlq(manifest: dict[str, Any]) -> dict[str, int]:
    """Expected DLQ records per reason from a replay manifest."""
    bad = manifest["bad_by_reason"]
    return {
        "bad_json": bad["bad_json"],
        "missing_field": bad["missing_field"],
        "schema_invalid": bad["schema_invalid"],
        "late_event": manifest["late"],
    }


def build_checks(manifest: dict[str, Any], actual: Actual) -> list[Check]:
    """Compare a manifest with measured results; one Check per rule."""
    n = manifest["n_unique_valid"]
    want_dlq = expected_dlq(manifest)
    checks = [
        Check("count() FINAL == n_unique_valid", n, actual.final_count),
        Check("uniqExact(event_id) == n_unique_valid", n, actual.unique_event_ids),
        Check(
            "DLQ total == bad + late",
            sum(want_dlq.values()),
            sum(actual.dlq_by_reason.get(r, 0) for r in DLQ_REASONS),
        ),
    ]
    checks += [
        Check(f"DLQ {reason}", want_dlq[reason], actual.dlq_by_reason.get(reason, 0))
        for reason in DLQ_REASONS
    ]
    return checks


def info_rows(actual: Actual) -> list[tuple[str, int]]:
    """Information-only figures (not pass/fail)."""
    unexpected = {k: v for k, v in actual.dlq_by_reason.items() if k not in DLQ_REASONS}
    rows = [
        ("raw count()", actual.raw_count),
        ("duplicates not yet merged (raw - FINAL)", actual.raw_count - actual.final_count),
    ]
    rows += [(f"DLQ other reason: {k}", v) for k, v in sorted(unexpected.items())]
    return rows


def exit_code(checks: list[Check]) -> int:
    """0 if every check passed, else 1."""
    return 0 if all(c.passed for c in checks) else 1


def render_table(checks: list[Check], info: list[tuple[str, int]]) -> str:
    """Plain-text expected vs actual table."""
    width = max(len(c.name) for c in checks + [Check(name, 0, 0) for name, _ in info])
    lines = [f"{'check':<{width}}  {'expected':>10}  {'actual':>10}  result"]
    lines.append("-" * len(lines[0]))
    for c in checks:
        result = "PASS" if c.passed else "FAIL"
        lines.append(f"{c.name:<{width}}  {c.expected:>10,}  {c.actual:>10,}  {result}")
    lines.append("")
    lines += [f"{name:<{width}}  {'':>10}  {value:>10,}  info" for name, value in info]
    passed = sum(c.passed for c in checks)
    lines += ["", f"{passed}/{len(checks)} checks passed"]
    return "\n".join(lines)


def render_markdown(
    manifest: dict[str, Any], checks: list[Check], info: list[tuple[str, int]], verified_at: str
) -> str:
    """Markdown report with the same content as render_table."""
    passed = sum(c.passed for c in checks)
    lines = [
        f"# Replay verification: {manifest['run_id']}",
        "",
        f"- prefix: `{manifest['prefix']}`, seed: {manifest['seed']}",
        f"- produced: {manifest['produced_total']:,} messages "
        f"({manifest['n_unique_valid']:,} unique valid, "
        f"{manifest['duplicates_sent']:,} duplicates)",
        f"- replay: {manifest['started_at']} to {manifest['finished_at']}; verified: {verified_at}",
        f"- result: **{passed}/{len(checks)} checks passed**",
        "",
        "| check | expected | actual | result |",
        "|---|---:|---:|---|",
    ]
    lines += [
        f"| {c.name} | {c.expected:,} | {c.actual:,} | {'PASS' if c.passed else 'FAIL'} |"
        for c in checks
    ]
    lines += ["", "| info | value |", "|---|---:|"]
    lines += [f"| {name} | {value:,} |" for name, value in info]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- I/O against the stack


def _partitions(consumer: Consumer, topic: str) -> list[TopicPartition]:
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    if meta.error is not None:
        raise RuntimeError(f"topic {topic}: {meta.error}")
    return [TopicPartition(topic, p) for p in sorted(meta.partitions)]


def total_lag(settings: Settings) -> int:
    """Uncommitted messages for the consumer group across every clicks partition."""
    reader = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap, "group.id": settings.consumer_group}
    )
    try:
        parts = _partitions(reader, settings.clicks_topic)
        lag = 0
        for tp in reader.committed(parts, timeout=10):
            low, high = reader.get_watermark_offsets(tp, timeout=10)
            position = tp.offset if tp.offset >= 0 else low  # no commit yet: start of log
            lag += max(0, high - position)
        return lag
    finally:
        reader.close()


def wait_for_zero_lag(settings: Settings, timeout_s: float) -> int:
    """Poll until lag is 0 or the timeout passes; returns the last lag seen."""
    deadline = time.monotonic() + timeout_s
    next_report = 0.0
    while True:
        lag = total_lag(settings)
        now = time.monotonic()
        if lag == 0 or now >= deadline:
            print(f"consumer lag: {lag:,}" + ("" if lag == 0 else " (timed out)"))
            return lag
        if now >= next_report:
            print(f"waiting for consumer, lag: {lag:,}")
            next_report = now + PROGRESS_EVERY_S
        time.sleep(1)


def clickhouse_counts(settings: Settings, prefix: str) -> tuple[int, int, int]:
    """(raw count, FINAL count, uniqExact(event_id)) for rows of this run."""
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )
    try:
        where = "startsWith(short_code, {prefix:String})"
        params = {"prefix": prefix}
        raw = client.command(f"SELECT count() FROM analytics.clicks WHERE {where}", params)
        final = client.command(f"SELECT count() FROM analytics.clicks FINAL WHERE {where}", params)
        unique = client.command(
            f"SELECT uniqExact(event_id) FROM analytics.clicks WHERE {where}", params
        )
        return int(raw), int(final), int(unique)
    finally:
        client.close()


def dlq_counts(settings: Settings, run_id: str, timeout_s: float = 120) -> dict[str, int]:
    """Read the DLQ topic from the start (throwaway group) and count this run's records."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"verify-{uuid.uuid4().hex[:12]}",
            "enable.auto.commit": False,
        }
    )
    counts: Counter[str] = Counter()
    try:
        parts = _partitions(consumer, settings.dlq_topic)
        ends = {tp.partition: consumer.get_watermark_offsets(tp, timeout=10) for tp in parts}
        pending = {p for p, (low, high) in ends.items() if high > low}
        consumer.assign([TopicPartition(settings.dlq_topic, p, OFFSET_BEGINNING) for p in pending])
        wanted = run_id.encode()
        deadline = time.monotonic() + timeout_s
        while pending:
            if time.monotonic() > deadline:
                raise TimeoutError(f"DLQ read did not reach the end of partitions {pending}")
            for msg in consumer.consume(num_messages=10_000, timeout=1):
                if msg.error() is not None:
                    continue
                headers = dict(msg.headers() or [])
                if headers.get("run_id") == wanted:
                    counts[headers.get("dlq_reason", b"?").decode()] += 1
                if msg.offset() + 1 >= ends[msg.partition()][1]:
                    pending.discard(msg.partition())
    finally:
        consumer.close()
    return dict(counts)


def main(argv: list[str] | None = None) -> int:
    """Run the verification and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", required=True, type=Path, help="replay-<run_id>.json")
    parser.add_argument("--timeout", type=float, default=300, help="seconds to wait for lag 0")
    args = parser.parse_args(argv)

    manifest = orjson.loads(args.manifest.read_bytes())
    settings = get_settings()
    print(f"verifying run {manifest['run_id']} (prefix {manifest['prefix']})")

    lag = wait_for_zero_lag(settings, args.timeout)
    raw, final, unique = clickhouse_counts(settings, manifest["prefix"])
    actual = Actual(raw, final, unique, dlq_counts(settings, manifest["run_id"]))

    checks = build_checks(manifest, actual)
    info = info_rows(actual) + [("consumer lag at verification", lag)]
    print()
    print(render_table(checks, info))

    verified_at = datetime.now(UTC).isoformat(timespec="seconds")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    report = RESULTS_DIR / f"verify-{manifest['run_id']}.md"
    report.write_text(render_markdown(manifest, checks, info, verified_at))
    print(f"\nreport: {report.relative_to(RESULTS_DIR.parents[1])}")
    return exit_code(checks)


if __name__ == "__main__":
    sys.exit(main())
