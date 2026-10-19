"""kill -9 the consumer while events are flowing, restart it, and prove zero loss.

Usage: uv run python tools/crash_test.py --count 300000 --rate 20000 --kill-after 5

Produces --count events (unique short_code prefix, header run_id) in a background thread,
SIGKILLs the consumer container mid-stream, starts it again, waits for lag 0 and checks
that every event is in ClickHouse exactly once. Report: docs/results/crash-<run_id>.md.
"""

import argparse
import hashlib
import itertools
import random
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Producer

from clickpipe.config import get_settings
from clickpipe.fake import make_event
from clickpipe.producer import LoadResult, now_ms, send_events
from clickpipe.stackcheck import clickhouse_counts, wait_for_zero_lag

RESULTS_DIR = Path(__file__).resolve().parents[1] / "docs" / "results"


@dataclass(frozen=True)
class Check:
    name: str
    expected: int
    actual: int

    @property
    def passed(self) -> bool:
        return self.expected == self.actual


@dataclass(frozen=True)
class CrashResult:
    """Everything measured during one crash test run."""

    run_id: str
    prefix: str
    produced: int
    delivery_errors: int
    raw_count: int
    final_count: int
    unique_event_ids: int
    killed_at: str
    restarted_at: str
    lag_at_end: int


# ---------------------------------------------------------------- pure, unit-tested logic


def build_checks(result: CrashResult) -> list[Check]:
    """Zero loss (unique ids == produced) and zero duplicates (FINAL == produced)."""
    return [
        Check(
            "zero loss: uniqExact(event_id) == produced", result.produced, result.unique_event_ids
        ),
        Check("zero duplicates: count() FINAL == produced", result.produced, result.final_count),
    ]


def info_rows(result: CrashResult) -> list[tuple[str, str]]:
    """Information-only figures for the report."""
    return [
        ("consumer killed (SIGKILL) at", result.killed_at),
        ("consumer restarted at", result.restarted_at),
        ("producer delivery errors", f"{result.delivery_errors:,}"),
        ("raw count()", f"{result.raw_count:,}"),
        ("duplicates collapsed (raw - FINAL)", f"{result.raw_count - result.final_count:,}"),
        ("consumer lag at verification", f"{result.lag_at_end:,}"),
    ]


def exit_code(checks: list[Check]) -> int:
    """0 if every check passed, else 1."""
    return 0 if all(c.passed for c in checks) else 1


def render_text(result: CrashResult, checks: list[Check]) -> str:
    """Plain-text PASS/FAIL report."""
    info = info_rows(result)
    width = max(len(name) for name in [c.name for c in checks] + [n for n, _ in info])
    lines = [
        f"crash test {result.run_id} (prefix {result.prefix})",
        "",
        f"{'check':<{width}}  {'expected':>10}  {'actual':>10}  result",
        "-" * (width + 32),
    ]
    for c in checks:
        status = "PASS" if c.passed else "FAIL"
        lines.append(f"{c.name:<{width}}  {c.expected:>10,}  {c.actual:>10,}  {status}")
    lines.append("")
    lines += [f"{name:<{width}}  {value}" for name, value in info]
    lines += ["", f"{sum(c.passed for c in checks)}/{len(checks)} checks passed"]
    return "\n".join(lines)


def render_markdown(result: CrashResult, checks: list[Check]) -> str:
    """Markdown version of the report."""
    passed = sum(c.passed for c in checks)
    lines = [
        f"# Crash test: {result.run_id}",
        "",
        f"- prefix: `{result.prefix}`, produced: {result.produced:,} events",
        f"- result: **{passed}/{len(checks)} checks passed**",
        "",
        "| check | expected | actual | result |",
        "|---|---:|---:|---|",
    ]
    lines += [
        f"| {c.name} | {c.expected:,} | {c.actual:,} | {'PASS' if c.passed else 'FAIL'} |"
        for c in checks
    ]
    lines += ["", "| info | value |", "|---|---|"]
    lines += [f"| {name} | {value} |" for name, value in info_rows(result)]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- orchestration


def _stamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)


def main(argv: list[str] | None = None) -> int:
    """Run the crash test and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=300_000, help="events to produce")
    parser.add_argument("--rate", type=float, default=20_000, help="events per second")
    parser.add_argument("--kill-after", type=float, default=5, help="seconds before SIGKILL")
    parser.add_argument("--restart-after", type=float, default=3, help="seconds before restart")
    parser.add_argument("--timeout", type=float, default=300, help="seconds to wait for lag 0")
    args = parser.parse_args(argv)

    settings = get_settings()
    run_id = "crash-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    prefix = "k" + hashlib.sha1(run_id.encode()).hexdigest()[:5]
    print(f"run {run_id}: {args.count:,} events at {args.rate:,.0f}/s, prefix {prefix}")

    rng = random.Random()

    def events() -> Iterator[dict[str, Any]]:
        while True:
            yield make_event(rng, now_ms(), code_prefix=prefix)

    outcome: dict[str, LoadResult] = {}

    def produce() -> None:
        producer = Producer(settings.producer_config())
        outcome["load"] = send_events(
            producer,
            settings.clicks_topic,
            itertools.islice(events(), args.count),
            rate=args.rate,
            headers=[("run_id", run_id.encode())],
        )

    sender = threading.Thread(target=produce, daemon=True)
    sender.start()

    time.sleep(args.kill_after)
    _compose("kill", "-s", "SIGKILL", "consumer")
    killed_at = _stamp()
    print(f"{killed_at}  consumer killed with SIGKILL")
    time.sleep(args.restart_after)
    _compose("start", "consumer")
    restarted_at = _stamp()
    print(f"{restarted_at}  consumer started again")

    sender.join()
    load = outcome["load"]
    print(f"producer done: {load.sent:,} sent, {load.delivery_errors} delivery errors")

    lag = wait_for_zero_lag(settings, args.timeout)
    raw, final, unique = clickhouse_counts(settings, prefix)
    result = CrashResult(
        run_id=run_id,
        prefix=prefix,
        produced=load.sent - load.delivery_errors,
        delivery_errors=load.delivery_errors,
        raw_count=raw,
        final_count=final,
        unique_event_ids=unique,
        killed_at=killed_at,
        restarted_at=restarted_at,
        lag_at_end=lag,
    )
    checks = build_checks(result)
    print()
    print(render_text(result, checks))

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    report = RESULTS_DIR / f"crash-{run_id.removeprefix('crash-')}.md"
    report.write_text(render_markdown(result, checks))
    print(f"\nreport: {report.relative_to(RESULTS_DIR.parents[1])}")
    return exit_code(checks)


if __name__ == "__main__":
    sys.exit(main())
