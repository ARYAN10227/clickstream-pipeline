"""End-to-end latency report for a set of stored clicks (deduplicated with FINAL).

Usage: uv run python tools/latency_report.py --prefix r371e1 [--out docs/results/lat-x]
       uv run python tools/latency_report.py --since 2026-10-19T18:00:00 --until 2026-10-19T19:00:00

Latency is dateDiff('millisecond', produced_at, ingested_at) per event. Consumer throughput
is rows / seconds between the first and last ingested_at. --since/--until filter on
event_time (UTC). With --out PATH, writes PATH.json and PATH.md.
"""

import argparse
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import clickhouse_connect
import orjson

from clickpipe.config import Settings, get_settings

QUERY = """
SELECT
  count() AS rows,
  quantiles(0.5, 0.95, 0.99)(latency_ms) AS q,
  max(latency_ms) AS max_ms,
  min(ingested_at) AS first_ingested,
  max(ingested_at) AS last_ingested
FROM
(
  SELECT dateDiff('millisecond', produced_at, ingested_at) AS latency_ms, ingested_at
  FROM analytics.clicks FINAL
  WHERE {where}
)
"""


@dataclass(frozen=True)
class LatencyStats:
    """Latency and throughput for one selection of rows."""

    selection: str
    rows: int
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    max_ms: int | None
    first_ingested: str | None
    last_ingested: str | None
    ingest_span_s: float | None
    consumer_throughput_eps: float | None


# ---------------------------------------------------------------- pure, unit-tested logic


def throughput(
    rows: int, first: datetime | None, last: datetime | None
) -> tuple[float | None, float | None]:
    """(span seconds, rows per second) between first and last ingest; None if undefined."""
    if rows == 0 or first is None or last is None:
        return None, None
    span = (last - first).total_seconds()
    if span <= 0:
        return span, None
    return span, rows / span


def build_stats(selection: str, row: tuple[Any, ...]) -> LatencyStats:
    """Turn the QUERY result row (rows, quantiles, max, first, last) into LatencyStats."""
    rows, quantiles, max_ms, first, last = row
    rows = int(rows)
    if rows == 0:
        return LatencyStats(selection, 0, None, None, None, None, None, None, None, None)
    span, eps = throughput(rows, first, last)
    p50, p95, p99 = (round(float(v), 1) for v in quantiles)
    return LatencyStats(
        selection=selection,
        rows=rows,
        p50_ms=p50,
        p95_ms=p95,
        p99_ms=p99,
        max_ms=int(max_ms),
        first_ingested=first.isoformat(timespec="milliseconds"),
        last_ingested=last.isoformat(timespec="milliseconds"),
        ingest_span_s=round(span, 3) if span is not None else None,
        consumer_throughput_eps=round(eps, 1) if eps is not None else None,
    )


def _fmt(value: Any, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.1f}{suffix}"
    if isinstance(value, int):
        return f"{value:,}{suffix}"
    return f"{value}{suffix}"


def table_rows(stats: LatencyStats) -> list[tuple[str, str]]:
    """(label, formatted value) pairs shared by the text and Markdown renderers."""
    return [
        ("selection", stats.selection),
        ("rows (FINAL)", _fmt(stats.rows)),
        ("latency p50", _fmt(stats.p50_ms, " ms")),
        ("latency p95", _fmt(stats.p95_ms, " ms")),
        ("latency p99", _fmt(stats.p99_ms, " ms")),
        ("latency max", _fmt(stats.max_ms, " ms")),
        ("first ingested_at", _fmt(stats.first_ingested)),
        ("last ingested_at", _fmt(stats.last_ingested)),
        ("ingest span", _fmt(stats.ingest_span_s, " s")),
        ("consumer throughput", _fmt(stats.consumer_throughput_eps, " events/s")),
    ]


def render_text(stats: LatencyStats) -> str:
    """Aligned plain-text table."""
    rows = table_rows(stats)
    width = max(len(label) for label, _ in rows)
    return "\n".join(f"{label:<{width}}  {value}" for label, value in rows)


def render_markdown(stats: LatencyStats) -> str:
    """Markdown table."""
    lines = ["# End-to-end latency report", "", "| metric | value |", "|---|---|"]
    lines += [f"| {label} | {value} |" for label, value in table_rows(stats)]
    return "\n".join(lines) + "\n"


def where_clause(
    prefix: str | None, since: datetime | None, until: datetime | None
) -> tuple[str, dict[str, Any], str]:
    """(SQL condition, query parameters, human description) for the selection."""
    if prefix:
        return "startsWith(short_code, {prefix:String})", {"prefix": prefix}, f"prefix {prefix}"
    if since is None or until is None:
        raise ValueError("pass --prefix, or both --since and --until")
    cond = "event_time >= {since:DateTime64(3)} AND event_time < {until:DateTime64(3)}"
    params = {"since": since.replace(tzinfo=None), "until": until.replace(tzinfo=None)}
    return cond, params, f"event_time {since.isoformat()} .. {until.isoformat()}"


# ---------------------------------------------------------------- I/O


def run_report(
    prefix: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    settings: Settings | None = None,
) -> LatencyStats:
    """Query ClickHouse and return the stats for the selection."""
    settings = settings or get_settings()
    cond, params, description = where_clause(prefix, since, until)
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )
    try:
        row = client.query(QUERY.format(where=cond), parameters=params).result_rows[0]
    finally:
        client.close()
    return build_stats(description, row)


def write_outputs(stats: LatencyStats, out: Path) -> tuple[Path, Path]:
    """Write <out>.json and <out>.md; returns both paths."""
    out.parent.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out.with_suffix(".json"), out.with_suffix(".md")
    json_path.write_bytes(orjson.dumps(asdict(stats), option=orjson.OPT_INDENT_2) + b"\n")
    md_path.write_text(render_markdown(stats))
    return json_path, md_path


def _utc(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prefix", help="short_code prefix of the run")
    parser.add_argument("--since", type=_utc, help="ISO time, event_time >= since (UTC default)")
    parser.add_argument("--until", type=_utc, help="ISO time, event_time < until (UTC default)")
    parser.add_argument("--out", type=Path, help="write <out>.json and <out>.md")
    args = parser.parse_args(argv)
    if not args.prefix and not (args.since and args.until):
        parser.error("pass --prefix, or both --since and --until")

    stats = run_report(args.prefix, args.since, args.until)
    print(render_text(stats))
    if args.out:
        for path in write_outputs(stats, args.out):
            print(f"wrote {path}")
    return 0 if stats.rows else 1


if __name__ == "__main__":
    sys.exit(main())
