from datetime import UTC, datetime, timedelta

import pytest
from helpers import load_tool

latency_report = load_tool("latency_report")
T0 = datetime(2026, 10, 20, 0, 0, 0, tzinfo=UTC)


def test_throughput() -> None:
    assert latency_report.throughput(1000, T0, T0 + timedelta(seconds=4)) == (4.0, 250.0)
    assert latency_report.throughput(0, None, None) == (None, None)
    assert latency_report.throughput(5, T0, T0) == (0.0, None)  # all ingested at once


def test_build_stats_rounds_and_computes_throughput() -> None:
    row = (20_000, [354.0, 622.25, 782.04], 1031, T0, T0 + timedelta(seconds=8))
    stats = latency_report.build_stats("prefix abc", row)
    assert (stats.rows, stats.p50_ms, stats.p95_ms, stats.p99_ms, stats.max_ms) == (
        20_000,
        354.0,
        622.2,
        782.0,
        1031,
    )
    assert stats.ingest_span_s == 8.0
    assert stats.consumer_throughput_eps == 2500.0
    assert stats.first_ingested == "2026-10-20T00:00:00.000+00:00"


def test_build_stats_with_no_rows() -> None:
    stats = latency_report.build_stats("prefix none", (0, [0.0, 0.0, 0.0], 0, T0, T0))
    assert stats.rows == 0
    assert stats.p99_ms is None and stats.consumer_throughput_eps is None


def test_renderers_include_every_metric() -> None:
    row = (3, [10.0, 20.0, 30.0], 31, T0, T0 + timedelta(seconds=1))
    stats = latency_report.build_stats("prefix x", row)
    text = latency_report.render_text(stats)
    markdown = latency_report.render_markdown(stats)
    for label, value in latency_report.table_rows(stats):
        assert label in text and value in text
        assert f"| {label} | {value} |" in markdown
    assert "3.0 events/s" in text
    empty = latency_report.render_text(latency_report.build_stats("y", (0, [0, 0, 0], 0, T0, T0)))
    assert "n/a" in empty


def test_where_clause() -> None:
    cond, params, desc = latency_report.where_clause("r1abc", None, None)
    assert "startsWith" in cond and params == {"prefix": "r1abc"} and desc == "prefix r1abc"
    cond, params, _ = latency_report.where_clause(None, T0, T0 + timedelta(hours=1))
    assert "event_time >=" in cond and params["since"].tzinfo is None
    with pytest.raises(ValueError):
        latency_report.where_clause(None, T0, None)


def test_write_outputs(tmp_path) -> None:  # type: ignore[no-untyped-def]
    stats = latency_report.build_stats("p", (2, [1.0, 2.0, 3.0], 3, T0, T0 + timedelta(seconds=2)))
    json_path, md_path = latency_report.write_outputs(stats, tmp_path / "lat-run")
    assert json_path.name == "lat-run.json" and md_path.name == "lat-run.md"
    assert '"consumer_throughput_eps": 1.0' in json_path.read_text()
    assert "| latency p99 | 3.0 ms |" in md_path.read_text()
