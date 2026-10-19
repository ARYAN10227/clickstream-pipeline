from helpers import load_tool

bench = load_tool("bench")
latency_report = load_tool("latency_report")

LOADGEN_OUT = """events sent:     150000
elapsed:         30.02 s
actual rate:     4,997 events/s (target 5,000)
delivery errors: 0
"""


def _latency(p99: float | None = 900.0, eps: float | None = 21_000.0):  # type: ignore[no-untyped-def]
    return latency_report.LatencyStats(
        "prefix b", 6_000_000, 400.0, 800.0, p99, 1500, "a", "b", 300.0, eps
    )


def test_parse_loadgen_output() -> None:
    assert bench.parse_loadgen_output(LOADGEN_OUT) == bench.ProducerResult(150_000, 30.02, 0)


def test_achieved_rate_uses_longest_producer() -> None:
    results = [bench.ProducerResult(100_000, 10.0, 0), bench.ProducerResult(100_000, 12.5, 2)]
    assert bench.achieved_rate(results) == (200_000, 2, 16_000.0)


def test_all_targets_pass() -> None:
    checks = bench.evaluate(300, 20_100.0, _latency(), final_lag=0)
    assert [c.passed for c in checks] == [True, True]


def test_throughput_fails_when_too_slow_too_short_or_lagging() -> None:
    assert not bench.evaluate(300, 19_000.0, _latency(), 0)[0].passed  # producer too slow
    assert not bench.evaluate(300, 20_100.0, _latency(eps=15_000.0), 0)[0].passed  # consumer
    assert not bench.evaluate(30, 20_100.0, _latency(), 0)[0].passed  # shorter than 5 min
    assert not bench.evaluate(300, 20_100.0, _latency(), 1_234)[0].passed  # lag not drained


def test_latency_target_is_strict_and_handles_missing() -> None:
    assert not bench.evaluate(300, 20_100.0, _latency(p99=2000.0), 0)[1].passed
    assert bench.evaluate(300, 20_100.0, _latency(p99=1999.9), 0)[1].passed
    assert not bench.evaluate(300, 20_100.0, _latency(p99=None), 0)[1].passed


def test_cloud_vm_status() -> None:
    assert bench.cloud_vm_status("yes", None, False).startswith("yes")
    assert bench.cloud_vm_status("auto", "Google", True) == "yes (DMI vendor: Google)"
    assert bench.cloud_vm_status("auto", None, True).startswith("likely")
    assert bench.cloud_vm_status("auto", None, False) == "no virtualization detected"


def test_markdown_shows_numbers_and_computed_results() -> None:
    checks = bench.evaluate(30, 4_997.0, _latency(eps=5_100.0), 0)
    report = {
        "bench_id": "20261020T000000",
        "machine": {
            "cpu_model": "Xeon",
            "nproc": 4,
            "ram_gib": 15.7,
            "os": "Ubuntu",
            "docker": "Engine 29",
            "consumer_replicas": 1,
            "cloud_vm": "likely",
        },
        "run": {
            "prefix": "b12345",
            "started_at": "t",
            "target_rate": 5000,
            "duration_s": 30.0,
            "procs": 1,
            "produced": 150_000,
            "delivery_errors": 0,
            "achieved_produce_rate": 4997.0,
            "max_lag": 4000,
            "final_lag": 0,
            "lag_samples": [0, 4000, 0],
        },
        "latency": _latency(eps=5_100.0).__dict__,
        "checks": [c.__dict__ for c in checks],
    }
    md = bench.render_markdown(report)
    assert "**1/2 SPEC targets passed.**" in md
    assert "| sustained throughput |" in md and "| FAIL |" in md and "| PASS |" in md
    assert "4,997 events/s" in md and "| max consumer lag | 4,000 |" in md
    assert "400.0 / 800.0 / 900.0 / 1,500 ms" in md
