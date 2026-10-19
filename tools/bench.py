"""Benchmark: produce at RATE for DURATION, measure lag, latency and throughput vs SPEC targets.

Usage: uv run python tools/bench.py --rate 20000 --duration 300 [--procs 2]

Runs tools/loadgen.py (split over --procs processes) with a unique short_code prefix,
samples total consumer lag every 5 s, waits for lag 0, then runs the latency report for
the prefix. Writes docs/results/bench-<timestamp>.json and .md. PASS/FAIL is computed here.
"""

import argparse
import hashlib
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent))  # sibling tools (latency_report)
from latency_report import LatencyStats, run_report  # noqa: E402

from clickpipe.config import get_settings  # noqa: E402
from clickpipe.stackcheck import total_lag, wait_for_zero_lag  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "docs" / "results"
LAG_SAMPLE_S = 5
TARGET_THROUGHPUT_EPS = 20_000
TARGET_DURATION_S = 300
TARGET_P99_MS = 2_000
CLOUD_VENDORS = ("Google", "Amazon", "Microsoft", "DigitalOcean", "Hetzner", "OpenStack", "Xen")


@dataclass(frozen=True)
class ProducerResult:
    sent: int
    elapsed_s: float
    delivery_errors: int


@dataclass(frozen=True)
class Check:
    name: str
    target: str
    actual: str
    passed: bool


# ---------------------------------------------------------------- pure, unit-tested logic


def parse_loadgen_output(text: str) -> ProducerResult:
    """Extract sent / elapsed / delivery errors from tools/loadgen.py's summary."""

    def find(pattern: str) -> str:
        match = re.search(pattern, text)
        if match is None:
            raise ValueError(f"loadgen output has no match for {pattern!r}:\n{text}")
        return match.group(1)

    return ProducerResult(
        sent=int(find(r"events sent:\s+(\d+)")),
        elapsed_s=float(find(r"elapsed:\s+([\d.]+) s")),
        delivery_errors=int(find(r"delivery errors:\s+(\d+)")),
    )


def achieved_rate(results: list[ProducerResult]) -> tuple[int, int, float]:
    """(total sent, total delivery errors, events/s) across parallel producers."""
    sent = sum(r.sent for r in results)
    errors = sum(r.delivery_errors for r in results)
    longest = max((r.elapsed_s for r in results), default=0.0)
    return sent, errors, (sent / longest if longest > 0 else 0.0)


def evaluate(
    duration_s: float, produce_rate: float, latency: LatencyStats, final_lag: int
) -> list[Check]:
    """PASS/FAIL against the SPEC targets."""
    consumer_eps = latency.consumer_throughput_eps or 0.0
    sustained = min(produce_rate, consumer_eps)
    p99 = latency.p99_ms
    return [
        Check(
            "sustained throughput",
            f">= {TARGET_THROUGHPUT_EPS:,} events/s for {TARGET_DURATION_S} s, lag drained",
            f"{sustained:,.0f} events/s (min of produce {produce_rate:,.0f}, "
            f"consumer {consumer_eps:,.0f}) for {duration_s:g} s, final lag {final_lag:,}",
            sustained >= TARGET_THROUGHPUT_EPS
            and duration_s >= TARGET_DURATION_S
            and final_lag == 0,
        ),
        Check(
            "end-to-end latency p99",
            f"< {TARGET_P99_MS:,} ms",
            "n/a" if p99 is None else f"{p99:,.1f} ms",
            p99 is not None and p99 < TARGET_P99_MS,
        ),
    ]


def cloud_vm_status(override: str, dmi_vendor: str | None, hypervisor: bool) -> str:
    """Human-readable answer to "is this a cloud VM?"."""
    if override == "yes":
        return "yes (stated with --cloud-vm yes)"
    if override == "no":
        return "no (stated with --cloud-vm no)"
    if dmi_vendor and any(v.lower() in dmi_vendor.lower() for v in CLOUD_VENDORS):
        return f"yes (DMI vendor: {dmi_vendor})"
    if hypervisor:
        return "likely (hypervisor detected, cloud vendor unknown)"
    return "no virtualization detected"


def render_markdown(report: dict[str, Any]) -> str:
    """Markdown report from the result dict."""
    m, r, lat = report["machine"], report["run"], report["latency"]
    passed = sum(c["passed"] for c in report["checks"])
    lines = [
        f"# Benchmark {report['bench_id']}",
        "",
        f"**{passed}/{len(report['checks'])} SPEC targets passed.**",
        "",
        "## Results",
        "",
        "| metric | value |",
        "|---|---|",
        f"| target rate | {r['target_rate']:,} events/s for {r['duration_s']:g} s "
        f"({r['procs']} producer process{'es' if r['procs'] > 1 else ''}) |",
        f"| achieved produce rate | {r['achieved_produce_rate']:,.0f} events/s "
        f"({r['produced']:,} sent, {r['delivery_errors']:,} delivery errors) |",
        f"| consumer throughput | {_num(lat['consumer_throughput_eps'], ' events/s')} |",
        f"| rows stored (FINAL) | {lat['rows']:,} |",
        f"| max consumer lag | {r['max_lag']:,} |",
        f"| final consumer lag | {r['final_lag']:,} |",
        f"| latency p50 / p95 / p99 / max | {_num(lat['p50_ms'])} / {_num(lat['p95_ms'])} / "
        f"{_num(lat['p99_ms'])} / {_num(lat['max_ms'])} ms |",
        "",
        "## SPEC targets",
        "",
        "| target | requirement | measured | result |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| {c['name']} | {c['target']} | {c['actual']} | {'PASS' if c['passed'] else 'FAIL'} |"
        for c in report["checks"]
    ]
    lines += [
        "",
        "## Machine",
        "",
        "| | |",
        "|---|---|",
        f"| CPU | {m['cpu_model']} |",
        f"| vCPUs (nproc) | {m['nproc']} |",
        f"| RAM | {m['ram_gib']} GiB |",
        f"| OS | {m['os']} |",
        f"| Docker | {m['docker']} |",
        f"| consumer replicas | {m['consumer_replicas']} |",
        f"| cloud VM | {m['cloud_vm']} |",
        "",
        f"Run: prefix `{r['prefix']}`, started {r['started_at']}, lag samples every "
        f"{LAG_SAMPLE_S} s: {r['lag_samples']}",
    ]
    return "\n".join(lines) + "\n"


def _num(value: float | int | None, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:,.1f}{suffix}" if isinstance(value, float) else f"{value:,}{suffix}"


# ---------------------------------------------------------------- machine and stack


def _run(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True, cwd=REPO_ROOT).stdout
    except (OSError, subprocess.CalledProcessError):
        return ""


def machine_specs(cloud_override: str) -> dict[str, Any]:
    """CPU, cores, RAM, OS, Docker version, consumer replicas, cloud VM answer."""
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in _run("lscpu").splitlines()
            if line.startswith("Model name:")
        ),
        platform.processor() or "unknown",
    )
    mem_kb = next(
        (
            int(line.split()[1])
            for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemTotal:")
        ),
        0,
    )
    os_release = dict(
        line.split("=", 1)
        for line in Path("/etc/os-release").read_text().splitlines()
        if "=" in line
    )
    dmi = Path("/sys/class/dmi/id/sys_vendor")
    hypervisor = " hypervisor " in f" {Path('/proc/cpuinfo').read_text()} "
    replicas = [
        x
        for x in _run("docker", "compose", "ps", "--status", "running", "-q", "consumer").split()
        if x
    ]
    return {
        "cpu_model": cpu,
        "nproc": os.cpu_count(),
        "ram_gib": round(mem_kb / 1024 / 1024, 1),
        "os": f"{os_release.get('PRETTY_NAME', '').strip(chr(34))}, kernel {platform.release()}",
        "docker": _run("docker", "version", "--format", "Engine {{.Server.Version}}").strip()
        or "unknown",
        "consumer_replicas": len(replicas),
        "cloud_vm": cloud_vm_status(
            cloud_override, dmi.read_text().strip() if dmi.exists() else None, hypervisor
        ),
    }


def start_producers(
    rate: float, duration: float, procs: int, prefix: str, run_id: str
) -> list[subprocess.Popen[str]]:
    """Start `procs` loadgen processes that split the rate evenly."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "tools" / "loadgen.py"),
        "--duration",
        str(duration),
        "--rate",
        str(rate / procs),
        "--code-prefix",
        prefix,
        "--run-id",
        run_id,
    ]
    return [
        subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=REPO_ROOT
        )
        for _ in range(procs)
    ]


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark; exit 1 if any SPEC target fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=float, default=20_000, help="target events/s (total)")
    parser.add_argument("--duration", type=float, default=300, help="seconds of producing")
    parser.add_argument("--procs", type=int, default=1, help="producer processes sharing the rate")
    parser.add_argument("--lag-timeout", type=float, default=300, help="seconds to wait for lag 0")
    parser.add_argument("--cloud-vm", choices=["auto", "yes", "no"], default="auto")
    parser.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    args = parser.parse_args(argv)
    if args.procs < 1 or args.rate <= 0 or args.duration <= 0:
        parser.error("--procs >= 1, --rate > 0 and --duration > 0 required")

    settings = get_settings()
    started = datetime.now(UTC)
    bench_id = started.strftime("%Y%m%dT%H%M%S")
    prefix = "b" + hashlib.sha1(bench_id.encode()).hexdigest()[:5]
    machine = machine_specs(args.cloud_vm)
    print(
        f"bench {bench_id}: {args.rate:,.0f} events/s for {args.duration:g} s over "
        f"{args.procs} process(es), prefix {prefix}, "
        f"{machine['consumer_replicas']} consumer replica(s)"
    )

    procs = start_producers(args.rate, args.duration, args.procs, prefix, f"bench-{bench_id}")
    lag_samples: list[int] = []
    while any(p.poll() is None for p in procs):
        lag_samples.append(total_lag(settings))
        print(f"  t+{(datetime.now(UTC) - started).total_seconds():5.0f}s  lag {lag_samples[-1]:,}")
        deadline = time.monotonic() + LAG_SAMPLE_S
        while time.monotonic() < deadline and any(p.poll() is None for p in procs):
            time.sleep(0.2)
    outputs = [p.communicate()[0] for p in procs]
    results = [parse_loadgen_output(out) for out in outputs]
    produced, errors, produce_rate = achieved_rate(results)
    print(f"producers done: {produced:,} sent at {produce_rate:,.0f} events/s, {errors} errors")

    final_lag = wait_for_zero_lag(settings, args.lag_timeout)
    lag_samples.append(final_lag)
    latency = run_report(prefix=prefix, settings=settings)
    checks = evaluate(args.duration, produce_rate, latency, final_lag)

    report = {
        "bench_id": bench_id,
        "machine": machine,
        "run": {
            "prefix": prefix,
            "started_at": started.isoformat(timespec="seconds"),
            "target_rate": int(args.rate),
            "duration_s": args.duration,
            "procs": args.procs,
            "produced": produced,
            "delivery_errors": errors,
            "achieved_produce_rate": round(produce_rate, 1),
            "max_lag": max(lag_samples),
            "final_lag": final_lag,
            "lag_samples": lag_samples,
        },
        "latency": asdict(latency),
        "checks": [asdict(c) for c in checks],
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    base = args.out_dir / f"bench-{bench_id}"
    base.with_suffix(".json").write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2) + b"\n")
    base.with_suffix(".md").write_text(render_markdown(report))

    print()
    for c in checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name}: {c.actual} (target {c.target})")
    try:
        shown = base.relative_to(REPO_ROOT)
    except ValueError:
        shown = base
    print(f"\nreport: {shown}.md / .json")
    return 0 if all(c.passed for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
