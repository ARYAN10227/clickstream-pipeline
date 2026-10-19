# Results

Measured results for the click-event pipeline. Every number here is copied from the
linked result files, which were written by the tools in `tools/`.

- [Correctness](correctness.md): 1M-event replay with duplicates, bad and late records, and
  the `kill -9` crash test.
- Baseline benchmark: below.

## Baseline

`make bench RATE=20000 DURATION=300` with **1 consumer replica**, run on 2026-10-19.

Files: [`bench-20261019T194024.md`](bench-20261019T194024.md),
[`bench-20261019T194024.json`](bench-20261019T194024.json).

| metric | value |
|---|---|
| target rate | 20,000 events/s for 300 s (1 producer process) |
| achieved produce rate | 19,999 events/s (5,999,802 sent, 0 delivery errors) |
| consumer throughput | 20,019.4 events/s |
| rows stored (FINAL) | 5,999,802 |
| max consumer lag | 8,059 |
| final consumer lag | 0 |
| latency p50 / p95 / p99 / max | 237.0 / 363.0 / 407.0 / 1,047 ms |

| SPEC target | requirement | measured | result |
|---|---|---|---|
| sustained throughput | >= 20,000 events/s for 300 s, lag drained | 19,999 events/s for 300 s, final lag 0 | **FAIL** |
| end-to-end latency p99 | < 2,000 ms | 407.0 ms | PASS |

**The sustained-throughput target FAILED** in this run: the achieved produce rate was
19,999 events/s against a target of at least 20,000 (5,999,802 events sent in the run;
6,000,000 would be exactly 20,000/s for 300 s). The consumer kept up: its throughput was
20,019.4 events/s, the lag never exceeded 8,059 messages and drained to 0, and all
5,999,802 produced events were stored. The shortfall is on the producing side, where the
load generator stops at the 300 s mark. The latency target passed with a large margin
(p99 407.0 ms vs 2,000 ms).

Machine: a cloud VM (sandboxed Linux container) with an Intel(R) Xeon(R) Processor @
2.10GHz, 4 vCPUs, 15.7 GiB RAM, Ubuntu 24.04.5 LTS, Docker Engine 29.8.2. Redpanda,
ClickHouse, the consumer, Prometheus, Grafana and the load generator all ran on this one
machine. ClickHouse ran with `nofile` 20000 instead of 262144 because of a sandbox limit.
