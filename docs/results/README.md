# Results

Measured results for the click-event pipeline. Every number here is copied from the
linked result files, which were written by the tools in `tools/`.

- [Correctness](correctness.md): 1M-event replay with duplicates, bad and late records, and
  the `kill -9` crash test.
- Baseline benchmark: below.

## Baseline

`make bench RATE=20000 DURATION=300` with **1 consumer replica** and 1 producer process,
run twice on 2026-10-19 on the same machine.

| run | achieved produce rate | events sent | consumer throughput | max lag | final lag | p50 / p95 / p99 / max | sustained throughput | p99 < 2,000 ms |
|---|---|---|---|---|---|---|---|---|
| [`20261019T194024`](bench-20261019T194024.md) ([json](bench-20261019T194024.json)) | 19,999.3 events/s | 5,999,802 | 20,019.4 events/s | 8,059 | 0 | 237.0 / 363.0 / 407.0 / 1,047 ms | **FAIL** | PASS |
| [`20261019T195118`](bench-20261019T195118.md) ([json](bench-20261019T195118.json)) | 20,000.0 events/s | 6,000,000 | 20,051.1 events/s | 8,564 | 0 | 230.0 / 353.0 / 391.0 / 993 ms | PASS | PASS |

The first run missed the sustained-throughput target by 1 event/s because the load
generator stopped at the 300 s mark without sending its final tick's events; that was fixed
in the previous commit (`fix(loadgen): send exactly rate x duration events`), and the
re-run sent exactly 6,000,000 events and passed both SPEC targets. The targets themselves
were not changed: sustained throughput >= 20,000 events/s for 300 s with the lag drained,
and end-to-end latency p99 < 2,000 ms.

In both runs the consumer kept up (throughput above 20,000 events/s, lag drained to 0) and
every produced event was stored exactly once (`count() FINAL` equals events sent).

Machine: a cloud VM (sandboxed Linux container) with an Intel(R) Xeon(R) Processor @
2.10GHz, 4 vCPUs, 15.7 GiB RAM, Ubuntu 24.04.5 LTS, Docker Engine 29.8.2. Redpanda,
ClickHouse, the consumer, Prometheus, Grafana and the load generator all ran on this one
machine. ClickHouse ran with `nofile` 20000 instead of 262144 because of a sandbox limit.
