# Final results: v1.0.0

Re-measured on 2026-10-19 on **current `main` (v1.0.0, commit `061b9b9`)**, which includes the
review fixes:
- the dedup token includes an `event_id` hash;
- late and future events are judged against `produced_at`;
- ports are published on localhost only.

Every number below is copied from the linked result files or from the quoted tool output.

## Machine

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Processor @ 2.10GHz |
| vCPUs (nproc) | 4 |
| RAM | 15.7 GiB |
| OS | Ubuntu 24.04.5 LTS, kernel 6.18.44-fc-v139 |
| Docker | Engine 29.8.2 |
| consumer replicas | 1 |
| cloud VM | likely (hypervisor detected, cloud vendor unknown). This is a cloud VM (sandboxed Linux container). |

The whole stack ran on this one machine: Redpanda, ClickHouse, the consumer, Prometheus,
Grafana and the load generators. Because of a sandbox limit, ClickHouse ran with `nofile`
20000 instead of the 262144 in `docker-compose.yml`.

## Commands (in this order)

```sh
make up                                            # 1 consumer replica; waited for lag 0
make bench RATE=20000 DURATION=300
uv run python tools/loadgen.py --count 60000 --rate 20000   # warm-up, then waited for lag 0
make bench RATE=30000 DURATION=120
make replay N=1000000 DUP=0.05 BAD=0.01 LATE=0.01
make crash-test
make cov > docs/results/coverage.txt               # output saved with a header line
make down
```

Warm-up output:

```
events sent:     60000
elapsed:         3.00 s
actual rate:     19,975 events/s (target 20,000)
delivery errors: 0
```

## Throughput and latency

| run | load | produce rate (events/s) | events sent | delivery errors | consumer throughput (events/s) | rows stored (FINAL) | max lag | final lag | p50 / p95 / p99 / max (ms) |
|---|---|---|---|---|---|---|---|---|---|
| [`bench-20261019T204939`](bench-20261019T204939.md) ([json](bench-20261019T204939.json)) | 20,000/s x 300 s | 20,000.0 | 6,000,000 | 0 | 20,083.1 | 6,000,000 | 8,625 | 0 | 215.0 / 341.0 / 397.1 / 1,757 |
| [`bench-20261019T205515`](bench-20261019T205515.md) ([json](bench-20261019T205515.json)) | 30,000/s x 120 s | 29,967.5 | 3,600,000 | 0 | 29,968.3 | 3,600,000 | 27,746 | 0 | 257.0 / 764.0 / 929.1 / 1,025 |

SPEC targets:
- **20,000 events/s for 300 s:** sustained throughput: **PASS** / end-to-end latency p99: **PASS**
- **30,000 events/s for 120 s (headroom):** sustained throughput: **FAIL** / end-to-end latency p99: **PASS**

**The 30,000 events/s run shows FAIL on sustained throughput.** That SPEC check requires a
300 s run, and this headroom run was 120 s. In addition, the single producer process
reached 29,967.5 events/s, slightly below 30,000. The consumer kept up with everything it was sent
(throughput 29,968.3 events/s, lag drained to 0) and the p99 latency target passed.

## 1,000,000-event replay

Manifest: [`replay-20261019T205727.json`](replay-20261019T205727.json).
Report: [`verify-20261019T205727.md`](verify-20261019T205727.md).

It sent 1,070,000 messages: 1,000,000 unique valid events,
50,000 byte-identical duplicates, 10,000 late events, and
10,000 invalid records (bad_json 3,334,
missing_field 3,333, schema_invalid 3,333).

| check | expected | actual | result |
|---|---:|---:|---|
| count() FINAL == n_unique_valid | 1,000,000 | 1,000,000 | PASS |
| uniqExact(event_id) == n_unique_valid | 1,000,000 | 1,000,000 | PASS |
| DLQ total == bad + late | 20,000 | 20,000 | PASS |
| DLQ bad_json | 3,334 | 3,334 | PASS |
| DLQ missing_field | 3,333 | 3,333 | PASS |
| DLQ schema_invalid | 3,333 | 3,333 | PASS |
| DLQ late_event | 10,000 | 10,000 | PASS |

| info | value |
|---|---:|
| raw count() | 1,010,786 |
| duplicates not yet merged (raw - FINAL) | 10,786 |
| consumer lag at verification | 0 |

**7/7 checks passed:** zero duplicates after the replay, and 100% of invalid and late
records routed to the DLQ with the right reason.

## Crash test (kill -9 the consumer mid-stream)

Report: [`crash-20261019T205905.md`](crash-20261019T205905.md). It produced 300,000 events.

| check | expected | actual | result |
|---|---:|---:|---|
| zero loss: uniqExact(event_id) == produced | 300,000 | 300,000 | PASS |
| zero duplicates: count() FINAL == produced | 300,000 | 300,000 | PASS |

| info | value |
|---|---|
| consumer killed (SIGKILL) at | 2026-10-19T20:59:10.617+00:00 |
| consumer restarted at | 2026-10-19T20:59:15.334+00:00 |
| producer delivery errors | 0 |
| raw count() | 304,401 |
| duplicates collapsed (raw - FINAL) | 4,401 |
| consumer lag at verification | 0 |

**2/2 checks passed: zero loss and zero duplicates.** In this run the kill landed between
an insert and its commit. The redelivered batch was stored again (raw `count()` above
`FINAL`), and ReplacingMergeTree collapsed those duplicate rows on `event_id`, so
`count() FINAL` equals the events produced.

## Test coverage

From [`coverage.txt`](coverage.txt) (`make cov`: unit + integration, 157 tests passed):

```
TOTAL                           617    107    83%
Total coverage: 82.66%
```

## Summary

| SPEC target | result | source |
|---|---|---|
| sustained throughput >= 20,000 events/s for 300 s | **PASS** | [bench-20261019T204939](bench-20261019T204939.md) |
| end-to-end latency p99 < 2 s | **PASS** (397.1 ms) | [bench-20261019T204939](bench-20261019T204939.md) |
| duplicates after 1M replay = 0 | **PASS** | [verify-20261019T205727](verify-20261019T205727.md) |
| loss after kill -9 + restart = 0 | **PASS** | [crash-20261019T205905](crash-20261019T205905.md) |
| invalid records routed to DLQ = 100% | **PASS** | [verify-20261019T205727](verify-20261019T205727.md) |
| test coverage >= 80% | **PASS** (82.66%) | [coverage.txt](coverage.txt) |
