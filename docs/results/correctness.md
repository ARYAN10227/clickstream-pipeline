# Correctness results

Recorded on 2026-10-19. Every number below is copied from tool output; nothing is estimated.

## Machine

This ran on a **cloud VM** (a sandboxed Linux container), not on a laptop. Redpanda,
ClickHouse and the consumer all ran on this one machine via Docker Compose, alongside the
load generators.

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Processor @ 2.10GHz |
| vCPUs (nproc) | 4 |
| RAM | 15Gi (16479424 kB) |
| OS | Ubuntu 24.04.5 LTS, kernel 6.18.44-fc-v139 |
| Docker | Engine 29.8.2, client 29.8.2; Compose 5.6.0 |
| Python (tools) | 3.13.16 |
| Images | redpandadata/redpanda:v26.2.5, clickhouse/clickhouse-server:26.9.21.9 |

Sandbox note: this VM caps open files at 20000, so ClickHouse ran with `nofile` 20000
instead of the 262144 in `docker-compose.yml` (via a local, uncommitted compose override).

## Commands

```sh
make up
make replay N=1000000 DUP=0.05 BAD=0.01 LATE=0.01
make crash-test        # defaults: --count 300000 --rate 20000 --kill-after 5 --restart-after 3
make down
```

## 1. Replay: 1,000,000 unique events with duplicates, bad and late records

Files: [`replay-20261019T185634.json`](replay-20261019T185634.json) (manifest),
[`verify-20261019T185634.md`](verify-20261019T185634.md) (report).

Replay output:

```
run_id:           20261019T185634  (prefix r371e1)
sent:             1070000 in 48.3 s (22,156 msg/s)
  valid:          1000000
  duplicates:     50000
  late:           10000
  bad_json:       3334
  missing_field:  3333
  schema_invalid: 3333
delivery errors:  0
manifest:         docs/results/replay-20261019T185634.json
```

Verification (`tools/verify.py`), run after the consumer group reached lag 0:

```
check                                      expected      actual  result
-----------------------------------------------------------------------
count() FINAL == n_unique_valid           1,000,000   1,000,000  PASS
uniqExact(event_id) == n_unique_valid     1,000,000   1,000,000  PASS
DLQ total == bad + late                      20,000      20,000  PASS
DLQ bad_json                                  3,334       3,334  PASS
DLQ missing_field                             3,333       3,333  PASS
DLQ schema_invalid                            3,333       3,333  PASS
DLQ late_event                               10,000      10,000  PASS

raw count()                                           1,010,898  info
duplicates not yet merged (raw - FINAL)                  10,898  info
consumer lag at verification                                  0  info

7/7 checks passed
```

**Result: 7/7 checks passed.** All 1,000,000 unique events are stored exactly once
(`count() FINAL` and `uniqExact(event_id)` both 1,000,000), the 50,000 injected
duplicates did not inflate the counts, and all 20,000 invalid or late records reached the
DLQ with the right reason. "duplicates not yet merged" (10,898) is information only: rows
that ReplacingMergeTree had not merged away yet at query time; `FINAL` already excludes them.

## 2. Crash test: `kill -9` the consumer mid-stream

File: [`crash-20261019T185757.md`](crash-20261019T185757.md).

```
run crash-20261019T185757: 300,000 events at 20,000/s, prefix ka9d2d
2026-10-19T18:58:03.018+00:00  consumer killed with SIGKILL
2026-10-19T18:58:07.602+00:00  consumer started again
producer done: 300,000 sent, 0 delivery errors
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 200,000
waiting for consumer, lag: 190,000
waiting for consumer, lag: 10,000
consumer lag: 0
```

```
crash test crash-20261019T185757 (prefix ka9d2d)

check                                         expected      actual  result
--------------------------------------------------------------------------
zero loss: uniqExact(event_id) == produced     300,000     300,000  PASS
zero duplicates: count() FINAL == produced     300,000     300,000  PASS

consumer killed (SIGKILL) at                2026-10-19T18:58:03.018+00:00
consumer restarted at                       2026-10-19T18:58:07.602+00:00
producer delivery errors                    0
raw count()                                 300,000
duplicates collapsed (raw - FINAL)          0
consumer lag at verification                0

2/2 checks passed
```

**Result: 2/2 checks passed (zero loss, zero duplicates).**
**Duplicates collapsed (raw - FINAL): 0.** In this run no duplicate rows reached the table
at all: the kill landed between batches, or the redelivered batch was skipped by the
insert-level dedup token.

The lag stayed at 200,000 for seven 5-second progress reports after the restart because the killed consumer
never left the group, so its partitions were only reassigned once its session timed out
(default `session.timeout.ms`, 45 s).
