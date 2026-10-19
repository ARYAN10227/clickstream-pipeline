# Consumer scaling: 1 vs 3 replicas

`make bench RATE=30000 DURATION=120` (1 producer process reached the rate, so `--procs 2` was
not needed), with the consumer at 1 replica and at 3 replicas
(`docker compose up -d --scale consumer=3`; the `clicks` topic has 6 partitions, so each
replica owned 2). Code: after the `perf(consumer)` change. Every number is copied from the
linked result files or from ClickHouse's `system.query_log`.

**Machine:** Intel(R) Xeon(R) Processor @ 2.10GHz, **4 vCPUs shared by everything**: Redpanda,
ClickHouse, every consumer replica, Prometheus, Grafana and the load generator. 15.7 GiB
RAM, Ubuntu 24.04.5 LTS, kernel 6.18.44-fc-v139, Docker 29.8.2. Cloud VM: likely (hypervisor detected, cloud vendor unknown).

## Results

| replicas | run | replicas (recorded) | produce rate (events/s) | consumer throughput (events/s) | max lag | p50 / p95 / p99 / max (ms) |
|---|---|---|---|---|---|---|
| 1 (cold start) | [`20261019T201902`](bench-20261019T201902.md) | 1 | 30,000.0 | 30,224.9 | 59,430 | 282.0 / 1824.4 / 2094.3 / 2,293 |
| 3 (warmed) | [`20261019T202150`](bench-20261019T202150.md) | 3 | 29,997.5 | 30,017.5 | 16,230 | 363.0 / 640.4 / 747.1 / 893 |
| 1 (warmed) | [`20261019T202410`](bench-20261019T202410.md) | 1 | 29,997.5 | 30,090.3 | 12,614 | 262.0 / 405.0 / 464.1 / 898 |

**Run order and setup:**
1. 1 replica, immediately after the consumer container was recreated (cold).
2. Scaled to 3, waited for a stable group at lag 0, then warmed up with 60,000 events at
   20,000/s and waited for lag 0 again.
3. Scaled back to 1 (the surviving replica), same wait and the same warm-up.

All three runs stored all 3,600,000 events and drained the lag to 0. Every run shows FAIL on
the SPEC throughput target only because that target requires 300 s and these runs were 120 s.

**ClickHouse inserts during each run** (`system.query_log`, the 125 s after each run's start;
the window can include a few rows from just outside the run):

| replicas | inserts | rows written | rows per insert (avg) | insert duration p95 (ms) |
|---|---|---|---|---|
| 1 (cold start) | 720 | 3,600,000 | 5,000 | 76 |
| 3 (warmed) | 738 | 3,600,000 | 4,878 | 81 |
| 1 (warmed) | 721 | 3,605,000 | 5,000 | 77 |

## What the numbers say

- **3 replicas did not help; on this machine they were slightly worse.** With both setups
  warmed, 1 replica had max lag 12,614 and p99 464.1 ms; 3 replicas had max lag 16,230 and
  p99 747.1 ms (p50 262.0 vs 363.0 ms). Throughput was the same, because each run
  could only consume what the producer sent (about 30,000 events/s).
- **Insert batching did not change:** about the same number of inserts of about 5,000 rows
  each, with nearly the same p95 insert time, so ClickHouse saw almost the same write
  workload in both cases. The likely reason for the extra latency is CPU contention: three
  Python consumer processes plus Redpanda, ClickHouse and the load generator all compete for
  the same 4 vCPUs, and one consumer was not the bottleneck at 30,000 events/s. CPU usage was
  **not** measured per container in these runs, so this is the likely explanation, not a
  proven one.
- **Startup matters more than replica count here:** the cold 1-replica run had p99
  2,094.3 ms and max lag 59,430, from the first ~30 s (lag samples 48,236 and 59,430 at
  5 s and 10 s), before settling into the same range as the warmed run.
- Each configuration was measured once; differences of a few hundred milliseconds between
  single runs on a shared 4-vCPU machine should not be over-read.

Running this experiment needed one compose change: the consumer's metrics port is now
published as a range (`"9108-9110:9108"`), because a fixed `9108:9108` made
`--scale consumer=3` fail with "port is already allocated". The first replica still serves
`localhost:9108`, and Prometheus scrapes every replica over the Docker network.

## Where this design breaks at 10x (200,000+ events/s)

- **Partition count caps consumers.** A consumer group uses at most one consumer per
  partition, so `clicks` with 6 partitions allows at most 6 useful replicas. 10x needs more
  partitions, and repartitioning changes key-to-partition mapping, so plan it up front.
- **Single broker.** One Redpanda node (`--smp 2`, `--overprovisioned`, replication factor 1)
  is a single point of failure and a throughput ceiling. 10x needs a multi-node cluster with
  replication factor 3 and real, non-overprovisioned cores.
- **ClickHouse insert rate.** Today each consumer sends batches of about 5,000 rows. At 10x
  with many replicas, the number of inserted parts per second grows and background merges
  (including ReplacingMergeTree dedup) can fall behind (`TOO_MANY_PARTS`). The fix is larger
  or async-buffered batches, fewer writers, and eventually a sharded or replicated
  ClickHouse cluster.
- **Python validation cost.** The [profile](profile-notes.md) shows the consumer CPU-bound in
  Python: JSON parsing, pydantic validation, row building and driver serialization. One
  process kept up with 30,000 events/s here, so 10x needs many processes on dedicated cores.
  Beyond that, the hot path (validation and row encoding) has to move to compiled code or a
  vectorized path (ADR-0001's documented ceiling).
