# Consumer profile (baseline)

Flame graph: [`profile-baseline.svg`](profile-baseline.svg) (open in a browser; it is
interactive). Recorded on 2026-10-19 with:

```sh
make up && docker compose stop consumer
uv run python tools/loadgen.py --rate 20000 --duration 75 &      # 1,500,000 events at 19,999/s
uv run py-spy record -d 60 --idle -o docs/results/profile-baseline.svg -- python -m clickpipe.main
```

The consumer ran on the host (against `localhost:19092` and ClickHouse on `localhost:8123`)
with the default settings (batches of up to 5,000 messages / 500 ms). The SVG has 11,659
samples. For the numbers below, a second py-spy attached to the same process in
non-blocking mode wrote raw stacks (10,626 samples, `--idle` included). Of those, 5,333
belong to the Prometheus metrics HTTP server thread, which only sleeps in `select()`; the
shares below are of the **5,293 consumer main-thread samples**.

Machine: cloud VM, Intel(R) Xeon(R) Processor @ 2.10GHz, 4 vCPUs, shared with Redpanda,
ClickHouse, Prometheus, Grafana and the load generator.

## Top 5 functions by self time (main thread)

| # | function | self time |
|---|---|---|
| 1 | `readinto` (socket.py), waiting for ClickHouse's HTTP response to an insert | 16.2% |
| 2 | `_datetime64_ticks` (clickhouse_connect/datatypes/temporal.py), converting our `datetime` objects back into integer ticks | 8.2% |
| 3 | `validate` (clickpipe/schema.py), includes the `orjson.loads` C call | 7.8% |
| 4 | `run` (clickpipe/consumer.py), mostly the `consume()` call into librdkafka | 7.1% |
| 5 | `model_validate` (pydantic/main.py) | 6.9% |

## Waiting vs computing

**Waiting (about 24%):**
- 16.2%: ClickHouse response to the insert (`socket.readinto`)
- 7.1%: inside `consume()`, waiting for messages from Redpanda
- 0.6%: sending request bytes

**Computing (about 76%), all in Python on one thread:**
- **38.1% `process_batch`**, which includes:
  - `validate` 19.6% (pydantic 6.9%)
  - `to_row` 14.9%, which includes `ms_to_datetime` 4.3% and enrichment:
    `referrer_domain` 5.3%, `country` 2.8%, `parse_ua` 1.8% (cache hits)
- **25.9% clickhouse-connect serializing rows** inside `insert_rows`, led by
  `_datetime64_ticks` 8.2%, `_row_block_data` 3.5% and the LowCardinality dictionary 2.6%
- **10.1% per-event Prometheus histogram updates** (`observe_e2e_latency`)
- **0.7% dedup token**, and the lag refresh is 0.6%

So at 20,000 events/s the consumer is CPU-bound in Python, not I/O-bound. That matches the
baseline benchmark, where consumer throughput (20,051.1 events/s) is barely above the input
rate.

## Recommended first change

**Pass `event_time` and `produced_at` to ClickHouse as integer epoch milliseconds instead
of `datetime` objects.**

Today `to_row` turns each event's epoch-ms integers into timezone-aware `datetime` objects
(`ms_to_datetime`, 4.3%). Then clickhouse-connect turns every one of them back into integer
ticks (`_datetime64_ticks`, 8.2%, the top computing frame), because both columns are
`DateTime64(3)`, whose ticks *are* epoch milliseconds.

clickhouse-connect writes `int` values for a DateTime64 column directly, with no conversion
(`DateTime64._write_column_binary`: `if isinstance(first, int) ... write_array("q", column)`).
Removing the round trip should save roughly the 12.5% of main-thread time those two frames
take, plus part of the 1.6% in the temporal column writer.

It is the largest avoidable cost, a small change (`to_row` plus the one place that reads
`produced_at` back, the e2e-latency metric), and it changes nothing in the stored data.

Not chosen first:
- **Per-event histogram updates (10.1%):** they would need a change in what the metric
  means, or a custom bucket counter.
- **Validating straight from bytes with pydantic (`model_validate_json`):** a larger change
  to the data-quality path.
