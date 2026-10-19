# Demo script (2 minutes)

A walkthrough that shows the pipeline running, the dashboards, and the two correctness
proofs. Commands are exact; run them from the repository root.

## Before you start (not part of the 2 minutes)

```sh
cp .env.example .env     # first time only
make up                  # builds and starts everything, then prints the smoke-check table
```

Wait for `5/5 checks passed`. Open a terminal and a browser at http://localhost:3000. Log in as `admin` with the `GRAFANA_ADMIN_PASSWORD` from `.env`.

## 1. Pipeline Ops dashboard (empty, waiting)

In Grafana, open **Dashboards → Clickpipe → Pipeline Ops**. Say: *"this is the consumer's
health: throughput, lag, DLQ by reason, end-to-end latency, batch size, Redpanda produce
rate."* It refreshes every few seconds.

## 2. Load and watch it move

```sh
make bench RATE=20000 DURATION=30
```

On **Pipeline Ops**, show:
- **Inserted events/s** and **Consumed vs inserted** climbing to the load rate
- **Total consumer lag** rising a little during batching and draining back to 0
- **End-to-end latency p50 / p95 / p99** staying well under the SPEC's 2 s target

When the bench finishes it prints PASS/FAIL against the SPEC targets. **The throughput
check shows FAIL for this short run by design:** the SPEC target requires 300 s. Point at the
real 300 s baseline instead ([docs/results/README.md](results/README.md)).

## 3. Click Analytics dashboard

Open **Dashboards → Clickpipe → Click Analytics** and set the time range to the last 15
minutes. Show clicks per minute, the top short codes, referrers, devices and browsers.
Then pick one value in the **Short code** dropdown to filter every panel. Say: *"every count
here uses `uniqExact(event_id)`, so duplicates can never inflate it."*

## 4. Replay with duplicates and bad records

```sh
make replay N=100000
```

This sends unique events plus byte-identical duplicates, invalid records of each kind and
late events. Then it waits for lag 0 and prints the expected-vs-actual table. Show that:
- `count() FINAL` and `uniqExact(event_id)` equal the number of unique events (duplicates
  collapsed);
- every DLQ reason count matches.

The last line should be `7/7 checks passed`.

## 5. Crash test (last, because it kills the consumer)

```sh
make crash-test
```

This runs last on purpose: it `kill -9`s the consumer container, so running it alongside
steps 2–4 would distort their lag and latency. It produces events, kills the consumer
mid-stream and restarts it. Then it waits for the restarted consumer to get its partitions
back (the killed consumer's group session has to time out first) and verifies. While it
waits, explain what it checks. The report shows:
- the SIGKILL and restart timestamps;
- **zero loss**: `uniqExact(event_id)` equals produced;
- **zero duplicates**: `count() FINAL` equals produced;
- "duplicates collapsed", the number of redelivered rows that deduplication absorbed.

If the 2 minutes run out before it finishes, show a recorded run instead:
[docs/results/crash-20261019T185757.md](results/crash-20261019T185757.md).

## Afterwards

```sh
make down     # stops the stack and keeps data volumes
```

The reports from steps 2, 4 and 5 are written to `docs/results/` (gitignored for trial runs).
For why each guarantee holds, see [architecture.md](architecture.md).
