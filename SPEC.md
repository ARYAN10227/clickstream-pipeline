# SPEC — Real-Time Click-Event Pipeline

## Problem
The URL shortener records nothing about clicks. Every redirect should be captured as an
event, streamed through a durable log, and queryable within seconds, without losing
events on crashes or double-counting them on replays.

## MVP features
1. Node producer hook in the URL shortener: on each redirect, non-blocking send of a
   click event to Redpanda topic `clicks`.
2. Python load generator: rate-controlled (target 20k events/s) plus a replay mode with
   injected duplicates and bad records.
3. Python consumer (group `clickpipe`): poll batch → validate → enrich → insert into
   ClickHouse with a dedup token → send invalid records to `clicks-dlq` → flush DLQ →
   commit offsets. Offsets are committed only after both succeed.
4. Idempotent storage: producer-assigned UUID `event_id`, ReplacingMergeTree keyed on it,
   plus batch-level insert deduplication tokens.
5. Data-quality rules with DLQ reason codes: bad_json, schema_invalid, missing_field,
   late_event (older than 300 s), future_event (more than 60 s ahead).
6. Observability: Prometheus metrics and Grafana dashboards (Click Analytics, Pipeline Ops).
7. Correctness tools: 1M-event replay with duplicates; kill -9 crash/restart test.
8. One-command run (`make up`), tests, benchmarks (`make bench`), GitHub Actions CI.

## Out of scope
Multi-node clusters, managed cloud, Kafka transactions, dashboard auth, Avro/schema
registry, storing raw IP addresses.

## Event schema (JSON, schema_version = 1)
| field          | type           | rule                               |
|----------------|----------------|------------------------------------|
| event_id       | UUID v4 string | required                           |
| short_code     | string         | required, ^[A-Za-z0-9_-]{1,16}$    |
| event_time     | int (epoch ms) | required, click time               |
| produced_at    | int (epoch ms) | required, set right before send    |
| ip             | string         | optional, GeoIP only, never stored |
| user_agent     | string         | optional, max 512 chars            |
| referrer       | string or null | optional                           |
| schema_version | int            | required, must be 1                |

Kafka message key = short_code. Optional header `run_id` for test runs.

## ClickHouse table (database `analytics`)
```sql
CREATE TABLE IF NOT EXISTS analytics.clicks (
  event_id        UUID,
  short_code      LowCardinality(String),
  event_time      DateTime64(3, 'UTC'),
  produced_at     DateTime64(3, 'UTC'),
  ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3),
  referrer_domain LowCardinality(String) DEFAULT '',
  device          LowCardinality(String) DEFAULT 'unknown',
  browser         LowCardinality(String) DEFAULT 'unknown',
  country         LowCardinality(String) DEFAULT 'ZZ',
  schema_version  UInt8,
  kafka_partition UInt16,
  kafka_offset    UInt64
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (short_code, event_time, event_id)
SETTINGS non_replicated_deduplication_window = 1000;
```
Correct counts use FINAL or uniqExact(event_id), never plain count().

## Tech stack
Redpanda (Kafka API), Python 3.12 + uv, confluent-kafka, clickhouse-connect,
Pydantic v2 + orjson, prometheus-client, ClickHouse, Prometheus, Grafana,
Node.js producer (@confluentinc/kafka-javascript), pytest + testcontainers, ruff,
Docker Compose.

## Target metrics (must be measured, with machine specs recorded)
| metric                        | target                               |
|-------------------------------|--------------------------------------|
| sustained throughput          | >= 20,000 events/s for 5 minutes     |
| end-to-end latency            | p99 < 2 s (ingested_at - produced_at) |
| duplicates after 1M replay    | 0                                    |
| loss after kill -9 + restart  | 0                                    |
| invalid records routed to DLQ | 100%                                 |
| test coverage                 | >= 80%                               |
