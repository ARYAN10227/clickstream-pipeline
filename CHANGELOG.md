# Changelog

All notable changes, grouped by [Conventional Commits](https://www.conventionalcommits.org/) type.

## [1.0.0] - 2026-10-19

First release: real-time click-event pipeline (Redpanda -> Python consumer -> ClickHouse) with
idempotent writes, a dead-letter topic, Prometheus/Grafana monitoring, replay/crash/bench
tooling and measured results in [docs/results](docs/results/README.md).

### Features

- **infra:** add Redpanda broker with clicks and DLQ topics (3b8bbb2)
- **infra:** add ClickHouse with ReplacingMergeTree clicks table (b41e900)
- **config:** add typed settings with Kafka and ClickHouse client configs (0653a6e)
- **tools:** add stack smoke check (c0e2248)
- **schema:** add ClickEvent model and row mapping (698a2ae)
- **tools:** add rate-controlled click event load generator (a3d56e0)
- **sink:** add ClickHouse batch sink (656db23)
- **consumer:** add batch consume-insert-commit loop (aa37078)
- run consumer as a compose service (fc15b05)
- **integration:** add non-blocking Node producer for the URL shortener (0cf0b2d)
- **dq:** add data-quality validation with DLQ reason codes (9fa8a47)
- **dlq:** add dead-letter producer with reason headers (66df9b6)
- **consumer:** route invalid and late events to DLQ before commit (4f26e86)
- **dedup:** add deterministic batch dedup token (e50c8e0)
- **sink:** make batch inserts idempotent with dedup tokens (0ffdaa8)
- **consumer:** retry inserts with backoff and never commit on failure (d031fed)
- **consumer:** graceful shutdown and safe rebalance handling (7b79af9)
- **tools:** add replay generator with injected duplicates and bad records (de97758)
- **tools:** add replay verification against ClickHouse and DLQ (e2726d5)
- **tools:** add kill -9 crash and restart no-loss test (934a8d8)
- **enrich:** add referrer, device, browser and country enrichment (ac6a1e4)
- **consumer:** enrich events before insert (3692ade)
- **metrics:** expose Prometheus pipeline metrics (dd27d90)
- **infra:** add Prometheus scraping consumer and Redpanda (4bb8e97)
- **infra:** add Grafana with provisioned datasources (089d9c9)
- **dashboards:** add Pipeline Ops dashboard (34e3033)
- **dashboards:** add Click Analytics dashboard (ef49c39)
- **tools:** add end-to-end latency report (f827d44)
- **bench:** add make bench harness (1aa88e9)

### Bug fixes

- **loadgen:** send exactly rate x duration events (704f376)
- **dedup:** include event_id hash in insert dedup token (15f5543)
- **dq:** judge late and future events against produced_at (3481e51)
- **compose:** publish service ports on localhost only (d1f1aa2)

### Performance

- **consumer:** pass timestamps to ClickHouse as epoch-ms ints (d524a72)

### Refactoring

- **sql:** separate init schema from dashboard queries (7543923)

### Tests

- add testcontainers fixtures for Redpanda and ClickHouse (1ee1a0c)
- **e2e:** verify produced events land in ClickHouse (cebe9fd)
- **e2e:** cover replay dedup and crash recovery (61e537b)
- use a separate ClickHouse client per thread in shutdown test (555e267)

### Documentation

- add project specification (462a7ba)
- add architecture decision records (07f6da7)
- **results:** record 1M replay and crash-test results (94e6831)
- **results:** add baseline benchmark (645284a)
- **results:** re-run baseline after load generator fix (da6e5ef)
- **perf:** profile consumer hot path (677a875)
- **results:** compare 1 vs 3 consumer replicas (e72154e)
- add full README with measured results (a2ebef6)
- explain delivery semantics and failure scenarios (728360c)
- note which results predate the review fixes (fd8e318)
- add demo script (c5a5f9a)
- run the crash test last in the demo (d40e704)

### CI

- add lint and unit test workflow (81b5fc3)

### Chores

- set up python package with uv, ruff and pytest (53779d8)
- add Makefile with project commands (ca75ef1)
- **integration:** add package-lock for reproducible installs (ab1f7dc)
