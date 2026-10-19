CREATE DATABASE IF NOT EXISTS analytics;

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
