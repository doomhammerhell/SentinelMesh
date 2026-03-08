# ADR-0001: Storage and Replay

## Status

Accepted

## Context

The initial prototype stored all active samples in memory. That was operationally simple but failed enterprise requirements:

- no durability
- no crash recovery
- no shared state across aggregator replicas
- no idempotent replay path

## Decision

SentinelMesh will persist ingestion to PostgreSQL and optionally append every envelope to a local NDJSON replay log.

The PostgreSQL layer stores:

- probe batch metadata
- endpoint samples as JSON payloads

The replay log stores:

- the full `ProbeEnvelope` as newline-delimited JSON

## Consequences

Positive:

- durable ingest
- batch idempotency
- horizontal scaling for aggregators
- crash recovery path

Tradeoffs:

- PostgreSQL becomes required for enterprise deployments
- replay log is local, not a distributed log
- analytics queries remain application-side over hydrated sample sets rather than SQL-native materialized views

## Follow-up

- add partitioning and retention jobs for large-scale history
- add ClickHouse sink for long-horizon analytics
- add object-storage archival of replay logs
