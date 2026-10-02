# ADR-0001: Storage and Replay

## Status

Accepted (Amended 2026-04-30)

## Context

The initial prototype stored all active samples in memory. That was operationally simple but failed enterprise requirements:

- no durability
- no crash recovery
- no shared state across aggregator replicas
- no idempotent replay path

## Decision

SentinelMesh will persist ingestion to **Kafka/Redpanda** for streaming durability and **ClickHouse** for analytics, with optional local NDJSON replay log.

The **Kafka/Redpanda** layer provides:

- partitioned ingestion (blake3 hash of sentinel_id for routing)
- horizontal scalability
- replay capability for new aggregator instances

The **ClickHouse** layer stores:

- probe batch metadata
- endpoint samples as JSON payloads
- Materialized Views for real-time analytics

The replay log stores:

- the full `ProbeEnvelope` as newline-delimited JSON (local backup)

## Consequences

Positive:

- durable ingest via distributed streaming
- batch idempotency
- horizontal scaling for aggregators
- crash recovery path
- high-throughput analytics with ClickHouse columnar storage
- separation of concerns: streaming (Kafka) vs analytics (ClickHouse)

Tradeoffs:

- Kafka/Redpanda and ClickHouse become required for enterprise deployments
- replay log is local, not a distributed log
- operational complexity increased (two storage systems vs one)

## Follow-up

- ✅ add ClickHouse sink for long-horizon analytics (implemented)
- add partitioning and retention jobs for large-scale history
- add object-storage archival of replay logs
- consider tiered storage (hot/warm/cold) for cost optimization
