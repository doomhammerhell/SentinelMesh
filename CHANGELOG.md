# Changelog

## Unreleased

- **BREAKING**: Migrated storage from PostgreSQL to Kafka/Redpanda + ClickHouse
- Added Kafka/Redpanda streaming ingestion with blake3 hash partitioning
- Added ClickHouse columnar storage with Materialized Views
- Added signed probe envelopes with key rotation primitives
- Added validator-centric probes and canary transaction support
- Added MEV Audit with Kendall tau correlation analysis
- Added AWS Nitro Enclaves support for hardware attestation
- Added Z-Score statistical anomaly detection with SlidingWindow
- Added ASN HHI for topological concentration analysis
- Added OTLP tracing configuration and observability assets
- Added deployment, CI/CD and OSS governance scaffolding
