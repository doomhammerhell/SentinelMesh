# Security Policy

## Reporting a Vulnerability

Do not open a public issue for a suspected vulnerability.

Please report security findings privately to the repository maintainers with:

- affected component
- impact summary
- reproduction steps
- suggested mitigation if known

## Scope

High-priority areas:

- signed ingestion and key verification
- canary transaction execution and key handling
- agent WAL durability and data tampering
- Kafka/Redpanda and ClickHouse persistence security
- AWS Nitro Enclaves attestation and key isolation
- deployment manifests that affect mTLS or exposure

## Disclosure

The project aims for coordinated disclosure. Maintainers will acknowledge reports, assess severity and coordinate a fix before public release.
