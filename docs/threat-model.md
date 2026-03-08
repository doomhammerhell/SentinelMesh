# Threat Model

## Goals

- Detect inconsistent infrastructure behavior across Solana access layers
- Preserve integrity of probe telemetry between agents and aggregators
- Minimize trust in any single RPC provider or aggregator instance

## In Scope

- malicious or faulty RPC providers
- delayed or filtered transaction visibility
- inconsistent account state across providers
- compromised agent transport path
- aggregator restarts and partial data loss
- key rotation and stale signer acceptance

## Out of Scope

- compromise of the Solana consensus protocol itself
- nation-state level network partitioning
- host-level compromise of a sentinel node after key theft
- full validator binary attestation

## Threats and Mitigations

### Malicious RPC returns selective or stale data

Mitigations:

- multi-provider probes per cycle
- divergence metrics across slots, block heights and blockhashes
- tracked account hash comparisons
- propagation windows across providers

### Aggregator crash causes data loss

Mitigations:

- PostgreSQL durability
- replay log append before durable ingest completion
- state rebuild from storage on restart

### Forged telemetry from an unauthorized agent

Mitigations:

- signed probe envelopes
- `key_id` scoped verification
- trusted signer allowlist
- optional API-key gate

### Credential rotation drift

Mitigations:

- multiple trusted signers in config
- `key_id` carried on every envelope
- staged rollout with overlap windows

### Transport interception

Mitigations:

- TLS client support in agent
- native TLS/mTLS termination in the aggregator
- mTLS manifests for service mesh or ingress
- application-layer signing independent of transport

## Residual Risks

- replay logs are local durability, not a distributed commit log
- operators still need certificate issuance, rotation, and trust-store management for native mTLS
- canary transactions depend on the security of the configured sender keypair
