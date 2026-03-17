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
- full validator binary attestation
*(Note: Host-level memory compromise of a sentinel node is mitigated strictly for environments leveraging AWS Nitro Enclaves, which isolate the private key from the host OS)*

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

### Remote Fleet Hijacking & Aggregator Control 

Mitigations:

- `x-sentinelmesh-api-key` header required for all Control Plane modifications (`/v1/admin/broadcast`)
- Asynchronous TCP Ping/Pong mechanism forcing disconnected/half-open agents to reconnect actively.

### Edge Host Denial of Service & Command Injection

Mitigations:

- Strict **Ring Buffer** limits (10,000 batches) applied to the Agent `sled` WAL, discarding oldest logs if the aggregator is unreachable, protecting host disks from absolute ingestion saturation.
- Canary Smart Contract command execution exclusively validates base file paths (preventing Shell/Path traversal `../bin/sh`) discarding untrusted YAML injection vectors.

## Residual Risks

- replay logs are local durability, not a distributed commit log
- operators still need certificate issuance, rotation, and trust-store management for native mTLS
- canary transactions depend on the security of the configured sender keypair
