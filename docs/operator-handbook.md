# Operator Handbook

## Deployment Modes

### Single-node development

- Kafka/Redpanda (single node) or Docker Compose stack
- ClickHouse (single node)
- one aggregator
- one or more agents
- no signing required
- `devnet` or `solana-test-validator`

### Production baseline

- managed **Kafka/Redpanda** cluster (3+ nodes recommended)
- **ClickHouse** cluster for analytics
- at least two aggregator replicas
- multiple agents across regions/providers
- signed envelopes enabled
- native mTLS on the aggregator enabled
- ingress/service-mesh mTLS optional as an additional control layer
- Prometheus + Grafana + OTLP collector

## Bootstrap

1. Provision Kafka/Redpanda and ClickHouse.
2. Apply the Helm chart or Docker deployment.
3. Configure trusted signers on the aggregator.
4. Roll out agent signing keys.
5. Enable canary transactions only after funding the canary keypair.

## Key Rotation

1. Add the new signer public key to aggregator config.
2. Roll out the new private key and `key_id` to agents.
3. Confirm ingestion accepts the new `key_id`.
4. Remove the old trusted signer from aggregator config.

## Hardware Attestation (AWS Nitro Enclaves)

SentinelMesh supports **AWS Nitro Enclaves** for hardware-level private key isolation. This feature provides:

- **Proof of Origin**: Cryptographic attestation that probes originated from a trusted enclave
- **Key Protection**: Private keys never leave the hardware-isolated CVM
- **Tamper Evidence**: PCR registers measure the enclave code and configuration

### Configuration

**Agent side** (`agent.yaml`):
```yaml
publish:
  auth:
    signing:
      type: nitro_enclave
      signer_id: sentinel-scl-01
      key_id: enclave-key-2026
      vsock_cid: 16
      vsock_port: 5000
```

**Enclave side**: Deploy the `sentinelmesh-enclave` signer service inside the Nitro Enclave.

### Attestation Quote

Each `ProbeEnvelope` includes an `AttestationQuote` with:
- `pcr0`, `pcr1`, `pcr2`: Platform Configuration Registers
- `enclave_id`: Unique enclave identifier
- `signature_b64`: Enclave-signed attestation
- `public_key_b64`: Enclave public key for verification

> **Note**: In development mode without Nitro Enclaves, the attestation contains mock values. Production deployments should use real enclave attestation.

## Native mTLS

Configure the aggregator with:

- `security.server_cert_path`
- `security.server_key_path`
- `security.trusted_client_ca_path`
- `security.require_client_cert: true`

Configure each agent with:

- `publish.tls.ca_cert_path`
- `publish.tls.client_cert_path`
- `publish.tls.client_key_path`
- `publish.tls.domain_name` when the certificate CN/SAN does not match the raw host value

Service-mesh mTLS may still be layered on top, but it is no longer required for certificate-authenticated transport between agents and aggregators.

## Canary Operations

- keep canary amount minimal
- isolate the canary keypair from operator wallets
- fund the canary keypair only with the runway required for observation
- monitor `sentinelmesh_agent_canary_failure_total`

## Recovery

### Aggregator restart

- recent state is hydrated from ClickHouse automatically
- Kafka consumer groups resume from last committed offset

### Storage lag or outage

- ingestion will fail closed
- agent WAL retains batches locally for later flush
- once storage returns, the WAL flusher catches up automatically

## Recommended Alerts

- low RPC consistency index
- high slot spread
- high propagation p95
- repeated canary failures
- ingestion failures
- WAL depth growth without flush
