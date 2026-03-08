# Operator Handbook

## Deployment Modes

### Single-node development

- one PostgreSQL instance
- one aggregator
- one or more agents
- no signing required
- `devnet` or `solana-test-validator`

### Production baseline

- managed PostgreSQL or HA PostgreSQL cluster
- at least two aggregator replicas
- multiple agents across regions/providers
- signed envelopes enabled
- native mTLS on the aggregator enabled
- ingress/service-mesh mTLS optional as an additional control layer
- Prometheus + Grafana + OTLP collector

## Bootstrap

1. Provision PostgreSQL.
2. Apply the Helm chart or Docker deployment.
3. Configure trusted signers on the aggregator.
4. Roll out agent signing keys.
5. Enable canary transactions only after funding the canary keypair.

## Key Rotation

1. Add the new signer public key to aggregator config.
2. Roll out the new private key and `key_id` to agents.
3. Confirm ingestion accepts the new `key_id`.
4. Remove the old trusted signer from aggregator config.

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

- replay log is read on startup when enabled
- recent state is hydrated from PostgreSQL automatically

### PostgreSQL lag or outage

- ingestion will fail closed
- replay log retains envelopes locally for later replay
- once storage returns, restart or allow replay process to catch up

## Recommended Alerts

- low RPC consistency index
- high slot spread
- high propagation p95
- repeated canary failures
- ingestion failures
- replay log growth without DB catch-up
