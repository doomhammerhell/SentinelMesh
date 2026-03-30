# SentinelMesh

SentinelMesh is an open infrastructure observability layer for the Solana network. It runs a distributed mesh of lightweight sentinel nodes that probe heterogeneous RPC and validator-facing endpoints, compare their behavior, and surface signals that matter for censorship resistance and infrastructure integrity.

The repository is structured as an enterprise-oriented open-source Rust workspace with durable storage, signed ingestion, replayable telemetry, validator-centric probes, canary transaction support, deployment assets, CI/CD and operational documentation.

**[Read the Elite Grant Strategy Document](docs/grant-strategy.md)** — Our technical vision for irrefutable integrity via Formal Methods and ZK.

## On-Chain Addresses (Solana Devnet)

| Contract | Program ID | Explorer |
|----------|-----------|----------|
| SentinelMesh Canary | `441bXQJ5SekBDdNmWZ7WZoGvEdQkMQuCr72MSeEVHMA2` | [View on Explorer](https://explorer.solana.com/address/441bXQJ5SekBDdNmWZ7WZoGvEdQkMQuCr72MSeEVHMA2?cluster=devnet) |
| IDL Metadata Account | `DLqFVg3v7Fu6ym8ZgaXtzMCijm682cr3Bcu2Zp7NruMn` | [View on Explorer](https://explorer.solana.com/address/DLqFVg3v7Fu6ym8ZgaXtzMCijm682cr3Bcu2Zp7NruMn?cluster=devnet) |

## What It Does

- Continuously probes multiple Solana endpoints in parallel
- Measures RPC consistency, blockhash agreement, slot spread and block height spread
- Tracks transaction propagation windows across providers
- Compares tracked account state hashes across the infrastructure edge
- Samples validator-centric views such as identity, vote accounts, cluster nodes and leader schedule
- Produces real-time integrity metrics and anomalies through a public aggregation API
- Stores observations durably in PostgreSQL and replays local ingestion logs for crash recovery
- Features a **Cyberpunk-themed Premium Dashboard** (`/`) for real-time visualization of Provider HHI and fleet telemetry
- Supports hardware-isolated cryptographic signing via **AWS Nitro Enclaves** (TEEs)
- Audits network Censorship Resistance deterministically via the **Canary DEX Smart Contract**
- Orchestrates global agent topologies via an authenticated **WebSocket Control Plane**

## Workspace

- `apps/sentinelmesh-agent`: sentinel node runtime, canary producer, signed batch publisher and local admin plane
- `apps/sentinelmesh-aggregator`: ingestion plane, storage-backed aggregation API, dashboard and Prometheus exporter
- `crates/sentinelmesh-core`: shared config, wire models, signing and telemetry bootstrap
- `crates/sentinelmesh-solana`: Solana JSON-RPC client and advanced probe logic
- `crates/sentinelmesh-analysis`: freshness-window analytics and anomaly derivation
- `crates/sentinelmesh-storage`: PostgreSQL durability and replay-log support

## Architecture

1. Agent nodes probe a portfolio of RPC and validator-capable endpoints.
2. Each probe cycle yields a `ProbeBatch`, optionally signed with an Ed25519 key.
3. The aggregator validates auth, persists the batch to PostgreSQL, appends to the replay log and updates the in-memory analytics view.
4. Any aggregator instance can rebuild the active window from shared storage, enabling stateless horizontal scaling behind a load balancer.
5. Operators and developers consume data through the REST API, dashboard, Prometheus metrics, OTLP traces and versioned monitoring assets.

## Key Features

### Durability and HA

- PostgreSQL-backed durable ingestion
- Idempotent batch persistence keyed by `batch_id`
- **Agent Local WAL (`sled`)** with Ring Buffer Eviction (Zero-Day disk exhaustion protection)
- Periodic in-memory state hydration from storage for stateless aggregator instances

### Security & Hardware Isolation

- Signed ingestion envelopes with Ed25519
- **AWS Nitro Enclaves (TEE)** integration for hardware-level private key isolation
- API-key authenticated Control Plane (`/v1/admin/broadcast`) protecting against Remote Fleet Hijacking
- Asynchronous **TCP Ping/Pong Keep-Alive** eliminating Half-Open Zombie connections
- Native TLS/mTLS support in the aggregator process
- Service-mesh mTLS manifests in `deploy/istio`

### Observability

- Prometheus metrics on both services
- Optional OTLP tracing
- Versioned Prometheus alert rules
- Versioned Grafana dashboard JSON

### Solana Depth

- Standard RPC probes
- Validator-centric probes: `getIdentity`, `getVoteAccounts`, `getClusterNodes`, `getLeaderSchedule`
- Canary transaction support via automated `solana transfer` execution
- **Deterministic MEV Auditing** via the `sentinelmesh-canary-client` interacting with Compute-Unit intensive Smart Contracts

## Quickstart

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Start the aggregator:

```bash
cargo run --bin sentinelmesh-aggregator -- --config config/aggregator.example.yaml
```

Start one sentinel agent:

```bash
cargo run --bin sentinelmesh-agent -- --config config/agent.example.yaml
```

Open:

- Dashboard: [http://127.0.0.1:9480](http://127.0.0.1:9480)
- Aggregator health: [http://127.0.0.1:9480/healthz](http://127.0.0.1:9480/healthz)
- Agent status: [http://127.0.0.1:9490/v1/status](http://127.0.0.1:9490/v1/status)

## Runtime Surface

Aggregator:

- `GET /` (Premium Analytics Dashboard)
- `GET /healthz`
- `GET /metrics`
- `GET /v1/snapshot`
- `GET /v1/providers`
- `GET /v1/signatures`
- `GET /v1/accounts`
- `POST /v1/ingest`
- `GET /v1/ws/control` (WebSocket Control Plane)
- `POST /v1/admin/broadcast` (Requires `x-sentinelmesh-api-key`)

Agent:

- `GET /`
- `GET /healthz`
- `GET /metrics`
- `GET /v1/status`

## Validation

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check
cargo audit
cargo test --workspace
```

## Documentation

- [Architecture](./docs/architecture.md)
- [Threat Model](./docs/threat-model.md)
- [API Specification](./docs/api.md)
- [Operator Handbook](./docs/operator-handbook.md)
- [ADR-0001: Storage and Replay](./docs/adrs/0001-storage-and-replay.md)

## Deploy

- Dockerfiles: `deploy/docker`
- Compose stack: `docker-compose.yml`
- Helm chart: `deploy/helm/sentinelmesh`
- systemd units: `deploy/systemd`
- Observability assets: `deploy/observability`
- mTLS service-mesh manifests: `deploy/istio`

## License

Apache-2.0. See [LICENSE](./LICENSE).
