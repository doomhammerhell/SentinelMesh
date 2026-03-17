# Architecture

SentinelMesh is split into a probe plane and an aggregation plane.

## Probe Plane

`sentinelmesh-agent` fans out requests to multiple Solana endpoints in parallel and captures:

- health
- slot
- block height
- latest blockhash
- version
- validator identity
- vote accounts
- cluster nodes
- leader schedule
- tracked accounts
- tracked transaction signatures

Agents may also emit canary transactions using the `solana` CLI. Fresh canary signatures are fed back into the tracking set so the next probe cycle measures propagation windows without relying on a static signature list.

Each probe cycle emits a `ProbeBatch`. Batches can be wrapped in a signed `ProbeEnvelope` using Ed25519. The signer is identified by `signer_id` and `key_id`, allowing key rotation without changing the sentinel identity.

To support **Proof of Origin**, cryptographic signing has been abstracted securely via the `SignerBackend` trait. Enterprise nodes can leverage **AWS Nitro Enclaves** (`NitroEnclaveSigner` via VSOCK) to sign batches in hardware-isolated CVMs without ever exposing private keys to the host memory.

### Resilience and Auto-Healing
If the Aggregator cannot be reached, the Agent fails over to a local zero-dependency **WAL (Write-Ahead Log)** powered by `sled`. A dedicated background `Flusher` thread retries dispatching rested batches with exponential backoff. The WAL employs a **Ring Buffer Eviction Policy** (`max_entries = 10_000`) preventing zero-day disk exhaustion during permanent network partitions.

### Canary DEX Smart Contract Protocol 
For deterministic MEV and Censorship Resistance auditing, agents can invoke the native `sentinelmesh-canary-client`. It interacts securely with a dedicated Solana smart contract targeting intensive Compute Unit burn (~200k CUs) to force validators to reveal true block inclusion priorities over dummy transfers. Executions are highly sanitized to prevent Command Injection.

## Aggregation Plane

`sentinelmesh-aggregator` receives probe envelopes through `POST /v1/ingest`.

Processing pipeline:

1. Request auth is checked using API keys and optional signed-envelope verification.
2. The envelope is appended to the local replay log.
3. The batch is persisted idempotently to PostgreSQL.
4. Endpoint samples are inserted as JSON payloads keyed by batch and endpoint.
5. The in-memory analytics window is updated or rebuilt from durable storage.

The aggregator is intentionally designed to be horizontally scalable:

- durability lives in PostgreSQL plus replay log
- aggregation nodes can rehydrate from storage
- no local in-memory state is required for correctness

### Control Plane (WebSocket Topology)
The Aggregator exposes a secured, authenticated (`/v1/ws/control`) WebSocket endpoint. Agents connect in real-time, allowing the centralized platform to dynamically broadcast RPC Endpoints updates globally. 
The mesh is protected from **TCP Half-Open (Zombie) Connections** through forced 30-second asynchronous **Ping/Pong** heartbeats. Administrative broadcasts (`/v1/admin/broadcast`) are tightly shielded against Remote Fleet Hijacking by strictly requiring `x-sentinelmesh-api-key` headers.

## Analysis Model

`MeshStore` computes:

- RPC consistency index
- slot spread
- block height spread
- blockhash disagreement ratio
- account divergence count
- provider HHI
- propagation summary
- anomaly list

The active view is derived from the freshest sample per `(sentinel_id, endpoint_id)` key within the configured freshness window.

### Command Center (Premium Analytics)
All analytical telemetry is served through a **Vanilla JS/CSS Cyberpunk-themed Dashboard** (`/`) hosted natively by the Aggregator. It leverages **Glassmorphism**, Chart.js CDN for visual Provider HHI Distribution (Doughnut charts), and a fully reactive table with real-time glowing health status indicators synced silently without FOUC (Flash of Unstyled Content).

## Transport and Security

- Ed25519 signed envelopes protect integrity at the application layer
- the aggregator can terminate TLS and require client certificates natively
- API keys provide simple bootstrap auth
- client TLS hooks allow agents to present certificates to an ingress or service mesh
- service-mesh mTLS manifests are provided in `deploy/istio`

## Failure Recovery

- duplicate batch ingest is safe due to idempotent keys
- replay log supports cold-start recovery
- PostgreSQL remains the source of truth for recent history
