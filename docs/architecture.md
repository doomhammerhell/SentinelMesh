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
