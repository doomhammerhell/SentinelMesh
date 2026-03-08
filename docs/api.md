# API Specification

## Aggregator

### `POST /v1/ingest`

Accepts a JSON `ProbeEnvelope`.

Headers:

- `x-sentinelmesh-api-key` when API key auth is enabled
- `x-sentinelmesh-key-id` optional metadata from agent publish path

Response:

```json
{
  "accepted": true,
  "batch_id": "00000000-0000-0000-0000-000000000000",
  "endpoints_received": 4,
  "received_at": "2026-03-08T20:00:00Z",
  "persisted": true
}
```

### `GET /v1/snapshot`

Returns the current `NetworkSnapshot`.

### `GET /v1/providers`

Returns a provider status row per active endpoint view.

### `GET /v1/signatures`

Returns the propagation window per tracked signature.

### `GET /v1/accounts`

Returns tracked-account divergence variants.

### `GET /metrics`

Prometheus metrics endpoint.

### `GET /healthz`

Liveness probe.

## Agent

### `GET /v1/status`

Returns sentinel runtime status, publish health and the latest canary signature if available.

### `GET /metrics`

Prometheus metrics endpoint.

### `GET /healthz`

Liveness probe.

## Data Contracts

Primary wire types:

- `ProbeEnvelope`
- `ProbeBatch`
- `EndpointObservation`
- `NetworkSnapshot`
- `ProviderStatus`
- `SignaturePropagation`
- `AccountDivergence`

See the source of truth in:

- [model.rs](/Users/mac/Projects/SentinelMesh/crates/sentinelmesh-core/src/model.rs)
