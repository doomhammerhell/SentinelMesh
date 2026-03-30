# Chaos Experiments

Automated chaos test suite for validating SentinelMesh resilience under adverse conditions.

## Prerequisites

- `curl` and `jq` (required)
- [Toxiproxy](https://github.com/Shopify/toxiproxy) (recommended) — or `tc`/`iptables` as fallback (requires `sudo`)
- SentinelMesh Agent and Aggregator running in a staging environment
- Kafka/Redpanda and ClickHouse accessible by the Aggregator

## Quick Start

```bash
# Run all 4 scenarios
./run_chaos.sh

# Run specific scenarios by number
./run_chaos.sh 01 03

# List available scenarios
./run_chaos.sh --list
```

## Scenarios

| # | Scenario | Fault Injection | Validates |
|---|----------|----------------|-----------|
| 01 | Latency & packet loss (Agent↔Aggregator) | Toxiproxy latency + timeout / `tc netem` | WAL captures batches, flusher re-sends after recovery |
| 02 | RPC endpoint unavailability | Toxiproxy disable / `iptables` DROP | Circuit breaker activates, HHI metrics shift |
| 03 | Aggregator restart | SIGTERM + restart | State reconstructed from ClickHouse |
| 04 | Kafka network partition | Toxiproxy disable / `iptables` DROP | Batch buffer retains records, flushes after recovery |

## Configuration

All settings are controlled via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_URL` | `http://127.0.0.1:9481` | Agent metrics endpoint |
| `AGGREGATOR_URL` | `http://127.0.0.1:9480` | Aggregator base URL |
| `API_KEY` | `sentinelmesh-dev-token` | API key for authenticated endpoints |
| `TOXIPROXY_URL` | `http://127.0.0.1:8474` | Toxiproxy API URL |
| `REPORT_DIR` | `tests/chaos/reports` | Report output directory |
| `FAULT_DURATION` | `15` | Duration of fault injection (seconds) |

### Scenario-specific variables

**Scenario 01** (Latency & packet loss):
- `TOXI_PROXY_LISTEN` — Toxiproxy listen address (default: `127.0.0.1:19480`)
- `AGGREGATOR_UPSTREAM` — Aggregator upstream for proxy (default: `127.0.0.1:9480`)
- `LATENCY_MS` — Injected latency in ms (default: `2000`)
- `PACKET_LOSS_PCT` — Packet loss percentage (default: `50`)

**Scenario 02** (RPC unavailability):
- `TOXI_RPC_LISTEN` — Toxiproxy listen address (default: `127.0.0.1:18899`)
- `RPC_UPSTREAM` — RPC upstream for proxy (default: `127.0.0.1:8899`)
- `RPC_BLOCK_PORT` — Port to block via iptables fallback (default: `8899`)

**Scenario 03** (Aggregator restart):
- `AGGREGATOR_BIN` — Aggregator binary name (default: `sentinelmesh-aggregator`)
- `AGGREGATOR_CONFIG` — Config file path (default: `config.yaml`)
- `RESTART_WAIT` — Max wait for restart in seconds (default: `10`)

**Scenario 04** (Kafka partition):
- `TOXI_KAFKA_LISTEN` — Toxiproxy listen address (default: `127.0.0.1:19092`)
- `KAFKA_UPSTREAM` — Kafka upstream for proxy (default: `127.0.0.1:9092`)
- `KAFKA_BLOCK_PORT` — Port to block via iptables fallback (default: `9092`)

## Reports

Each scenario produces a JSON report in `tests/chaos/reports/`. After all scenarios complete, a `summary.json` is generated with aggregated results.

Example summary:

```json
{
  "total": 4,
  "passed": 3,
  "failed": 1,
  "timestamp": "2025-01-15T10:30:00Z",
  "scenarios": [
    {
      "scenario": "01_latency_packet_loss",
      "result": "pass",
      "timestamp": "2025-01-15T10:28:00Z",
      "details": {
        "wal_depth_before": 0,
        "wal_depth_during": 12,
        "wal_depth_after": 0,
        "wal_grew_during_fault": true,
        "wal_drained_after_recovery": true
      }
    }
  ]
}
```

## Toxiproxy Setup

For the recommended Toxiproxy-based approach, start Toxiproxy before running the suite:

```bash
# Start Toxiproxy server
toxiproxy-server &

# Verify it's running
curl http://127.0.0.1:8474/version
```

When using Toxiproxy, configure your Agent/Aggregator to connect through the proxy addresses instead of directly to upstream services. For example, point the Agent's aggregator URL to `127.0.0.1:19480` (the Toxiproxy listen address for scenario 01).

## Fallback: tc / iptables

If Toxiproxy is not available, the scripts automatically fall back to `tc` (traffic control with `netem`) and `iptables` for fault injection. This requires `sudo` privileges and only works on Linux.

## Adding New Scenarios

Create a new script in `tests/chaos/scenarios/` following the naming convention `NN_description.sh`. Source the common library and use the provided helpers:

```bash
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

SCENARIO_NAME="05_my_scenario"

run_scenario() {
  # 1. Capture baseline
  # 2. Inject fault
  # 3. Validate behavior
  # 4. Remove fault and verify recovery
  # 5. Write result
  write_result "$SCENARIO_NAME" "pass" '{"key":"value"}'
}

run_scenario
```
