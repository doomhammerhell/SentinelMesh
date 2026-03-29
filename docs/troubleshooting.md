# SentinelMesh Troubleshooting Guide

This guide covers the most common operational issues encountered when running SentinelMesh in production. Each section describes symptoms, diagnostic steps, root causes, and resolution procedures.

## Prerequisites

- Access to Agent and Aggregator logs (via `journalctl` or container logs)
- Access to Prometheus metrics (default: `http://<host>:9490/metrics` for Agent, `http://<host>:9480/metrics` for Aggregator)
- Access to Agent and Aggregator configuration files
- `curl` and `jq` installed for API queries

---

## 1. Agent Cannot Publish to Aggregator

### Symptoms

- Agent logs show `network dispatch failed, writing to WAL` repeatedly
- `sentinelmesh_agent_wal_depth` metric is increasing
- No new data appears in the Aggregator snapshot endpoint

### Diagnostic Steps

**Step 1: Check Agent logs for publish errors**

```bash
journalctl -u sentinelmesh-agent --since "10 minutes ago" | grep -i "dispatch failed\|publish\|error\|rejected"
```

**Step 2: Verify the Aggregator is reachable from the Agent host**

```bash
# Use the ingestion_url from agent config
curl -v http://<aggregator_host>:9480/v1/ingest
```

A healthy Aggregator returns `405 Method Not Allowed` for GET requests. Connection refused or timeout indicates a network or process issue.

**Step 3: Check the API key configuration**

```bash
# Agent config — publish.auth.api_key must match one of the Aggregator's ingestion.auth.api_keys
grep -A2 "api_key" /etc/sentinelmesh/agent.yaml
```

**Step 4: Check WAL depth metric**

```bash
curl -s http://localhost:9490/metrics | grep sentinelmesh_agent_wal_depth
```

**Step 5: If using Ed25519 signed batches, verify signing configuration**

```bash
# Check Agent logs for signing errors
journalctl -u sentinelmesh-agent --since "10 minutes ago" | grep -i "sign\|key_id\|signer"
```

### Common Root Causes

| Cause | Resolution |
|---|---|
| Aggregator process is down | Restart the Aggregator: `systemctl restart sentinelmesh-aggregator` |
| Firewall blocking port 9480 | Open the port or check security group rules |
| API key mismatch | Ensure `publish.auth.api_key` in Agent config matches an entry in `ingestion.auth.api_keys` on the Aggregator |
| TLS certificate mismatch | Verify `ca_cert_path`, `client_cert_path`, and `client_key_path` in Agent config point to valid, non-expired certificates |
| Ed25519 `key_id` not trusted | Add the Agent's `key_id` and public key to the Aggregator's `trusted_signers` list (see [Key Rotation Runbook](key-rotation-runbook.md)) |
| Clock drift > 30 seconds | The `BatchVerifier` enforces a 30-second anti-replay window on `signed_at`. Sync clocks via NTP |
| DNS resolution failure | Verify the `ingestion_url` hostname resolves correctly from the Agent host |

### Recovery

Once the root cause is fixed, the Agent's WAL flusher automatically retries pending batches every 5 seconds with exponential backoff. Monitor recovery:

```bash
# Watch WAL depth decrease
watch -n5 'curl -s http://localhost:9490/metrics | grep sentinelmesh_agent_wal_depth'
```

---

## 2. Aggregator Not Receiving Data

### Symptoms

- Aggregator `/v1/snapshot` returns stale or empty data
- No new samples appear in the dashboard
- Agent logs show successful publishes (no WAL writes)

### Diagnostic Steps

**Step 1: Verify the Aggregator is running and listening**

```bash
systemctl status sentinelmesh-aggregator
curl -s http://localhost:9480/v1/snapshot | jq '.samples | length'
```

**Step 2: Check Aggregator ingestion logs**

```bash
journalctl -u sentinelmesh-aggregator --since "10 minutes ago" | grep -i "ingest\|envelope\|reject\|error"
```

**Step 3: Check if batches are being rejected due to authentication**

```bash
journalctl -u sentinelmesh-aggregator --since "10 minutes ago" | grep -i "401\|unauthorized\|invalid.*key\|signature.*fail"
```

**Step 4: Verify Kafka connectivity**

```bash
# Check if the Aggregator can reach Kafka/Redpanda
journalctl -u sentinelmesh-aggregator --since "10 minutes ago" | grep -i "kafka\|broker\|partition"
```

**Step 5: Check the Aggregator's bind address**

```bash
# Ensure the Aggregator is listening on the expected interface
ss -tlnp | grep 9480
```

### Common Root Causes

| Cause | Resolution |
|---|---|
| Aggregator not running | `systemctl start sentinelmesh-aggregator` |
| Bind address is `127.0.0.1` but Agent connects from another host | Change `server.bind_address` to `0.0.0.0:9480` in Aggregator config |
| `require_signed_batches: true` but Agent is not signing | Either enable signing on the Agent or set `require_signed_batches: false` |
| Kafka broker unreachable | Verify `storage.kafka.brokers` in Aggregator config; check Kafka/Redpanda health |
| `max_batch_bytes` exceeded | Reduce the number of endpoints per Agent or increase `ingestion.max_batch_bytes` |
| Agent sending to wrong URL | Verify `publish.ingestion_url` in Agent config points to the correct Aggregator address and port |

### Verification

After resolving, confirm data is flowing:

```bash
# Check that sample count increases over time
curl -s http://localhost:9480/v1/snapshot | jq '.samples | length'
sleep 30
curl -s http://localhost:9480/v1/snapshot | jq '.samples | length'
```

---

## 3. WAL Growing Indefinitely

### Symptoms

- `sentinelmesh_agent_wal_depth` metric is at or near `wal_max_entries` (default: 10,000)
- `sentinelmesh_agent_wal_evictions_total` counter is increasing
- Agent disk usage is growing
- Agent logs show repeated `wal capacity exceeded, evicting oldest probe batch` warnings

### Diagnostic Steps

**Step 1: Check WAL metrics**

```bash
curl -s http://localhost:9490/metrics | grep sentinelmesh_agent_wal
```

Key metrics to watch:
- `sentinelmesh_agent_wal_depth` — current queue depth
- `sentinelmesh_agent_wal_flush_latency_ms` — time to flush entries to disk
- `sentinelmesh_agent_wal_evictions_total` — number of evicted (lost) batches

**Step 2: Check if the flusher is running**

```bash
journalctl -u sentinelmesh-agent --since "10 minutes ago" | grep -i "flush\|wal"
```

**Step 3: Verify the Aggregator is reachable (same as Section 1)**

```bash
curl -v http://<aggregator_host>:9480/v1/ingest
```

**Step 4: Check disk space on the Agent host**

```bash
df -h $(grep -oP 'wal_path:\s*\K.*' /etc/sentinelmesh/agent.yaml 2>/dev/null || echo "/var/lib/sentinelmesh")
```

### Common Root Causes

| Cause | Resolution |
|---|---|
| Aggregator is unreachable for an extended period | Fix network connectivity or restart the Aggregator (see Sections 1 and 2) |
| Flusher is stuck due to repeated auth failures | Fix authentication configuration; the flusher retries every 5 seconds |
| Disk I/O bottleneck on sled database | Move the WAL directory to faster storage (SSD) |
| `wal_max_entries` set too low for the outage duration | Increase `runtime.wal_max_entries` in Agent config to retain more batches during outages |

### Tuning WAL Capacity

The WAL uses a ring buffer eviction strategy. When the WAL reaches `wal_max_entries`, the oldest batch is evicted to make room for new data. Adjust based on your tolerance for data loss during outages:

```yaml
# agent.yaml
runtime:
  wal_max_entries: 50000  # Retain more batches (default: 10000)
```

Estimate required capacity: if the Agent produces 1 batch every 15 seconds and you want to survive a 2-hour outage without data loss:

```
2 hours × 60 min × 4 batches/min = 480 batches
```

### Recovery

Once the Aggregator is reachable again, the flusher drains the WAL automatically. Monitor:

```bash
watch -n5 'curl -s http://localhost:9490/metrics | grep sentinelmesh_agent_wal_depth'
```

---

## 4. ClickHouse Not Hydrating

### Symptoms

- After Aggregator restart, `/v1/snapshot` returns empty or stale data
- Aggregator logs show ClickHouse connection errors
- `sentinelmesh_storage_batch_flush_failures_total` counter is increasing
- `sentinelmesh_storage_batch_buffer_size` metric is growing

### Diagnostic Steps

**Step 1: Check Aggregator logs for ClickHouse errors**

```bash
journalctl -u sentinelmesh-aggregator --since "10 minutes ago" | grep -i "clickhouse\|hydrat\|storage\|batch.*fail"
```

**Step 2: Verify ClickHouse connectivity**

```bash
# Use the URL from aggregator config (storage.clickhouse.url)
curl -s "http://localhost:8123/?query=SELECT+1"
# Expected output: 1
```

**Step 3: Verify the database and table exist**

```bash
curl -s "http://localhost:8123/?query=SHOW+TABLES+FROM+sentinelmesh"
```

You should see `probe_batches` (and any other SentinelMesh tables) in the output.

**Step 4: Check ClickHouse credentials**

```bash
# Test with the credentials from aggregator config
curl -s "http://localhost:8123/?user=sentinelmesh&password=sentinelmesh&query=SELECT+count()+FROM+sentinelmesh.probe_batches"
```

**Step 5: Check batch writer metrics**

```bash
curl -s http://localhost:9480/metrics | grep sentinelmesh_storage_batch
```

Key metrics:
- `sentinelmesh_storage_batch_buffer_size` — records waiting to be flushed
- `sentinelmesh_storage_batch_flush_failures_total` — failed flush attempts (records are retained for retry)

**Step 6: Check the adaptive refresh interval**

```bash
curl -s http://localhost:9480/metrics | grep sentinelmesh_aggregator_refresh_interval_ms
```

If this value is very high, the Aggregator has backed off because no new data was arriving from ClickHouse. Ingesting a new envelope resets it to the base interval.

### Common Root Causes

| Cause | Resolution |
|---|---|
| ClickHouse is down | Start ClickHouse: `systemctl start clickhouse-server` |
| Wrong URL, user, or password in config | Verify `storage.clickhouse.url`, `user`, `password`, and `database` in Aggregator config |
| Schema not initialized | The Aggregator runs `ensure_schema()` on startup. Check logs for schema creation errors. Manually run the DDL if needed |
| Network partition between Aggregator and ClickHouse | Check firewall rules and network connectivity to port 8123 |
| ClickHouse disk full | Free disk space or configure TTL-based retention on the `probe_batches` table |
| Batch timeout too aggressive | Increase `storage.clickhouse.batch_timeout_secs` (default: 5) if ClickHouse is slow |

### Forcing a Refresh

If the adaptive backoff has increased the refresh interval too much, ingesting a new envelope from any Agent resets the interval to the base value. Alternatively, restart the Aggregator:

```bash
systemctl restart sentinelmesh-aggregator
```

### ClickHouse Configuration Reference

```yaml
# aggregator.yaml
storage:
  clickhouse:
    url: http://127.0.0.1:8123
    user: sentinelmesh
    password: sentinelmesh
    database: sentinelmesh
    refresh_interval: 10s    # Base refresh interval for hydration
    batch_size: 100          # Flush after this many records
    batch_timeout_secs: 5    # Flush after this many seconds even if batch_size not reached
```

---

## 5. Alerts Not Firing

### Symptoms

- Anomalies appear in `/v1/snapshot` but no webhook notifications are received
- Alert webhooks are configured but never called
- `sentinelmesh_aggregator_alerts_dispatched_total` metric is not increasing

### Diagnostic Steps

**Step 1: Verify alerts are configured**

```bash
grep -A10 "alerts:" /etc/sentinelmesh/aggregator.yaml
```

Alerts require both `min_severity` and at least one `webhooks` entry:

```yaml
alerts:
  min_severity: warning
  rate_limit_window_secs: 900  # 15 minutes (default)
  webhooks:
    - url: "https://hooks.slack.com/services/T0000/B0000/XXXXX"
      headers:
        Content-Type: application/json
```

**Step 2: Check if anomalies are being generated**

```bash
curl -s http://localhost:9480/v1/snapshot | jq '.anomalies'
```

If the anomalies array is empty, the issue is not with alerting but with detection. Verify that enough data is being ingested and that detection thresholds are being met.

**Step 3: Check Aggregator logs for alert dispatch activity**

```bash
journalctl -u sentinelmesh-aggregator --since "30 minutes ago" | grep -i "alert\|dispatch\|webhook\|rate.limit"
```

**Step 4: Check if rate limiting is suppressing alerts**

The AlertSink rate-limits dispatches per anomaly code. Within the configured `rate_limit_window_secs` (default: 900 seconds / 15 minutes), only the first occurrence of each anomaly code triggers a webhook.

```bash
# Check the rate limit window in config
grep "rate_limit_window" /etc/sentinelmesh/aggregator.yaml
```

**Step 5: Verify webhook endpoint is reachable from the Aggregator**

```bash
# Test connectivity to the webhook URL
curl -v -X POST "https://hooks.slack.com/services/T0000/B0000/XXXXX" \
  -H "Content-Type: application/json" \
  -d '{"text": "SentinelMesh test alert"}'
```

**Step 6: Check for webhook timeouts**

The AlertSink enforces a 10-second timeout per webhook call. Slow endpoints will be logged and skipped.

```bash
journalctl -u sentinelmesh-aggregator --since "1 hour ago" | grep -i "timeout\|webhook"
```

### Common Root Causes

| Cause | Resolution |
|---|---|
| Alerts section commented out or missing in config | Uncomment and configure the `alerts` block in Aggregator config |
| `min_severity` set too high | Lower `min_severity` (e.g., from `critical` to `warning`) to receive more alerts |
| Rate limiting suppressing repeated alerts | Wait for the rate limit window to expire, or reduce `rate_limit_window_secs` for faster re-alerting |
| Webhook URL unreachable or returning errors | Verify the URL, check firewall rules, and test with `curl` |
| Webhook timeout (> 10 seconds) | Investigate the webhook endpoint's latency; the AlertSink does not retry timed-out dispatches |
| No anomalies being generated | Verify data is flowing (Sections 1–4) and that detection thresholds are appropriate |
| Backpressure dropping anomalies | If the alert channel is full, older anomalies are dropped. Check logs for backpressure warnings |

### Alert Configuration Reference

```yaml
# aggregator.yaml
alerts:
  min_severity: warning          # Minimum severity to dispatch: info, warning, critical
  rate_limit_window_secs: 900    # Suppress duplicate anomaly codes within this window (default: 15 min)
  webhooks:
    - url: "https://hooks.slack.com/services/T0000/B0000/XXXXX"
      headers:
        Content-Type: application/json
    - url: "https://events.pagerduty.com/v2/enqueue"
      headers:
        Authorization: "Token token=YOUR_API_KEY"
```

---

## Quick Reference: Key Metrics

| Metric | Component | Description |
|---|---|---|
| `sentinelmesh_agent_wal_depth` | Agent | Current WAL queue depth |
| `sentinelmesh_agent_wal_flush_latency_ms` | Agent | Time to flush a WAL entry to disk |
| `sentinelmesh_agent_wal_evictions_total` | Agent | Total evicted batches (data loss indicator) |
| `sentinelmesh_agent_circuit_breaker_state` | Agent | Per-endpoint circuit breaker state (0=closed, 1=open, 2=half_open) |
| `sentinelmesh_agent_batches_total` | Agent | Total batches produced |
| `sentinelmesh_storage_batch_buffer_size` | Aggregator | ClickHouse batch buffer pending records |
| `sentinelmesh_storage_batch_flush_failures_total` | Aggregator | Failed ClickHouse batch flushes |
| `sentinelmesh_aggregator_refresh_interval_ms` | Aggregator | Current adaptive refresh interval |

## Quick Reference: Key Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/v1/ingest` | POST | Agent → Aggregator batch ingestion |
| `/v1/snapshot` | GET | Current network snapshot with anomalies |
| `/v1/signatures` | GET | Tracked signature propagation data |
| `/v1/validator-history` | GET | Validator identity change history |
| `/v1/ws/control` | WebSocket | Control plane for live endpoint updates |

## Quick Reference: Circuit Breaker

If all RPC endpoints are in `Open` state, the Agent activates a total blackout fallback and probes all endpoints regardless of circuit breaker state. This prevents complete blindness when all endpoints are temporarily degraded.

Check circuit breaker state per endpoint:

```bash
curl -s http://localhost:9490/metrics | grep sentinelmesh_agent_circuit_breaker_state
```

Values: `0` = Closed (healthy), `1` = Open (failing, skipped), `2` = Half-Open (recovery probe).

Configuration:

```yaml
# agent.yaml
runtime:
  circuit_breaker:
    failure_threshold: 3       # Consecutive failures before opening
    recovery_interval_secs: 60 # Seconds before attempting recovery probe
```
