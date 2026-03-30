#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Scenario 4: Network partition between Aggregator and Kafka
#
# Validates: When the Aggregator cannot reach Kafka, the ClickHouse batch
#            buffer retains records. Once connectivity is restored, buffered
#            records are flushed.
#
# Requirements: 20.4
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

SCENARIO_NAME="04_kafka_partition"
PROXY_NAME="aggregator-to-kafka"
PROXY_LISTEN="${TOXI_KAFKA_LISTEN:-127.0.0.1:19092}"
KAFKA_UPSTREAM="${KAFKA_UPSTREAM:-127.0.0.1:9092}"
KAFKA_BLOCK_PORT="${KAFKA_BLOCK_PORT:-9092}"
FAULT_DURATION="${FAULT_DURATION:-15}"

run_scenario() {
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Scenario 4: Kafka Network Partition"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  local use_toxiproxy=true
  if ! toxiproxy_available; then
    echo -e "${YELLOW}Toxiproxy not available — falling back to iptables${NC}"
    use_toxiproxy=false
  fi

  # ── Phase 1: Baseline ─────────────────────────────────────────────────
  echo "  [1/4] Capturing baseline metrics..."
  local agg_metrics_before
  agg_metrics_before="$(fetch_metrics "$AGGREGATOR_URL")"

  local buffer_size_before
  buffer_size_before="$(extract_metric "$agg_metrics_before" 'sentinelmesh_storage_batch_buffer_size')"
  buffer_size_before="${buffer_size_before:-0}"
  local flush_failures_before
  flush_failures_before="$(extract_metric "$agg_metrics_before" 'sentinelmesh_storage_batch_flush_failures_total')"
  flush_failures_before="${flush_failures_before:-0}"
  echo "    Batch buffer size: ${buffer_size_before}  Flush failures: ${flush_failures_before}"

  # ── Phase 2: Partition Kafka ───────────────────────────────────────────
  echo "  [2/4] Partitioning Aggregator from Kafka..."
  if $use_toxiproxy; then
    toxi_create_proxy "$PROXY_NAME" "$PROXY_LISTEN" "$KAFKA_UPSTREAM"
    toxi_disable_proxy "$PROXY_NAME"
  else
    iptables_block_port "$KAFKA_BLOCK_PORT"
  fi

  sleep_with_progress "$FAULT_DURATION" "Kafka partitioned"

  # ── Phase 3: Verify buffer retention ───────────────────────────────────
  echo "  [3/4] Checking batch buffer during partition..."
  local agg_metrics_during
  agg_metrics_during="$(fetch_metrics "$AGGREGATOR_URL")"

  local buffer_size_during
  buffer_size_during="$(extract_metric "$agg_metrics_during" 'sentinelmesh_storage_batch_buffer_size')"
  buffer_size_during="${buffer_size_during:-0}"
  local flush_failures_during
  flush_failures_during="$(extract_metric "$agg_metrics_during" 'sentinelmesh_storage_batch_flush_failures_total')"
  flush_failures_during="${flush_failures_during:-0}"
  echo "    Batch buffer size: ${buffer_size_during}  Flush failures: ${flush_failures_during}"

  # ── Phase 4: Restore and verify flush ──────────────────────────────────
  echo "  [4/4] Restoring Kafka connectivity..."
  if $use_toxiproxy; then
    toxi_enable_proxy "$PROXY_NAME"
    sleep 5
    toxi_delete_proxy "$PROXY_NAME"
  else
    iptables_unblock_port "$KAFKA_BLOCK_PORT"
  fi

  sleep_with_progress 10 "Waiting for buffer flush"

  local agg_metrics_after
  agg_metrics_after="$(fetch_metrics "$AGGREGATOR_URL")"

  local buffer_size_after
  buffer_size_after="$(extract_metric "$agg_metrics_after" 'sentinelmesh_storage_batch_buffer_size')"
  buffer_size_after="${buffer_size_after:-0}"
  local flush_latency
  flush_latency="$(extract_metric "$agg_metrics_after" 'sentinelmesh_storage_batch_flush_latency_ms_sum')"
  flush_latency="${flush_latency:-N/A}"
  echo "    Batch buffer size after recovery: ${buffer_size_after}"

  # ── Evaluate ───────────────────────────────────────────────────────────
  local result="fail"
  local buffer_retained="false"
  local buffer_flushed="false"

  # Buffer should have grown or at least not been empty during partition
  if awk "BEGIN{exit !(${buffer_size_during} > ${buffer_size_before})}"; then
    buffer_retained="true"
  fi

  # After recovery, buffer should have drained
  if awk "BEGIN{exit !(${buffer_size_after} < ${buffer_size_during})}"; then
    buffer_flushed="true"
  fi

  # Also check that flush failures increased during partition
  local failures_increased="false"
  if awk "BEGIN{exit !(${flush_failures_during} > ${flush_failures_before})}"; then
    failures_increased="true"
  fi

  if [[ "$buffer_retained" == "true" && "$buffer_flushed" == "true" ]]; then
    result="pass"
  fi

  local details
  details="$(cat <<EOF
{
  "fault_duration_s": ${FAULT_DURATION},
  "buffer_size_before": ${buffer_size_before},
  "buffer_size_during": ${buffer_size_during},
  "buffer_size_after": ${buffer_size_after},
  "flush_failures_before": ${flush_failures_before},
  "flush_failures_during": ${flush_failures_during},
  "failures_increased": ${failures_increased},
  "buffer_retained": ${buffer_retained},
  "buffer_flushed": ${buffer_flushed},
  "flush_latency_ms_sum": "${flush_latency}",
  "fault_injection": "$(if $use_toxiproxy; then echo toxiproxy; else echo iptables; fi)"
}
EOF
)"

  write_result "$SCENARIO_NAME" "$result" "$details"
}

run_scenario
