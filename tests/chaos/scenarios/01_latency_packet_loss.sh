#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Scenario 1: Latency & packet loss between Agent and Aggregator
#
# Validates: WAL captures batches during network degradation and the flusher
#            re-sends them once connectivity is restored.
#
# Requirements: 20.1
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

SCENARIO_NAME="01_latency_packet_loss"
PROXY_NAME="agent-to-aggregator"
PROXY_LISTEN="${TOXI_PROXY_LISTEN:-127.0.0.1:19480}"
AGGREGATOR_UPSTREAM="${AGGREGATOR_UPSTREAM:-127.0.0.1:9480}"
FAULT_DURATION="${FAULT_DURATION:-15}"
LATENCY_MS="${LATENCY_MS:-2000}"
PACKET_LOSS_PCT="${PACKET_LOSS_PCT:-50}"

run_scenario() {
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Scenario 1: Latency & Packet Loss (Agent↔Aggregator)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  local use_toxiproxy=true
  if ! toxiproxy_available; then
    echo -e "${YELLOW}Toxiproxy not available — falling back to tc/netem${NC}"
    use_toxiproxy=false
  fi

  # ── Phase 1: Capture baseline metrics ──────────────────────────────────
  echo "  [1/4] Capturing baseline metrics..."
  local agent_metrics_before
  agent_metrics_before="$(fetch_metrics "$AGENT_URL")"
  local wal_depth_before
  wal_depth_before="$(extract_metric "$agent_metrics_before" 'sentinelmesh_agent_wal_depth')"
  wal_depth_before="${wal_depth_before:-0}"
  echo "    WAL depth before: ${wal_depth_before}"

  # ── Phase 2: Inject fault ──────────────────────────────────────────────
  echo "  [2/4] Injecting fault: ${LATENCY_MS}ms latency + ${PACKET_LOSS_PCT}% packet loss..."
  if $use_toxiproxy; then
    toxi_create_proxy "$PROXY_NAME" "$PROXY_LISTEN" "$AGGREGATOR_UPSTREAM"
    toxi_add_toxic "$PROXY_NAME" "latency" "latency" 1.0 "latency=${LATENCY_MS}"
    toxi_add_toxic "$PROXY_NAME" "packet_loss" "timeout" "${PACKET_LOSS_PCT}" "timeout=0"
  else
    tc_add_latency "lo" "$LATENCY_MS" "$PACKET_LOSS_PCT"
  fi

  sleep_with_progress "$FAULT_DURATION" "Fault active"

  # ── Phase 3: Verify WAL captured batches ───────────────────────────────
  echo "  [3/4] Checking WAL depth during fault..."
  local agent_metrics_during
  agent_metrics_during="$(fetch_metrics "$AGENT_URL")"
  local wal_depth_during
  wal_depth_during="$(extract_metric "$agent_metrics_during" 'sentinelmesh_agent_wal_depth')"
  wal_depth_during="${wal_depth_during:-0}"
  echo "    WAL depth during fault: ${wal_depth_during}"

  # ── Phase 4: Remove fault and verify flusher drains WAL ────────────────
  echo "  [4/4] Removing fault, waiting for WAL flush..."
  if $use_toxiproxy; then
    toxi_remove_toxic "$PROXY_NAME" "latency"
    toxi_remove_toxic "$PROXY_NAME" "packet_loss"
    toxi_delete_proxy "$PROXY_NAME"
  else
    tc_remove "lo"
  fi

  sleep_with_progress 10 "Waiting for flusher"

  local agent_metrics_after
  agent_metrics_after="$(fetch_metrics "$AGENT_URL")"
  local wal_depth_after
  wal_depth_after="$(extract_metric "$agent_metrics_after" 'sentinelmesh_agent_wal_depth')"
  wal_depth_after="${wal_depth_after:-0}"
  local flush_latency
  flush_latency="$(extract_metric "$agent_metrics_after" 'sentinelmesh_agent_wal_flush_latency_ms_sum')"
  flush_latency="${flush_latency:-N/A}"
  echo "    WAL depth after recovery: ${wal_depth_after}"

  # ── Evaluate ───────────────────────────────────────────────────────────
  local result="fail"
  local wal_grew="false"
  local wal_drained="false"

  # WAL should have grown during the fault (or at least not be empty if
  # the agent was actively probing).
  if awk "BEGIN{exit !(${wal_depth_during} > ${wal_depth_before})}"; then
    wal_grew="true"
  fi

  # After recovery the flusher should have drained the WAL back down.
  if awk "BEGIN{exit !(${wal_depth_after} < ${wal_depth_during})}"; then
    wal_drained="true"
  fi

  if [[ "$wal_grew" == "true" && "$wal_drained" == "true" ]]; then
    result="pass"
  fi

  local details
  details="$(cat <<EOF
{
  "latency_ms": ${LATENCY_MS},
  "packet_loss_pct": ${PACKET_LOSS_PCT},
  "fault_duration_s": ${FAULT_DURATION},
  "wal_depth_before": ${wal_depth_before},
  "wal_depth_during": ${wal_depth_during},
  "wal_depth_after": ${wal_depth_after},
  "wal_grew_during_fault": ${wal_grew},
  "wal_drained_after_recovery": ${wal_drained},
  "flush_latency_ms_sum": "${flush_latency}",
  "fault_injection": "$(if $use_toxiproxy; then echo toxiproxy; else echo tc_netem; fi)"
}
EOF
)"

  write_result "$SCENARIO_NAME" "$result" "$details"
}

run_scenario
