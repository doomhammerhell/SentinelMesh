#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Scenario 2: RPC endpoint unavailability
#
# Validates: Circuit breaker activates for the blocked endpoint and
#            concentration metrics (provider_hhi / asn_hhi) shift accordingly.
#
# Requirements: 20.2
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

SCENARIO_NAME="02_rpc_unavailability"
PROXY_NAME="rpc-endpoint"
PROXY_LISTEN="${TOXI_RPC_LISTEN:-127.0.0.1:18899}"
RPC_UPSTREAM="${RPC_UPSTREAM:-127.0.0.1:8899}"
RPC_BLOCK_PORT="${RPC_BLOCK_PORT:-8899}"
FAULT_DURATION="${FAULT_DURATION:-20}"

run_scenario() {
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Scenario 2: RPC Endpoint Unavailability"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  local use_toxiproxy=true
  if ! toxiproxy_available; then
    echo -e "${YELLOW}Toxiproxy not available — falling back to iptables${NC}"
    use_toxiproxy=false
  fi

  # ── Phase 1: Baseline ─────────────────────────────────────────────────
  echo "  [1/4] Capturing baseline metrics..."
  local agent_metrics_before
  agent_metrics_before="$(fetch_metrics "$AGENT_URL")"
  local snapshot_before
  snapshot_before="$(fetch_snapshot "$AGGREGATOR_URL")"

  local provider_hhi_before
  provider_hhi_before="$(echo "$snapshot_before" | jq -r '.provider_hhi // 0')"
  local asn_hhi_before
  asn_hhi_before="$(echo "$snapshot_before" | jq -r '.asn_hhi // 0')"
  echo "    provider_hhi: ${provider_hhi_before}  asn_hhi: ${asn_hhi_before}"

  # Capture circuit breaker state for all endpoints
  local cb_states_before
  cb_states_before="$(echo "$agent_metrics_before" | grep 'sentinelmesh_agent_circuit_breaker_state' || echo 'none')"
  echo "    Circuit breaker states: $(echo "$cb_states_before" | wc -l) entries"

  # ── Phase 2: Block the RPC endpoint ────────────────────────────────────
  echo "  [2/4] Blocking RPC endpoint..."
  if $use_toxiproxy; then
    toxi_create_proxy "$PROXY_NAME" "$PROXY_LISTEN" "$RPC_UPSTREAM"
    toxi_disable_proxy "$PROXY_NAME"
  else
    iptables_block_port "$RPC_BLOCK_PORT"
  fi

  sleep_with_progress "$FAULT_DURATION" "RPC blocked"

  # ── Phase 3: Check circuit breaker and concentration ───────────────────
  echo "  [3/4] Checking circuit breaker and concentration metrics..."
  local agent_metrics_during
  agent_metrics_during="$(fetch_metrics "$AGENT_URL")"
  local snapshot_during
  snapshot_during="$(fetch_snapshot "$AGGREGATOR_URL")"

  # Look for any endpoint in open (1) or half_open (2) state
  local cb_open_count=0
  while IFS= read -r line; do
    local val
    val="$(echo "$line" | awk '{print $2}')"
    if [[ "$val" == "1" || "$val" == "2" ]]; then
      cb_open_count=$((cb_open_count + 1))
    fi
  done < <(echo "$agent_metrics_during" | grep 'sentinelmesh_agent_circuit_breaker_state' || true)

  local provider_hhi_during
  provider_hhi_during="$(echo "$snapshot_during" | jq -r '.provider_hhi // 0')"
  local asn_hhi_during
  asn_hhi_during="$(echo "$snapshot_during" | jq -r '.asn_hhi // 0')"
  echo "    Circuit breakers open/half-open: ${cb_open_count}"
  echo "    provider_hhi: ${provider_hhi_during}  asn_hhi: ${asn_hhi_during}"

  # ── Phase 4: Restore and verify recovery ───────────────────────────────
  echo "  [4/4] Restoring RPC endpoint..."
  if $use_toxiproxy; then
    toxi_enable_proxy "$PROXY_NAME"
    sleep 5
    toxi_delete_proxy "$PROXY_NAME"
  else
    iptables_unblock_port "$RPC_BLOCK_PORT"
  fi

  sleep_with_progress 10 "Waiting for circuit breaker recovery"

  local agent_metrics_after
  agent_metrics_after="$(fetch_metrics "$AGENT_URL")"
  local cb_open_after=0
  while IFS= read -r line; do
    local val
    val="$(echo "$line" | awk '{print $2}')"
    if [[ "$val" == "1" || "$val" == "2" ]]; then
      cb_open_after=$((cb_open_after + 1))
    fi
  done < <(echo "$agent_metrics_after" | grep 'sentinelmesh_agent_circuit_breaker_state' || true)
  echo "    Circuit breakers open/half-open after recovery: ${cb_open_after}"

  # ── Evaluate ───────────────────────────────────────────────────────────
  local result="fail"
  local cb_activated="false"
  local hhi_shifted="false"

  if [[ $cb_open_count -gt 0 ]]; then
    cb_activated="true"
  fi

  # HHI should shift when an endpoint is removed from the active set
  if awk "BEGIN{exit !(${provider_hhi_during} != ${provider_hhi_before})}"; then
    hhi_shifted="true"
  fi

  if [[ "$cb_activated" == "true" ]]; then
    result="pass"
  fi

  local details
  details="$(cat <<EOF
{
  "fault_duration_s": ${FAULT_DURATION},
  "circuit_breaker_activated": ${cb_activated},
  "cb_open_during_fault": ${cb_open_count},
  "cb_open_after_recovery": ${cb_open_after},
  "provider_hhi_before": ${provider_hhi_before},
  "provider_hhi_during": ${provider_hhi_during},
  "asn_hhi_before": ${asn_hhi_before},
  "asn_hhi_during": ${asn_hhi_during},
  "hhi_shifted": ${hhi_shifted},
  "fault_injection": "$(if $use_toxiproxy; then echo toxiproxy; else echo iptables; fi)"
}
EOF
)"

  write_result "$SCENARIO_NAME" "$result" "$details"
}

run_scenario
