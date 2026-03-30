#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Scenario 3: Aggregator restart
#
# Validates: After the Aggregator process restarts, it reconstructs its
#            in-memory state by hydrating from ClickHouse. The snapshot
#            should contain data that was ingested before the restart.
#
# Requirements: 20.3
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../lib/common.sh"

SCENARIO_NAME="03_aggregator_restart"
AGGREGATOR_BIN="${AGGREGATOR_BIN:-sentinelmesh-aggregator}"
AGGREGATOR_CONFIG="${AGGREGATOR_CONFIG:-config.yaml}"
RESTART_WAIT="${RESTART_WAIT:-10}"

run_scenario() {
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Scenario 3: Aggregator Restart & State Reconstruction"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # ── Phase 1: Capture pre-restart snapshot ──────────────────────────────
  echo "  [1/4] Capturing pre-restart snapshot..."
  local snapshot_before
  snapshot_before="$(fetch_snapshot "$AGGREGATOR_URL")"

  local sample_count_before
  sample_count_before="$(echo "$snapshot_before" | jq -r '.endpoint_samples | length // 0')"
  local anomaly_count_before
  anomaly_count_before="$(echo "$snapshot_before" | jq -r '.anomalies | length // 0')"
  local slot_spread_before
  slot_spread_before="$(echo "$snapshot_before" | jq -r '.slot_spread // "null"')"
  echo "    Samples: ${sample_count_before}  Anomalies: ${anomaly_count_before}  Slot spread: ${slot_spread_before}"

  if [[ "$sample_count_before" == "0" ]]; then
    echo -e "${YELLOW}  Warning: No samples before restart — results may be inconclusive${NC}"
  fi

  # ── Phase 2: Restart the Aggregator ────────────────────────────────────
  echo "  [2/4] Restarting Aggregator..."
  local aggregator_pid
  aggregator_pid="$(pgrep -f "$AGGREGATOR_BIN" 2>/dev/null | head -1 || echo '')"

  if [[ -n "$aggregator_pid" ]]; then
    echo "    Sending SIGTERM to PID ${aggregator_pid}..."
    kill "$aggregator_pid" 2>/dev/null || true
    sleep 2

    # If still alive, force kill
    if kill -0 "$aggregator_pid" 2>/dev/null; then
      echo "    Process still alive, sending SIGKILL..."
      kill -9 "$aggregator_pid" 2>/dev/null || true
    fi
  else
    echo -e "${YELLOW}    Could not find Aggregator process — attempting restart via systemctl${NC}"
    if command -v systemctl &>/dev/null; then
      sudo systemctl restart sentinelmesh-aggregator 2>/dev/null || true
    fi
  fi

  # ── Phase 3: Wait for Aggregator to come back ─────────────────────────
  echo "  [3/4] Waiting for Aggregator to restart..."

  # If we killed it manually, try to restart it
  if [[ -n "$aggregator_pid" ]] && ! command -v systemctl &>/dev/null; then
    echo "    Starting Aggregator in background..."
    nohup "$AGGREGATOR_BIN" --config "$AGGREGATOR_CONFIG" >/dev/null 2>&1 &
  fi

  if ! wait_for_service "${AGGREGATOR_URL}/v1/snapshot" "$RESTART_WAIT"; then
    echo -e "${RED}    Aggregator did not come back within ${RESTART_WAIT}s${NC}"
    write_result "$SCENARIO_NAME" "fail" '{"error":"aggregator_did_not_restart"}'
    return
  fi

  # Give it a moment to hydrate from ClickHouse
  sleep_with_progress 5 "Hydration window"

  # ── Phase 4: Verify state reconstruction ───────────────────────────────
  echo "  [4/4] Verifying state reconstruction from ClickHouse..."
  local snapshot_after
  snapshot_after="$(fetch_snapshot "$AGGREGATOR_URL")"

  local sample_count_after
  sample_count_after="$(echo "$snapshot_after" | jq -r '.endpoint_samples | length // 0')"
  local anomaly_count_after
  anomaly_count_after="$(echo "$snapshot_after" | jq -r '.anomalies | length // 0')"
  local slot_spread_after
  slot_spread_after="$(echo "$snapshot_after" | jq -r '.slot_spread // "null"')"
  echo "    Samples: ${sample_count_after}  Anomalies: ${anomaly_count_after}  Slot spread: ${slot_spread_after}"

  # ── Evaluate ───────────────────────────────────────────────────────────
  local result="fail"
  local state_reconstructed="false"

  # The Aggregator should have hydrated samples from ClickHouse.
  # We consider it a pass if the sample count after restart is > 0
  # (data was recovered) or if it matches the pre-restart count.
  if [[ "$sample_count_after" -gt 0 ]]; then
    state_reconstructed="true"
  fi

  if [[ "$state_reconstructed" == "true" ]]; then
    result="pass"
  fi

  local details
  details="$(cat <<EOF
{
  "sample_count_before": ${sample_count_before},
  "sample_count_after": ${sample_count_after},
  "anomaly_count_before": ${anomaly_count_before},
  "anomaly_count_after": ${anomaly_count_after},
  "slot_spread_before": ${slot_spread_before:-null},
  "slot_spread_after": ${slot_spread_after:-null},
  "state_reconstructed": ${state_reconstructed},
  "restart_wait_s": ${RESTART_WAIT}
}
EOF
)"

  write_result "$SCENARIO_NAME" "$result" "$details"
}

run_scenario
