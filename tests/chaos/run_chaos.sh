#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SentinelMesh Chaos Test Runner
#
# Orchestrates all chaos scenarios and produces a consolidated report.
#
# Usage:
#   ./run_chaos.sh                  # Run all scenarios
#   ./run_chaos.sh 01 03            # Run specific scenarios by number
#   ./run_chaos.sh --list           # List available scenarios
#
# Environment variables:
#   AGENT_URL          Agent metrics endpoint   (default: http://127.0.0.1:9481)
#   AGGREGATOR_URL     Aggregator base URL      (default: http://127.0.0.1:9480)
#   API_KEY            API key for auth         (default: sentinelmesh-dev-token)
#   TOXIPROXY_URL      Toxiproxy API            (default: http://127.0.0.1:8474)
#   REPORT_DIR         Report output directory  (default: tests/chaos/reports)
#   FAULT_DURATION     Fault duration in secs   (default: 15)
#
# Requirements: curl, jq
# Optional:     toxiproxy-cli (falls back to tc/iptables if unavailable)
#
# Validates: Requirements 20.1, 20.2, 20.3, 20.4, 20.5
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"

SCENARIOS_DIR="${SCRIPT_DIR}/scenarios"

# ---------------------------------------------------------------------------
# List available scenarios
# ---------------------------------------------------------------------------
list_scenarios() {
  echo "Available chaos scenarios:"
  echo ""
  for f in "${SCENARIOS_DIR}"/*.sh; do
    [[ ! -f "$f" ]] && continue
    local name
    name="$(basename "$f" .sh)"
    local desc
    desc="$(head -5 "$f" | grep '^# Scenario' | sed 's/^# //' || echo "$name")"
    echo "  ${name}  ${desc}"
  done
}

# ---------------------------------------------------------------------------
# Run a single scenario
# ---------------------------------------------------------------------------
run_single() {
  local script="$1"
  echo ""
  bash "$script"
  echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  if [[ "${1:-}" == "--list" || "${1:-}" == "-l" ]]; then
    list_scenarios
    exit 0
  fi

  if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    head -20 "$0" | grep '^#' | sed 's/^# //'
    exit 0
  fi

  check_prerequisites
  init_report

  # Clean previous reports
  rm -f "${REPORT_DIR}"/*.json 2>/dev/null || true

  echo "╔══════════════════════════════════════════════════════╗"
  echo "║        SentinelMesh Chaos Test Suite                 ║"
  echo "╠══════════════════════════════════════════════════════╣"
  echo "║  Agent:      ${AGENT_URL}"
  echo "║  Aggregator: ${AGGREGATOR_URL}"
  echo "║  Toxiproxy:  ${TOXIPROXY_URL}"
  echo "║  Reports:    ${REPORT_DIR}"
  echo "╚══════════════════════════════════════════════════════╝"
  echo ""

  # Check Toxiproxy availability
  if toxiproxy_available; then
    echo -e "${GREEN}✓ Toxiproxy available${NC}"
  else
    echo -e "${YELLOW}⚠ Toxiproxy not available — will use tc/iptables fallback (may require sudo)${NC}"
  fi
  echo ""

  local scenarios_to_run=()

  if [[ $# -gt 0 ]]; then
    # Run specific scenarios by number prefix
    for num in "$@"; do
      local pattern="${SCENARIOS_DIR}/${num}*.sh"
      local found=false
      for f in $pattern; do
        if [[ -f "$f" ]]; then
          scenarios_to_run+=("$f")
          found=true
        fi
      done
      if ! $found; then
        echo -e "${RED}No scenario matching '${num}' found${NC}" >&2
      fi
    done
  else
    # Run all scenarios in order
    for f in "${SCENARIOS_DIR}"/*.sh; do
      [[ -f "$f" ]] && scenarios_to_run+=("$f")
    done
  fi

  if [[ ${#scenarios_to_run[@]} -eq 0 ]]; then
    echo -e "${RED}No scenarios to run${NC}" >&2
    exit 1
  fi

  echo "Running ${#scenarios_to_run[@]} scenario(s)..."

  local exit_code=0
  for script in "${scenarios_to_run[@]}"; do
    if ! run_single "$script"; then
      echo -e "${YELLOW}Scenario $(basename "$script") exited with error${NC}"
      exit_code=1
    fi
  done

  # Generate consolidated summary
  write_summary

  exit $exit_code
}

main "$@"
