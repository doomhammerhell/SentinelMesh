#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SentinelMesh Chaos Tests — shared helpers
# ---------------------------------------------------------------------------
set -euo pipefail

# Defaults (override via env)
AGENT_URL="${AGENT_URL:-http://127.0.0.1:9481}"
AGGREGATOR_URL="${AGGREGATOR_URL:-http://127.0.0.1:9480}"
API_KEY="${API_KEY:-sentinelmesh-dev-token}"
TOXIPROXY_URL="${TOXIPROXY_URL:-http://127.0.0.1:8474}"
REPORT_DIR="${REPORT_DIR:-$(dirname "${BASH_SOURCE[0]}")/../reports}"

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Colour

# ---------------------------------------------------------------------------
# Prerequisites check
# ---------------------------------------------------------------------------
check_prerequisites() {
  local missing=()
  for cmd in curl jq; do
    if ! command -v "$cmd" &>/dev/null; then
      missing+=("$cmd")
    fi
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo -e "${RED}ERROR: Missing required tools: ${missing[*]}${NC}" >&2
    exit 1
  fi
}

# Check if Toxiproxy is reachable; returns 0 if yes, 1 if no.
toxiproxy_available() {
  curl -sf "${TOXIPROXY_URL}/version" &>/dev/null
}

# ---------------------------------------------------------------------------
# Toxiproxy helpers
# ---------------------------------------------------------------------------
toxi_create_proxy() {
  local name="$1" listen="$2" upstream="$3"
  curl -sf -X POST "${TOXIPROXY_URL}/proxies" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${name}\",\"listen\":\"${listen}\",\"upstream\":\"${upstream}\"}" \
    >/dev/null 2>&1 || true
}

toxi_delete_proxy() {
  local name="$1"
  curl -sf -X DELETE "${TOXIPROXY_URL}/proxies/${name}" >/dev/null 2>&1 || true
}

toxi_add_toxic() {
  local proxy="$1" toxic_name="$2" toxic_type="$3" toxicity="$4"
  shift 4
  # Remaining args are key=value pairs for attributes
  local attrs="{"
  local first=true
  for kv in "$@"; do
    local key="${kv%%=*}"
    local val="${kv#*=}"
    if $first; then first=false; else attrs+=","; fi
    attrs+="\"${key}\":${val}"
  done
  attrs+="}"

  curl -sf -X POST "${TOXIPROXY_URL}/proxies/${proxy}/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${toxic_name}\",\"type\":\"${toxic_type}\",\"toxicity\":${toxicity},\"attributes\":${attrs}}" \
    >/dev/null 2>&1
}

toxi_remove_toxic() {
  local proxy="$1" toxic_name="$2"
  curl -sf -X DELETE "${TOXIPROXY_URL}/proxies/${proxy}/toxics/${toxic_name}" \
    >/dev/null 2>&1 || true
}

toxi_disable_proxy() {
  local name="$1"
  curl -sf -X POST "${TOXIPROXY_URL}/proxies/${name}" \
    -H 'Content-Type: application/json' \
    -d '{"enabled":false}' \
    >/dev/null 2>&1
}

toxi_enable_proxy() {
  local name="$1"
  curl -sf -X POST "${TOXIPROXY_URL}/proxies/${name}" \
    -H 'Content-Type: application/json' \
    -d '{"enabled":true}' \
    >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# tc / iptables fallback helpers
# ---------------------------------------------------------------------------
tc_add_latency() {
  local iface="${1:-lo}" delay_ms="$2" loss_pct="${3:-0}"
  sudo tc qdisc add dev "$iface" root netem delay "${delay_ms}ms" loss "${loss_pct}%" 2>/dev/null || \
    sudo tc qdisc change dev "$iface" root netem delay "${delay_ms}ms" loss "${loss_pct}%" 2>/dev/null
}

tc_remove() {
  local iface="${1:-lo}"
  sudo tc qdisc del dev "$iface" root 2>/dev/null || true
}

iptables_block_port() {
  local port="$1"
  sudo iptables -A OUTPUT -p tcp --dport "$port" -j DROP 2>/dev/null
}

iptables_unblock_port() {
  local port="$1"
  sudo iptables -D OUTPUT -p tcp --dport "$port" -j DROP 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Metrics / snapshot helpers
# ---------------------------------------------------------------------------
fetch_snapshot() {
  local url="${1:-$AGGREGATOR_URL}"
  curl -sf "${url}/v1/snapshot" \
    -H "x-sentinelmesh-api-key: ${API_KEY}" 2>/dev/null || echo '{}'
}

fetch_metrics() {
  local url="${1:-$AGENT_URL}"
  curl -sf "${url}/metrics" 2>/dev/null || echo ''
}

extract_metric() {
  local metrics_text="$1" metric_name="$2"
  echo "$metrics_text" | grep "^${metric_name}" | head -1 | awk '{print $2}'
}

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
init_report() {
  mkdir -p "$REPORT_DIR"
}

# Write a scenario result to the JSON report
# Usage: write_result <scenario_name> <pass|fail> <details_json>
write_result() {
  local scenario="$1" result="$2" details="$3"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local file="${REPORT_DIR}/${scenario}.json"
  cat > "$file" <<EOF
{
  "scenario": "${scenario}",
  "result": "${result}",
  "timestamp": "${ts}",
  "details": ${details}
}
EOF
  if [[ "$result" == "pass" ]]; then
    echo -e "${GREEN}[PASS]${NC} ${scenario}"
  else
    echo -e "${RED}[FAIL]${NC} ${scenario}"
  fi
}

# Aggregate all scenario results into a single summary
write_summary() {
  local summary_file="${REPORT_DIR}/summary.json"
  local total=0 passed=0 failed=0
  local scenarios="["
  local first=true

  for f in "${REPORT_DIR}"/*.json; do
    [[ "$(basename "$f")" == "summary.json" ]] && continue
    [[ ! -f "$f" ]] && continue
    total=$((total + 1))
    local res
    res="$(jq -r '.result' "$f" 2>/dev/null || echo 'unknown')"
    if [[ "$res" == "pass" ]]; then
      passed=$((passed + 1))
    else
      failed=$((failed + 1))
    fi
    if $first; then first=false; else scenarios+=","; fi
    scenarios+="$(cat "$f")"
  done
  scenarios+="]"

  cat > "$summary_file" <<EOF
{
  "total": ${total},
  "passed": ${passed},
  "failed": ${failed},
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scenarios": ${scenarios}
}
EOF

  echo ""
  echo "════════════════════════════════════════════════════"
  echo "  SentinelMesh Chaos Test Summary"
  echo "════════════════════════════════════════════════════"
  echo "  Total:  ${total}"
  echo -e "  Passed: ${GREEN}${passed}${NC}"
  echo -e "  Failed: ${RED}${failed}${NC}"
  echo "  Report: ${summary_file}"
  echo "════════════════════════════════════════════════════"
}

# ---------------------------------------------------------------------------
# Wait / retry helpers
# ---------------------------------------------------------------------------
wait_for_service() {
  local url="$1" max_wait="${2:-30}"
  local elapsed=0
  while ! curl -sf "$url" >/dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ $elapsed -ge $max_wait ]]; then
      echo -e "${RED}Timed out waiting for ${url}${NC}" >&2
      return 1
    fi
  done
}

sleep_with_progress() {
  local secs="$1" label="${2:-Waiting}"
  for ((i = 1; i <= secs; i++)); do
    printf "\r  %s... %d/%ds" "$label" "$i" "$secs"
    sleep 1
  done
  printf "\r  %s... done.          \n" "$label"
}
