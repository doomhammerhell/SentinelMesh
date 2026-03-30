import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ---------------------------------------------------------------------------
// Configuration via environment variables
// ---------------------------------------------------------------------------
const VUS = __ENV.VUS ? parseInt(__ENV.VUS, 10) : 50;
const DURATION = __ENV.DURATION || "60s";
const BATCH_SIZE = __ENV.BATCH_SIZE ? parseInt(__ENV.BATCH_SIZE, 10) : 3;
const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:9480";
const API_KEY = __ENV.API_KEY || "sentinelmesh-dev-token";

// ---------------------------------------------------------------------------
// k6 options — thresholds enforce p99 < 500ms at 50 VUs
// ---------------------------------------------------------------------------
export const options = {
  vus: VUS,
  duration: DURATION,
  thresholds: {
    http_req_duration: ["p(99)<500"],
    ingest_errors: ["rate<0.05"],
  },
};

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const envelopesProcessed = new Counter("envelopes_processed");
const ingestErrors = new Rate("ingest_errors");
const ingestLatency = new Trend("ingest_latency_ms", true);

// ---------------------------------------------------------------------------
// Helpers for realistic data generation
// ---------------------------------------------------------------------------
let requestCounter = 0;

function uuidV4() {
  // Simple pseudo-UUID v4 using k6's Math.random
  const hex = "0123456789abcdef";
  let uuid = "";
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) {
      uuid += "-";
    } else if (i === 14) {
      uuid += "4";
    } else if (i === 19) {
      uuid += hex[(Math.random() * 4) | 8];
    } else {
      uuid += hex[(Math.random() * 16) | 0];
    }
  }
  return uuid;
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomHex(len) {
  const hex = "0123456789abcdef";
  let s = "";
  for (let i = 0; i < len; i++) {
    s += hex[(Math.random() * 16) | 0];
  }
  return s;
}

function randomBase58(len) {
  const chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let s = "";
  for (let i = 0; i < len; i++) {
    s += chars[(Math.random() * chars.length) | 0];
  }
  return s;
}

// ---------------------------------------------------------------------------
// Realistic endpoint pool
// ---------------------------------------------------------------------------
const PROVIDERS = ["Helius", "Triton", "QuickNode", "Alchemy", "GenesysGo"];
const REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "eu-central-1"];
const ASNS = [16509, 14618, 13335, 20473, 63949];
const SENTINEL_IDS = ["sentinel-alpha", "sentinel-beta", "sentinel-gamma", "sentinel-delta"];
const SENTINEL_LOCATIONS = ["New York", "San Francisco", "Frankfurt", "Singapore", "London"];

function makeEndpointConfig(idx) {
  const provider = PROVIDERS[idx % PROVIDERS.length];
  const region = REGIONS[idx % REGIONS.length];
  return {
    id: `ep-${provider.toLowerCase()}-${idx}`,
    label: `${provider} ${region}`,
    provider: provider,
    region: region,
    rpc_url: `https://${provider.toLowerCase()}.example.com/rpc-${idx}`,
    tags: {},
  };
}

function makeProbeValue(value, latencyRange) {
  return {
    value: value,
    latency_ms: randomInt(latencyRange[0], latencyRange[1]),
    error: null,
  };
}

// ---------------------------------------------------------------------------
// Generate a single realistic EndpointObservation
// ---------------------------------------------------------------------------
function makeEndpointObservation(idx) {
  const slot = randomInt(280000000, 290000000);
  const blockHeight = slot - randomInt(0, 5);

  return {
    endpoint: makeEndpointConfig(idx),
    overall_latency_ms: randomInt(20, 300),
    health: makeProbeValue("ok", [1, 10]),
    slot: makeProbeValue(slot, [5, 50]),
    block_height: makeProbeValue(blockHeight, [5, 50]),
    latest_blockhash: makeProbeValue(
      {
        blockhash: randomBase58(44),
        last_valid_block_height: blockHeight + randomInt(100, 200),
        context_slot: slot,
      },
      [10, 80]
    ),
    version: makeProbeValue(`1.18.${randomInt(10, 25)}`, [2, 15]),
    identity: makeProbeValue({ identity: randomBase58(44) }, [3, 20]),
    vote_accounts: makeProbeValue(
      {
        current_vote_accounts: randomInt(1800, 2200),
        delinquent_vote_accounts: randomInt(0, 50),
        current_activated_stake: randomInt(300000000, 500000000),
        delinquent_activated_stake: randomInt(0, 5000000),
      },
      [30, 150]
    ),
    cluster_nodes: makeProbeValue(
      {
        nodes: randomInt(2500, 3500),
        rpc_nodes: randomInt(200, 500),
        tpu_nodes: randomInt(1500, 2500),
      },
      [20, 100]
    ),
    leader_schedule: makeProbeValue(
      {
        validators: randomInt(1500, 2000),
        total_leader_slots: 432000,
        schedule: null,
      },
      [50, 200]
    ),
    accounts: makeAccountObservations(),
    signatures: makeSignatureObservations(),
    probe_errors: [],
    transaction_order: [],
  };
}

// ---------------------------------------------------------------------------
// Generate tracked account observations (2-4 per endpoint)
// ---------------------------------------------------------------------------
const TRACKED_PUBKEYS = [
  randomBase58(44),
  randomBase58(44),
  randomBase58(44),
  randomBase58(44),
];

function makeAccountObservations() {
  const count = randomInt(2, 4);
  const accounts = [];
  for (let i = 0; i < count; i++) {
    accounts.push({
      pubkey: TRACKED_PUBKEYS[i % TRACKED_PUBKEYS.length],
      commitment: "confirmed",
      slot: randomInt(280000000, 290000000),
      state_hash: randomHex(64),
      lamports: randomInt(1000000, 50000000000),
      owner: "11111111111111111111111111111111",
      executable: false,
      rent_epoch: 0,
      data_len: randomInt(0, 1024),
      latency_ms: randomInt(5, 80),
      error: null,
    });
  }
  return accounts;
}

// ---------------------------------------------------------------------------
// Generate tracked signature observations (1-3 per endpoint)
// ---------------------------------------------------------------------------
function makeSignatureObservations() {
  const count = randomInt(1, 3);
  const sigs = [];
  for (let i = 0; i < count; i++) {
    const found = Math.random() > 0.1;
    sigs.push({
      signature: randomBase58(88),
      latency_ms: randomInt(5, 100),
      status: found
        ? {
            slot: randomInt(280000000, 290000000),
            confirmation_status: "confirmed",
            confirmations: randomInt(1, 32),
            finalized: Math.random() > 0.5,
            err: null,
          }
        : null,
      error: found ? null : "signature not found",
    });
  }
  return sigs;
}

// ---------------------------------------------------------------------------
// Build a complete ProbeEnvelope with BATCH_SIZE endpoints
// ---------------------------------------------------------------------------
function makeProbeEnvelope(vuId) {
  requestCounter++;
  const sentinelIdx = vuId % SENTINEL_IDS.length;

  const endpoints = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    endpoints.push(makeEndpointObservation(i));
  }

  return {
    batch: {
      schema_version: 2,
      batch_id: uuidV4(),
      sampled_at: new Date().toISOString(),
      sentinel_id: SENTINEL_IDS[sentinelIdx],
      sentinel_location: SENTINEL_LOCATIONS[sentinelIdx],
      asn: ASNS[sentinelIdx % ASNS.length],
      endpoints: endpoints,
    },
    auth: null,
  };
}

// ---------------------------------------------------------------------------
// Main VU function
// ---------------------------------------------------------------------------
export default function () {
  const envelope = makeProbeEnvelope(__VU);
  const payload = JSON.stringify(envelope);

  const response = http.post(`${BASE_URL}/v1/ingest`, payload, {
    headers: {
      "Content-Type": "application/json",
      "x-sentinelmesh-api-key": API_KEY,
    },
    tags: { name: "ingest" },
  });

  const success = check(response, {
    "status is 200": (r) => r.status === 200,
  });

  ingestLatency.add(response.timings.duration);
  envelopesProcessed.add(1);
  ingestErrors.add(!success);

  sleep(0.1);
}

// ---------------------------------------------------------------------------
// Summary reporter — throughput, latency percentiles, error rate
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const duration = data.metrics.http_req_duration;
  const reqs = data.metrics.http_reqs;
  const errors = data.metrics.ingest_errors;

  const throughput =
    reqs && reqs.values && reqs.values.rate
      ? reqs.values.rate.toFixed(2)
      : "N/A";

  const p50 =
    duration && duration.values && duration.values["p(50)"] != null
      ? duration.values["p(50)"].toFixed(2)
      : "N/A";
  const p95 =
    duration && duration.values && duration.values["p(95)"] != null
      ? duration.values["p(95)"].toFixed(2)
      : "N/A";
  const p99 =
    duration && duration.values && duration.values["p(99)"] != null
      ? duration.values["p(99)"].toFixed(2)
      : "N/A";

  const errorRate =
    errors && errors.values && errors.values.rate != null
      ? (errors.values.rate * 100).toFixed(2)
      : "N/A";

  const summary = `
╔══════════════════════════════════════════════════════════╗
║           SentinelMesh Load Test Summary                ║
╠══════════════════════════════════════════════════════════╣
║  VUs: ${String(VUS).padEnd(10)} Duration: ${DURATION.padEnd(15)}      ║
║  Batch size: ${String(BATCH_SIZE).padEnd(8)} endpoints per envelope       ║
╠══════════════════════════════════════════════════════════╣
║  Throughput:  ${throughput.padEnd(10)} envelopes/sec              ║
║  Latency p50: ${p50.padEnd(10)} ms                            ║
║  Latency p95: ${p95.padEnd(10)} ms                            ║
║  Latency p99: ${p99.padEnd(10)} ms                            ║
║  Error rate:  ${errorRate.padEnd(10)} %                             ║
╠══════════════════════════════════════════════════════════╣
║  Threshold:   p99 < 500ms with 50 VUs                   ║
║  Result:      ${p99 !== "N/A" && parseFloat(p99) < 500 ? "PASS ✓" : "FAIL ✗"}                                        ║
╚══════════════════════════════════════════════════════════╝
`;

  console.log(summary);

  return {
    stdout: summary,
  };
}
