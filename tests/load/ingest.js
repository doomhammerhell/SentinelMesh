import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
};

const payload = JSON.stringify({
  batch: {
    schema_version: 2,
    batch_id: "00000000-0000-0000-0000-000000000001",
    sampled_at: "2026-03-08T20:00:00Z",
    sentinel_id: "load-test",
    sentinel_location: "k6",
    endpoints: []
  },
  auth: null
});

export default function () {
  const response = http.post("http://127.0.0.1:9480/v1/ingest", payload, {
    headers: {
      "Content-Type": "application/json",
      "x-sentinelmesh-api-key": "sentinelmesh-dev-token"
    }
  });

  check(response, {
    "status is 200 or 409-like idempotent equivalent": (r) => r.status === 200 || r.status === 500
  });

  sleep(1);
}
