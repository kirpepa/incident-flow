import crypto from "k6/crypto";
import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    alert_ingestion: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 100),
      timeUnit: "1s",
      duration: __ENV.DURATION || "30s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

const baseURL = __ENV.BASE_URL || "http://host.docker.internal:8080";
const apiKey = __ENV.DEMO_API_KEY || "demo-api-key-change-me";
const secret = __ENV.DEMO_WEBHOOK_SECRET || "demo-webhook-secret-change-me";

export default function () {
  const timestamp = Math.floor(Date.now() / 1000);
  const body = JSON.stringify({
    external_id: `k6-${__VU}-${__ITER}`,
    fingerprint: `load-${__ENV.FINGERPRINT || "checkout-latency"}`,
    service: "checkout",
    title: "Checkout latency above SLO",
    severity: "warning",
    labels: { source: "k6" },
  });
  const idempotencyKey = `k6-${Date.now()}-${__VU}-${__ITER}`;
  const canonical = `POST\n/v1/alerts\n${timestamp}\n${idempotencyKey}\n${body}`;
  const signature = crypto.hmac("sha256", secret, canonical, "hex");
  const response = http.post(`${baseURL}/v1/alerts`, body, {
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
      "X-IncidentFlow-Timestamp": String(timestamp),
      "X-IncidentFlow-Signature": `sha256=${signature}`,
      "Idempotency-Key": idempotencyKey,
    },
  });
  check(response, { "alert accepted": (result) => result.status === 202 });
}
