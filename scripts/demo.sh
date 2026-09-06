#!/usr/bin/env sh
set -eu

base_url="${BASE_URL:-http://localhost:8080}"
api_key="${DEMO_API_KEY:-demo-api-key-change-me}"
secret="${DEMO_WEBHOOK_SECRET:-demo-webhook-secret-change-me}"
alert_count="${ALERT_COUNT:-100}"
run_id="${DEMO_RUN_ID:-$(date +%s)-$$}"
fingerprint="checkout-latency-$run_id"

attempt=0
until curl --fail --silent "$base_url/readyz" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 40 ]; then
    echo "IncidentFlow did not become ready" >&2
    exit 1
  fi
  sleep 1
done

body="$(printf '{\"external_id\":\"demo-monitor\",\"fingerprint\":\"%s\",\"service\":\"checkout\",\"title\":\"Checkout latency above SLO\",\"severity\":\"critical\",\"labels\":{\"region\":\"eu-1\"},\"details\":{\"p95_ms\":940}}' "$fingerprint")"
timestamp="$(date +%s)"

send_alert() {
  index="$1"
  idempotency_key="demo-$run_id-$index"
  digest="$(printf '%s\n%s\n%s\n%s\n%s' 'POST' '/v1/alerts' "$timestamp" "$idempotency_key" "$body" | openssl dgst -sha256 -hmac "$secret" -hex | awk '{print $NF}')"
  curl --fail --silent --show-error \
    -X POST "$base_url/v1/alerts" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $api_key" \
    -H "X-IncidentFlow-Timestamp: $timestamp" \
    -H "X-IncidentFlow-Signature: sha256=$digest" \
    -H "Idempotency-Key: $idempotency_key" \
    --data-binary "$body" >/dev/null
}

index=1
while [ "$index" -le "$alert_count" ]; do
  send_alert "$index" &
  index=$((index + 1))
done
wait

attempt=0
while :; do
  incidents="$(curl --fail --silent -H "X-API-Key: $api_key" "$base_url/v1/incidents?status=open&limit=10")"
  matching="$(printf '%s' "$incidents" | jq --arg fingerprint "$fingerprint" '[.items[] | select(.fingerprint == $fingerprint)] | length')"
  occurrences="$(printf '%s' "$incidents" | jq --arg fingerprint "$fingerprint" '[.items[] | select(.fingerprint == $fingerprint)][0].occurrence_count // 0')"
  if [ "$matching" -eq 1 ] && [ "$occurrences" -ge "$alert_count" ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 100 ]; then
    echo "Correlation invariant failed: matching=$matching occurrences=$occurrences" >&2
    exit 1
  fi
  sleep 0.2
done

incident_id="$(printf '%s' "$incidents" | jq -r --arg fingerprint "$fingerprint" '[.items[] | select(.fingerprint == $fingerprint)][0].id')"
version="$(printf '%s' "$incidents" | jq -r --arg fingerprint "$fingerprint" '[.items[] | select(.fingerprint == $fingerprint)][0].version')"

attempt=0
while :; do
  timeline="$(curl --fail --silent -H "X-API-Key: $api_key" "$base_url/v1/incidents/$incident_id/timeline")"
  sent="$(printf '%s' "$timeline" | jq '[.items[] | select(.kind == "escalation_sent")] | length')"
  if [ "$sent" -ge 1 ]; then
    break
  fi
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 50 ]; then
    echo "First escalation was not delivered" >&2
    exit 1
  fi
  sleep 0.2
done
if [ "$sent" -ne 1 ]; then
  echo "Expected exactly one delivery before ACK, got $sent" >&2
  exit 1
fi

curl --fail --silent --show-error \
  -X POST "$base_url/v1/incidents/$incident_id/ack" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $api_key" \
  --data-binary "{\"actor\":\"demo-operator\",\"note\":\"Investigating\",\"expected_version\":$version}" >/dev/null

sleep 11
timeline="$(curl --fail --silent -H "X-API-Key: $api_key" "$base_url/v1/incidents/$incident_id/timeline")"
sent="$(printf '%s' "$timeline" | jq '[.items[] | select(.kind == "escalation_sent")] | length')"
if [ "$sent" -ne 1 ]; then
  echo "ACK cancellation invariant failed: expected 1 delivery, got $sent" >&2
  exit 1
fi

printf 'PASS incident=%s alerts=%s correlated=%s escalation_deliveries=%s\n' "$incident_id" "$alert_count" "$occurrences" "$sent"
