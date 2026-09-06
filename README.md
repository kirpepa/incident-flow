# IncidentFlow

IncidentFlow is an event-driven incident-management backend written in Go. It accepts signed monitoring alerts, correlates an alert storm into one incident, records an audit timeline and executes durable escalation steps. It is a portfolio-scale mini PagerDuty focused on failure semantics rather than UI.

## Why this project exists

A naive alert handler creates duplicate incidents, loses events between the database and broker, and repeats side effects after worker crashes. IncidentFlow makes those failure modes explicit:

- atomic alert ingestion with an idempotency key and transactional outbox;
- NATS JetStream at-least-once delivery with a transactional consumer inbox;
- concurrency-safe correlation with a PostgreSQL advisory transaction lock;
- incident state machine with optimistic version checks;
- escalation claims using `FOR UPDATE SKIP LOCKED`, leases and fencing tokens;
- retry with exponential backoff, terminal failure state and idempotent notification keys;
- tenant-scoped API queries, API-key auth and replay-resistant webhook HMAC;
- Prometheus metrics, structured logs, OpenAPI and reproducible integration/load tests;
- authenticated read-only MCP tools and an optional evidence-linked AI summary.

The project does not claim end-to-end exactly-once delivery. See [consistency and failure semantics](docs/consistency.md).

## Architecture

```text
monitor → API → PostgreSQL outbox → relay → NATS JetStream → correlator
                    PostgreSQL ← scheduler → notification target
                    PostgreSQL ← MCP diagnostics → optional LLM
```

The repository builds three runtime processes: `api`, `worker` and `mcp`. PostgreSQL remains the source of truth. A detailed diagram and request sequence are in [docs/architecture.md](docs/architecture.md).

## Quick start

Requirements: Docker Compose, `curl`, `jq` and `openssl`.

```bash
docker compose up --build -d
./scripts/demo.sh
```

The demo sends 100 concurrent alerts with different idempotency keys and one run-scoped fingerprint. It asserts that they become one incident with 100 occurrences, waits for the first escalation, acknowledges the incident and verifies that ACK cancelled the second step. The run-scoped fingerprint makes the demo repeatable on an existing volume.

Services:

| Endpoint | Purpose |
| --- | --- |
| `http://localhost:8080` | REST API, health and metrics |
| `http://localhost:8090/mcp` | Authenticated Streamable HTTP MCP |
| `http://localhost:9090` | Prometheus |
| `http://localhost:8222` | NATS monitoring |

Default local credentials are intentionally non-production values:

```text
X-API-Key: demo-api-key-change-me
webhook secret: demo-webhook-secret-change-me
```

Copy `.env.example` to `.env` and replace them for any non-local deployment.

## API example

The ingestion signature covers the method, path, timestamp, idempotency key and raw body:

```text
HMAC-SHA256(secret, "POST\n/v1/alerts\n<timestamp>\n<idempotency-key>\n<raw-body>")
```

```bash
body='{"service":"payments","title":"Error rate above SLO","severity":"critical","fingerprint":"payments-errors"}'
timestamp="$(date +%s)"
idempotency_key='monitor-event-42'
signature="$(printf '%s\n%s\n%s\n%s\n%s' 'POST' '/v1/alerts' "$timestamp" "$idempotency_key" "$body" | openssl dgst -sha256 -hmac 'demo-webhook-secret-change-me' -hex | awk '{print $NF}')"

curl -i http://localhost:8080/v1/alerts \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: demo-api-key-change-me' \
  -H "X-IncidentFlow-Timestamp: $timestamp" \
  -H "X-IncidentFlow-Signature: sha256=$signature" \
  -H "Idempotency-Key: $idempotency_key" \
  --data-binary "$body"
```

The complete contract is [api/openapi.yaml](api/openapi.yaml).

## MCP and AI boundary

The MCP server uses the official Go SDK and exposes only read-only tools:

- `list_incidents`;
- `get_incident`;
- `get_incident_timeline`;
- `summarize_incident`.

Requests require the same `X-API-Key`. Without AI configuration, summaries are deterministic and explicitly avoid root-cause claims. An optional Responses-compatible provider can be enabled with `AI_BASE_URL`, `AI_API_KEY` and `AI_MODEL`; incident evidence is then sent to that provider. The request disables response storage, asks for strict structured output and requires timeline event IDs as evidence. The LLM cannot acknowledge, resolve or otherwise mutate incidents.

## Verification

Local verification requires Go 1.25.13+, Node.js and Docker. The tool versions used by CI are pinned.

```bash
make verify
make integration
```

The integration test starts real PostgreSQL and NATS containers. It checks 100 concurrent alert writes, single-incident correlation, request and consumer-inbox replay, tenant isolation, escalation delivery, lease takeover with stale-token fencing and ACK cancellation.

Run a reproducible ingestion profile after starting the stack:

```bash
docker run --rm -i \
  -e BASE_URL=http://host.docker.internal:8080 \
  -v "$PWD/tests/load:/scripts" grafana/k6 run /scripts/alerts.js
```

Do not put latency or throughput numbers in a CV without preserving the test command, environment and output.

## Repository map

```text
cmd/api          REST API and Prometheus endpoint
cmd/worker       outbox relay, correlator and escalation scheduler
cmd/mcp          read-only MCP server and optional AI adapter
cmd/migrate      embedded versioned SQL migrations
cmd/seed         explicit local demo bootstrap
internal/domain  state machine, validation and event contracts
internal/database PostgreSQL transactions and durable claims
internal/messaging NATS JetStream adapter
internal/worker  background processing
tests/integration real dependency failure-path test
tests/load       k6 ingestion profile
```

## Current scope

This is a backend systems project, not a hosted PagerDuty replacement. It deliberately omits a web UI, on-call calendars, arbitrary runbook execution and autonomous AI actions. Those features would add surface area without strengthening the consistency model demonstrated here.
