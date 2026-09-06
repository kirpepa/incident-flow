# Architecture

IncidentFlow is intentionally a modular system with three deployable Go binaries, not a collection of thin demo microservices.

```mermaid
flowchart LR
    Monitor[Monitoring system] -->|HMAC webhook| API[API]
    API -->|alert + event in one transaction| PG[(PostgreSQL)]
    Relay[Outbox relay] -->|claim unpublished rows| PG
    Relay -->|alerts.received.v1| NATS[(NATS JetStream)]
    NATS --> Correlator[Correlator]
    Correlator -->|advisory lock + inbox| PG
    Scheduler[Escalation scheduler] -->|SKIP LOCKED + lease| PG
    Scheduler -->|idempotency key| Target[Notification target]
    MCP[MCP diagnostics] --> PG
    MCP -. optional .-> LLM[Responses-compatible model]
    Prometheus[Prometheus] -->|scrape| API
```

## Components

- `cmd/api` authenticates tenants, validates replay-resistant HMAC signatures, commits alerts and outbox events atomically, exposes incident state transitions and Prometheus metrics.
- `cmd/worker` runs the outbox relay, JetStream correlator and durable escalation scheduler. Multiple replicas coordinate through PostgreSQL locks and leases.
- `cmd/mcp` exposes four authenticated, read-only diagnostic tools. AI summarization is optional and outside the critical state-transition path.
- `cmd/migrate` only applies embedded, versioned SQL migrations. `cmd/seed` explicitly creates or refreshes local demo data.

PostgreSQL is the source of truth. The API writes only to PostgreSQL, so a broker outage does not stop ingestion. JetStream transports events with at-least-once delivery; duplicating a broker message must not corrupt business state.

## Alert lifecycle

```mermaid
sequenceDiagram
    participant M as Monitor
    participant A as API
    participant P as PostgreSQL
    participant R as Outbox relay
    participant N as JetStream
    participant C as Correlator

    M->>A: POST /v1/alerts + HMAC + idempotency key
    A->>P: BEGIN
    A->>P: INSERT alert
    A->>P: INSERT alert.received outbox event
    A->>P: COMMIT
    A-->>M: 202 Accepted
    R->>P: claim with SKIP LOCKED and lease
    R->>N: publish with Nats-Msg-Id
    R->>P: mark published
    N->>C: at-least-once delivery
    C->>P: inbox insert + advisory lock
    C->>P: create/update incident and timeline
    C-->>N: double ACK
```

The advisory transaction lock is derived from `(tenant_id, fingerprint)`. It serializes competing correlators for the same incident while allowing unrelated services to progress concurrently.

## Package boundaries

- `internal/domain`: validation, state machine and event contracts; no infrastructure imports.
- `internal/database`: PostgreSQL transactions, tenant isolation, inbox/outbox and worker claims.
- `internal/messaging`: JetStream topology, publishing and consumer acknowledgement.
- `internal/worker`: relay, correlator, scheduler and notification adapter.
- `internal/httpapi`: transport validation, authentication, error mapping and metrics.
- `internal/mcpapi` and `internal/ai`: read-only tools and replaceable summarization providers.
