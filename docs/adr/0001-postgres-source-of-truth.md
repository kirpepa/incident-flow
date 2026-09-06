# ADR 0001: PostgreSQL as the source of truth

- Status: accepted
- Date: 2026-09-06

## Context

Alert ingestion must acknowledge a request without risking a database/broker dual write. Correlation and escalation also need transactional state transitions and deterministic recovery after worker failure.

## Decision

PostgreSQL owns alerts, incidents, the audit timeline, escalation work, inbox rows and outbox rows. NATS JetStream carries versioned domain events but is not authoritative state. The outbox relay publishes committed events; consumers use an inbox row in the same transaction as their business update.

## Consequences

- Broker retries and duplicates are safe for internal state.
- SQL locking and leases provide horizontal worker coordination without a separate distributed-lock service.
- The relay and scheduler add operational components and eventual consistency.
- External notification exactly-once behaviour still depends on provider idempotency support.
