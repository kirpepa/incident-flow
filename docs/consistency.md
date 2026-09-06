# Consistency and failure model

IncidentFlow promises at-least-once processing with idempotent internal state transitions. It deliberately does **not** claim end-to-end exactly-once delivery.

| Failure point | Expected behaviour |
| --- | --- |
| API crashes before commit | Neither alert nor outbox event exists; caller retries with the same idempotency key. |
| API crashes after commit | Retry with the same key and payload returns the original alert ID. Reusing the key for another payload returns conflict. |
| NATS is unavailable or its work queue is full | API ingestion continues; the relay retains the event in PostgreSQL and retries with backoff. `DiscardNew` prevents eviction of older unacknowledged events. |
| Relay publishes and crashes before marking the row | Event may be published again. JetStream message ID deduplication reduces immediate duplicates; the consumer inbox is the correctness boundary. |
| Correlator receives a message twice | `(consumer, message_id)` prevents a second occurrence increment. |
| Correlators race on the same fingerprint | A PostgreSQL advisory transaction lock serializes incident creation/update. |
| Scheduler replica dies while processing | Its lease expires and another replica claims the row with a larger fencing token. A lease that expires after the final allowed attempt becomes terminal rather than looping forever. |
| Notification succeeds but completion commit fails | Delivery can be retried with the same snapshotted payload and idempotency key. Providers must honour that key to suppress duplicate external side effects. If the final attempt has uncertain delivery, the item becomes `dead` for operator review. |
| Operator acknowledges an incident | Pending/running escalation rows are cancelled transactionally. A delivery already in flight can still reach the external provider; its fenced completion cannot reactivate the cancelled row. |
| Poison alert event | Invalid envelope IDs, type, version or payload are terminated and logged. Valid events with transient processing failures retry with bounded exponential delay until they succeed. |

## Invariants

1. One `(tenant, idempotency_key)` maps to one request payload; a mismatched reuse is rejected.
2. One `(consumer, message_id)` is applied at most once to PostgreSQL state.
3. An incident transition only moves `open → acknowledged/resolved` or `acknowledged → resolved`.
4. Business reads and writes are tenant-scoped at their authorization boundary; cross-tenant incident IDs never expose another tenant's data.
5. PostgreSQL, not NATS or an LLM, owns incident state.

## Known boundary

The demo tenant secret is supplied through environment variables. A production deployment should use a secret manager and per-tenant encrypted webhook secrets. The built-in notification sink exists only to make failure scenarios reproducible, is disabled by default and is enabled explicitly in the localhost-bound Compose stack.
