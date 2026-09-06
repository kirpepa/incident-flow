CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    id uuid PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    api_key_hash bytea NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE alerts (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    idempotency_key text NOT NULL,
    external_id text NOT NULL DEFAULT '',
    fingerprint text NOT NULL,
    service text NOT NULL,
    title text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    labels jsonb NOT NULL DEFAULT '{}',
    details jsonb NOT NULL DEFAULT '{}',
    occurred_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
	request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX alerts_fingerprint_received_idx
    ON alerts (tenant_id, fingerprint, received_at DESC);

CREATE TABLE incidents (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    fingerprint text NOT NULL,
    service text NOT NULL,
    title text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    status text NOT NULL CHECK (status IN ('open', 'acknowledged', 'resolved')),
    occurrence_count integer NOT NULL CHECK (occurrence_count > 0),
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    acknowledged_at timestamptz,
    resolved_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX incidents_list_idx
    ON incidents (tenant_id, status, last_seen_at DESC);
CREATE INDEX incidents_correlation_idx
    ON incidents (tenant_id, fingerprint, last_seen_at DESC);

CREATE TABLE incident_timeline (
    id bigserial PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    kind text NOT NULL,
    actor text NOT NULL DEFAULT '',
    data jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX incident_timeline_incident_idx
    ON incident_timeline (tenant_id, incident_id, id);

CREATE TABLE escalation_policies (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    name text NOT NULL,
    is_default boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE UNIQUE INDEX escalation_policy_one_default_idx
    ON escalation_policies (tenant_id) WHERE is_default;

CREATE TABLE escalation_policy_steps (
    policy_id uuid NOT NULL REFERENCES escalation_policies(id) ON DELETE CASCADE,
    step_number integer NOT NULL CHECK (step_number >= 0),
    delay_seconds integer NOT NULL CHECK (delay_seconds >= 0),
    target_url text NOT NULL,
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    PRIMARY KEY (policy_id, step_number)
);

CREATE TABLE incident_escalations (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    step_number integer NOT NULL,
    target_url text NOT NULL,
	incident_title text NOT NULL,
	incident_severity text NOT NULL CHECK (incident_severity IN ('info', 'warning', 'critical')),
    status text NOT NULL CHECK (status IN ('pending', 'running', 'sent', 'cancelled', 'dead')),
    due_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    last_error text NOT NULL DEFAULT '',
    lease_id uuid,
    lease_until timestamptz,
    fencing_token bigint NOT NULL DEFAULT 0,
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (incident_id, step_number)
);

CREATE INDEX incident_escalations_claim_idx
    ON incident_escalations (status, due_at) WHERE status IN ('pending', 'running');
CREATE INDEX incident_escalations_expired_lease_idx
    ON incident_escalations (lease_until) WHERE status = 'running';

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    topic text NOT NULL,
    partition_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    lease_id uuid,
    lease_until timestamptz
);

CREATE INDEX outbox_events_claim_idx
    ON outbox_events (available_at, created_at) WHERE published_at IS NULL;

CREATE TABLE inbox_messages (
    consumer text NOT NULL,
    message_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

CREATE TABLE notification_deliveries (
    id bigserial PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    incident_id uuid NOT NULL,
    payload jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
