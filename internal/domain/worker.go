package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	ID        uuid.UUID       `json:"id"`
	TenantID  uuid.UUID       `json:"tenant_id"`
	Topic     string          `json:"topic"`
	Key       string          `json:"key"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	Attempts  int             `json:"attempts"`
	LeaseID   uuid.UUID       `json:"-"`
}

type Escalation struct {
	ID               uuid.UUID `json:"id"`
	TenantID         uuid.UUID `json:"tenant_id"`
	IncidentID       uuid.UUID `json:"incident_id"`
	StepNumber       int       `json:"step_number"`
	TargetURL        string    `json:"target_url"`
	Attempts         int       `json:"attempts"`
	MaxAttempts      int       `json:"max_attempts"`
	DueAt            time.Time `json:"due_at"`
	FencingToken     int64     `json:"fencing_token"`
	IncidentTitle    string    `json:"incident_title"`
	IncidentStatus   string    `json:"incident_status"`
	IncidentSeverity Severity  `json:"incident_severity"`
}

type Notification struct {
	IdempotencyKey string         `json:"idempotency_key"`
	IncidentID     uuid.UUID      `json:"incident_id"`
	Title          string         `json:"title"`
	Severity       Severity       `json:"severity"`
	StepNumber     int            `json:"step_number"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type IncidentSummary struct {
	IncidentID uuid.UUID `json:"incident_id"`
	Summary    string    `json:"summary"`
	Hypotheses []string  `json:"hypotheses"`
	NextSteps  []string  `json:"next_steps"`
	Evidence   []int64   `json:"evidence_event_ids"`
	Provider   string    `json:"provider"`
}
