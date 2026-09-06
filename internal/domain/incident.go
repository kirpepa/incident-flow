package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type IncidentStatus string

const (
	IncidentOpen         IncidentStatus = "open"
	IncidentAcknowledged IncidentStatus = "acknowledged"
	IncidentResolved     IncidentStatus = "resolved"
)

type Incident struct {
	ID              uuid.UUID      `json:"id"`
	TenantID        uuid.UUID      `json:"tenant_id,omitempty"`
	Fingerprint     string         `json:"fingerprint"`
	Service         string         `json:"service"`
	Title           string         `json:"title"`
	Severity        Severity       `json:"severity"`
	Status          IncidentStatus `json:"status"`
	OccurrenceCount int            `json:"occurrence_count"`
	FirstSeenAt     time.Time      `json:"first_seen_at"`
	LastSeenAt      time.Time      `json:"last_seen_at"`
	AcknowledgedAt  *time.Time     `json:"acknowledged_at,omitempty"`
	ResolvedAt      *time.Time     `json:"resolved_at,omitempty"`
	Version         int64          `json:"version"`
}

func (i Incident) CanTransition(to IncidentStatus) bool {
	switch i.Status {
	case IncidentOpen:
		return to == IncidentAcknowledged || to == IncidentResolved
	case IncidentAcknowledged:
		return to == IncidentResolved
	case IncidentResolved:
		return false
	default:
		return false
	}
}

func ParseIncidentStatus(raw string) (IncidentStatus, error) {
	status := IncidentStatus(strings.ToLower(strings.TrimSpace(raw)))
	switch status {
	case IncidentOpen, IncidentAcknowledged, IncidentResolved:
		return status, nil
	default:
		return "", fmt.Errorf("%w: unsupported incident status %q", ErrInvalidInput, raw)
	}
}

type TimelineEvent struct {
	ID         int64          `json:"id"`
	IncidentID uuid.UUID      `json:"incident_id"`
	Kind       string         `json:"kind"`
	Actor      string         `json:"actor,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
}

type IncidentFilter struct {
	Status IncidentStatus
	Limit  int
	Before *time.Time
}

type TransitionCommand struct {
	IncidentID uuid.UUID
	To         IncidentStatus
	Actor      string
	Note       string
	Expected   *int64
}
