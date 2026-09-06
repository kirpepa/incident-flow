package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/domain"
)

type Publisher interface {
	Publish(ctx context.Context, event domain.OutboxEvent) error
}

type Notifier interface {
	Notify(ctx context.Context, targetURL string, notification domain.Notification) error
}

type IncidentReader interface {
	ListIncidents(ctx context.Context, tenantID uuid.UUID, filter domain.IncidentFilter) ([]domain.Incident, error)
	GetIncident(ctx context.Context, tenantID, incidentID uuid.UUID) (domain.Incident, error)
	Timeline(ctx context.Context, tenantID, incidentID uuid.UUID, limit int) ([]domain.TimelineEvent, error)
}

type Summarizer interface {
	Name() string
	Summarize(ctx context.Context, incident domain.Incident, timeline []domain.TimelineEvent) (domain.IncidentSummary, error)
}

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
