package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
)

type AlertConsumer interface {
	ConsumeAlerts(context.Context, func(context.Context, domain.EventEnvelope) error) error
}

type Correlator struct {
	store  *database.Store
	bus    AlertConsumer
	window time.Duration
	logger *slog.Logger
}

func NewCorrelator(store *database.Store, bus AlertConsumer, window time.Duration, logger *slog.Logger) *Correlator {
	return &Correlator{store: store, bus: bus, window: window, logger: logger}
}

func (c *Correlator) Run(ctx context.Context) error {
	return c.bus.ConsumeAlerts(ctx, c.handle)
}

func (c *Correlator) handle(ctx context.Context, envelope domain.EventEnvelope) error {
	data, err := envelope.DecodeAlertReceived()
	if err != nil {
		return fmt.Errorf("decode alert received event: %w", err)
	}
	incident, duplicate, err := c.store.CorrelateAlert(
		ctx, "incident-correlator-v1", envelope.ID, envelope.TenantID, data.AlertID, c.window,
	)
	if err != nil {
		return err
	}
	if duplicate {
		c.logger.Debug("ignored duplicate alert event", "event_id", envelope.ID)
		return nil
	}
	c.logger.Info("alert correlated", "event_id", envelope.ID, "incident_id", incident.ID, "occurrences", incident.OccurrenceCount)
	return nil
}
