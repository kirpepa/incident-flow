package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/kirpepa/incident-flow/internal/ports"
)

type OutboxRelay struct {
	store     *database.Store
	publisher ports.Publisher
	clock     ports.Clock
	logger    *slog.Logger
	batchSize int
	lease     time.Duration
	poll      time.Duration
}

func NewOutboxRelay(store *database.Store, publisher ports.Publisher, clock ports.Clock, logger *slog.Logger, batchSize int, lease, poll time.Duration) *OutboxRelay {
	return &OutboxRelay{
		store: store, publisher: publisher, clock: clock, logger: logger,
		batchSize: batchSize, lease: lease, poll: poll,
	}
}

func (r *OutboxRelay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.poll)
	defer ticker.Stop()
	for {
		if err := r.runBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("outbox relay batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *OutboxRelay) runBatch(ctx context.Context) error {
	events, err := r.store.ClaimOutbox(ctx, r.batchSize, r.lease)
	if err != nil {
		return err
	}
	var waitGroup sync.WaitGroup
	for _, event := range events {
		event := event
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			r.publish(ctx, event)
		}()
	}
	waitGroup.Wait()
	return nil
}

func (r *OutboxRelay) publish(ctx context.Context, event domain.OutboxEvent) {
	publishContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := r.publisher.Publish(publishContext, event)
	cancel()
	if err != nil {
		retryAfter := retryDelay(event.Attempts + 1)
		r.logger.Warn("outbox publish failed", "event_id", event.ID, "retry_after", retryAfter, "error", err)
		if markErr := r.store.MarkOutboxFailed(ctx, event, err, retryAfter); markErr != nil {
			r.logger.Error("could not release failed outbox event", "event_id", event.ID, "error", markErr)
		}
		return
	}
	if err := r.store.MarkOutboxPublished(ctx, event, r.clock.Now()); err != nil {
		r.logger.Error("event published but not marked; safe duplicate is expected", "event_id", event.ID, "error", err)
	}
}
