package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/kirpepa/incident-flow/internal/ports"
)

type Scheduler struct {
	store     *database.Store
	notifier  ports.Notifier
	clock     ports.Clock
	logger    *slog.Logger
	batchSize int
	lease     time.Duration
	poll      time.Duration
}

func NewScheduler(store *database.Store, notifier ports.Notifier, clock ports.Clock, logger *slog.Logger, batchSize int, lease, poll time.Duration) *Scheduler {
	return &Scheduler{
		store: store, notifier: notifier, clock: clock, logger: logger,
		batchSize: batchSize, lease: lease, poll: poll,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	for {
		if err := s.runBatch(ctx); err != nil && ctx.Err() == nil {
			s.logger.Error("scheduler batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) runBatch(ctx context.Context) error {
	escalations, err := s.store.ClaimEscalations(ctx, s.batchSize, s.lease)
	if err != nil {
		return err
	}

	var waitGroup sync.WaitGroup
	for _, escalation := range escalations {
		escalation := escalation
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			s.process(ctx, escalation)
		}()
	}
	waitGroup.Wait()
	return nil
}

func (s *Scheduler) process(ctx context.Context, escalation domain.Escalation) {
	if escalation.IncidentStatus != string(domain.IncidentOpen) {
		// An ACK/resolve transaction normally cancels this row before it is claimed.
		s.logger.Debug("skipping escalation for inactive incident", "escalation_id", escalation.ID)
		return
	}
	notification := domain.Notification{
		IdempotencyKey: fmt.Sprintf("%s:%d", escalation.IncidentID, escalation.StepNumber),
		IncidentID:     escalation.IncidentID,
		Title:          escalation.IncidentTitle,
		Severity:       escalation.IncidentSeverity,
		StepNumber:     escalation.StepNumber,
	}

	notifyContext, cancel := context.WithTimeout(ctx, 7*time.Second)
	err := s.notifier.Notify(notifyContext, escalation.TargetURL, notification)
	cancel()
	if err == nil {
		if err := s.store.CompleteEscalation(ctx, escalation, s.clock.Now()); err != nil {
			s.logger.Error("notification sent but escalation completion failed", "escalation_id", escalation.ID, "error", err)
			return
		}
		s.logger.Info("escalation delivered", "incident_id", escalation.IncidentID, "step", escalation.StepNumber)
		return
	}

	retryAfter := retryDelay(escalation.Attempts)
	dead, storeErr := s.store.FailEscalation(ctx, escalation, err, retryAfter, s.clock.Now())
	if storeErr != nil {
		s.logger.Error("could not persist escalation failure", "escalation_id", escalation.ID, "error", storeErr)
		return
	}
	s.logger.Warn("escalation delivery failed", "escalation_id", escalation.ID, "dead", dead, "retry_after", retryAfter, "error", err)
}

func retryDelay(attempt int) time.Duration {
	exponent := math.Min(float64(max(attempt-1, 0)), 6)
	base := time.Duration(math.Pow(2, exponent)) * time.Second
	jitter := time.Duration(rand.IntN(500)) * time.Millisecond
	return base + jitter
}
