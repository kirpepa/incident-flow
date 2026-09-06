package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/kirpepa/incident-flow/internal/config"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/messaging"
	"github.com/kirpepa/incident-flow/internal/platform"
	"github.com/kirpepa/incident-flow/internal/ports"
	"github.com/kirpepa/incident-flow/internal/worker"
	"golang.org/x/sync/errgroup"
)

func main() {
	configuration, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}
	logger := platform.NewLogger(configuration.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := database.Open(ctx, configuration.DatabaseURL)
	if err != nil {
		logger.Error("open database", "error", err)
		return
	}
	defer store.Close()
	bus, err := messaging.Open(ctx, configuration.NATSURL, logger)
	if err != nil {
		logger.Error("open event bus", "error", err)
		return
	}
	defer bus.Close()

	clock := ports.SystemClock{}
	relay := worker.NewOutboxRelay(
		store, bus, clock, logger.With("component", "outbox-relay"),
		configuration.OutboxBatchSize, configuration.WorkerLease, configuration.WorkerPollInterval,
	)
	correlator := worker.NewCorrelator(
		store, bus, configuration.CorrelationWindow, logger.With("component", "correlator"),
	)
	scheduler := worker.NewScheduler(
		store, worker.NewHTTPNotifier(), clock, logger.With("component", "scheduler"),
		configuration.EscalationBatchSize, configuration.WorkerLease, configuration.WorkerPollInterval,
	)

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error { return relay.Run(groupContext) })
	group.Go(func() error { return correlator.Run(groupContext) })
	group.Go(func() error { return scheduler.Run(groupContext) })
	logger.Info("workers started")
	if err := group.Wait(); err != nil {
		logger.Error("worker stopped with error", "error", err)
	}
}
