package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kirpepa/incident-flow/internal/config"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/httpapi"
	"github.com/kirpepa/incident-flow/internal/platform"
	"github.com/kirpepa/incident-flow/internal/ports"
	"github.com/prometheus/client_golang/prometheus"
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
	handler := httpapi.NewServer(
		store,
		ports.SystemClock{},
		logger,
		prometheus.NewRegistry(),
		configuration.WebhookSecret,
		configuration.RequireWebhookHMAC,
		configuration.EnableDemoSink,
	)
	server := &http.Server{
		Addr:              configuration.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info("API listening", "address", configuration.HTTPAddr)
		serverError <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("API shutdown requested")
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("API server failed", "error", err)
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Error("graceful API shutdown failed", "error", err)
	}
}
