package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kirpepa/incident-flow/internal/ai"
	"github.com/kirpepa/incident-flow/internal/config"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/mcpapi"
	"github.com/kirpepa/incident-flow/internal/platform"
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
	summarizer, err := ai.New(configuration.AIBaseURL, configuration.AIAPIKey, configuration.AIModel)
	if err != nil {
		logger.Error("configure summarizer", "error", err)
		return
	}
	mcpHandler := mcpapi.NewHandler(store, summarizer, logger)
	mux := http.NewServeMux()
	mux.Handle("POST /mcp", mcpHandler)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	})
	server := &http.Server{
		Addr:              configuration.MCPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       35 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		logger.Info("MCP server listening", "address", configuration.MCPAddr, "summarizer", summarizer.Name())
		serverError <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("MCP server failed", "error", err)
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Error("graceful MCP shutdown failed", "error", err)
	}
}
