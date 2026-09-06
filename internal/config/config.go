package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr            string
	MCPAddr             string
	DatabaseURL         string
	NATSURL             string
	TenantSlug          string
	TenantName          string
	APIKey              string
	WebhookSecret       string
	NotificationTarget  string
	CorrelationWindow   time.Duration
	WorkerPollInterval  time.Duration
	WorkerLease         time.Duration
	OutboxBatchSize     int
	EscalationBatchSize int
	LogLevel            slog.Level
	AIBaseURL           string
	AIAPIKey            string
	AIModel             string
	RequireWebhookHMAC  bool
	EnableDemoSink      bool
}

func Load() (Config, error) {
	correlationWindow, err := durationEnv("CORRELATION_WINDOW", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := durationEnv("WORKER_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return Config{}, err
	}
	lease, err := durationEnv("WORKER_LEASE", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	logLevel, err := parseLogLevel(env("LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}

	config := Config{
		HTTPAddr:            env("HTTP_ADDR", ":8080"),
		MCPAddr:             env("MCP_ADDR", ":8090"),
		DatabaseURL:         env("DATABASE_URL", "postgres://incidentflow:incidentflow@localhost:5432/incidentflow?sslmode=disable"),
		NATSURL:             env("NATS_URL", "nats://localhost:4222"),
		TenantSlug:          env("DEMO_TENANT_SLUG", "demo"),
		TenantName:          env("DEMO_TENANT_NAME", "Demo Team"),
		APIKey:              env("DEMO_API_KEY", "demo-api-key-change-me"),
		WebhookSecret:       env("DEMO_WEBHOOK_SECRET", "demo-webhook-secret-change-me"),
		NotificationTarget:  env("NOTIFICATION_TARGET", "http://localhost:8080/internal/demo/notifications"),
		CorrelationWindow:   correlationWindow,
		WorkerPollInterval:  pollInterval,
		WorkerLease:         lease,
		OutboxBatchSize:     intEnv("OUTBOX_BATCH_SIZE", 50),
		EscalationBatchSize: intEnv("ESCALATION_BATCH_SIZE", 20),
		LogLevel:            logLevel,
		AIBaseURL:           strings.TrimRight(os.Getenv("AI_BASE_URL"), "/"),
		AIAPIKey:            os.Getenv("AI_API_KEY"),
		AIModel:             os.Getenv("AI_MODEL"),
		RequireWebhookHMAC:  boolEnv("REQUIRE_WEBHOOK_HMAC", true),
		EnableDemoSink:      boolEnv("ENABLE_DEMO_SINK", false),
	}

	if config.APIKey == "" || config.WebhookSecret == "" {
		return Config{}, fmt.Errorf("DEMO_API_KEY and DEMO_WEBHOOK_SECRET must not be empty")
	}
	if config.OutboxBatchSize < 1 || config.OutboxBatchSize > 500 || config.EscalationBatchSize < 1 || config.EscalationBatchSize > 500 {
		return Config{}, fmt.Errorf("worker batch sizes must be between 1 and 500")
	}
	if config.WorkerLease < 10*time.Second {
		return Config{}, fmt.Errorf("WORKER_LEASE must be at least 10s")
	}
	return config, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return parsed, nil
}

func intEnv(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return 0, fmt.Errorf("parse LOG_LEVEL: %w", err)
	}
	return level, nil
}
