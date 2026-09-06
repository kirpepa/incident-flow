package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

func ParseSeverity(raw string) (Severity, error) {
	severity := Severity(strings.ToLower(strings.TrimSpace(raw)))
	switch severity {
	case SeverityInfo, SeverityWarning, SeverityCritical:
		return severity, nil
	default:
		return "", fmt.Errorf("%w: unsupported severity %q", ErrInvalidInput, raw)
	}
}

func (s Severity) Rank() int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

func MaxSeverity(left, right Severity) Severity {
	if right.Rank() > left.Rank() {
		return right
	}
	return left
}

type AlertInput struct {
	ExternalID  string            `json:"external_id"`
	Fingerprint string            `json:"fingerprint,omitempty"`
	Service     string            `json:"service"`
	Title       string            `json:"title"`
	Severity    Severity          `json:"severity"`
	Labels      map[string]string `json:"labels,omitempty"`
	Details     map[string]any    `json:"details,omitempty"`
	OccurredAt  time.Time         `json:"occurred_at,omitempty"`
}

func (a AlertInput) Validate() error {
	service := strings.TrimSpace(a.Service)
	if service == "" {
		return fmt.Errorf("%w: service is required", ErrInvalidInput)
	}
	if utf8.RuneCountInString(service) > 128 {
		return fmt.Errorf("%w: service must be at most 128 characters", ErrInvalidInput)
	}
	title := strings.TrimSpace(a.Title)
	if title == "" {
		return fmt.Errorf("%w: title is required", ErrInvalidInput)
	}
	if utf8.RuneCountInString(title) > 512 {
		return fmt.Errorf("%w: title must be at most 512 characters", ErrInvalidInput)
	}
	if utf8.RuneCountInString(strings.TrimSpace(a.Fingerprint)) > 256 {
		return fmt.Errorf("%w: fingerprint must be at most 256 characters", ErrInvalidInput)
	}
	if utf8.RuneCountInString(a.ExternalID) > 256 {
		return fmt.Errorf("%w: external_id must be at most 256 characters", ErrInvalidInput)
	}
	if len(a.Labels) > 50 {
		return fmt.Errorf("%w: at most 50 labels are allowed", ErrInvalidInput)
	}
	normalizedKeys := make(map[string]struct{}, len(a.Labels))
	for key, value := range a.Labels {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if normalizedKey == "" || utf8.RuneCountInString(key) > 128 || utf8.RuneCountInString(value) > 512 {
			return fmt.Errorf("%w: label keys must contain 1-128 characters and values at most 512 characters", ErrInvalidInput)
		}
		if _, duplicate := normalizedKeys[normalizedKey]; duplicate {
			return fmt.Errorf("%w: label keys must be unique ignoring case and surrounding whitespace", ErrInvalidInput)
		}
		normalizedKeys[normalizedKey] = struct{}{}
	}
	if a.Severity.Rank() == 0 {
		return fmt.Errorf("%w: severity must be info, warning, or critical", ErrInvalidInput)
	}
	return nil
}

func (a AlertInput) NormalizedFingerprint() string {
	if value := strings.TrimSpace(a.Fingerprint); value != "" {
		return strings.ToLower(value)
	}

	normalizedLabels := make(map[string]string, len(a.Labels))
	for key, value := range a.Labels {
		normalizedLabels[strings.ToLower(strings.TrimSpace(key))] = strings.ToLower(strings.TrimSpace(value))
	}
	canonical, _ := json.Marshal(struct {
		Service string            `json:"service"`
		Title   string            `json:"title"`
		Labels  map[string]string `json:"labels"`
	}{
		Service: strings.ToLower(strings.TrimSpace(a.Service)),
		Title:   strings.ToLower(strings.TrimSpace(a.Title)),
		Labels:  normalizedLabels,
	})

	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:16])
}

type Alert struct {
	ID             uuid.UUID         `json:"id"`
	TenantID       uuid.UUID         `json:"tenant_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	ExternalID     string            `json:"external_id,omitempty"`
	Fingerprint    string            `json:"fingerprint"`
	Service        string            `json:"service"`
	Title          string            `json:"title"`
	Severity       Severity          `json:"severity"`
	Labels         map[string]string `json:"labels,omitempty"`
	Details        map[string]any    `json:"details,omitempty"`
	OccurredAt     time.Time         `json:"occurred_at"`
	ReceivedAt     time.Time         `json:"received_at"`
}

type AlertReceipt struct {
	AlertID   uuid.UUID `json:"alert_id"`
	Duplicate bool      `json:"duplicate"`
}
