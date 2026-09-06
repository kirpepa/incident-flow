package database

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/kirpepa/incident-flow/internal/domain"
)

func (s *Store) IngestAlert(
	ctx context.Context,
	tenantID uuid.UUID,
	idempotencyKey string,
	input domain.AlertInput,
	now time.Time,
) (domain.AlertReceipt, error) {
	input.ExternalID = strings.TrimSpace(input.ExternalID)
	input.Service = strings.TrimSpace(input.Service)
	input.Title = strings.TrimSpace(input.Title)
	if err := input.Validate(); err != nil {
		return domain.AlertReceipt{}, err
	}
	if idempotencyKey == "" {
		return domain.AlertReceipt{}, fmt.Errorf("%w: Idempotency-Key header is required", domain.ErrInvalidInput)
	}
	requestPayload, err := json.Marshal(input)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("encode alert request: %w", err)
	}
	requestHash := sha256.Sum256(requestPayload)
	if input.OccurredAt.IsZero() {
		input.OccurredAt = now
	}
	labels, err := json.Marshal(input.Labels)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("encode labels: %w", err)
	}
	details, err := json.Marshal(input.Details)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("encode details: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("begin alert ingestion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	alertID := uuid.New()
	err = tx.QueryRow(ctx, `
		INSERT INTO alerts (
			id, tenant_id, idempotency_key, external_id, fingerprint, service,
			title, severity, labels, details, occurred_at, received_at, request_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
		RETURNING id`,
		alertID, tenantID, idempotencyKey, input.ExternalID, input.NormalizedFingerprint(),
		input.Service, input.Title, string(input.Severity), labels, details,
		input.OccurredAt.UTC(), now.UTC(), requestHash[:],
	).Scan(&alertID)
	if errors.Is(err, pgx.ErrNoRows) {
		var storedHash []byte
		if err := tx.QueryRow(ctx, `
			SELECT id, request_hash FROM alerts WHERE tenant_id=$1 AND idempotency_key=$2`,
			tenantID, idempotencyKey,
		).Scan(&alertID, &storedHash); err != nil {
			return domain.AlertReceipt{}, fmt.Errorf("read duplicate alert: %w", err)
		}
		if subtle.ConstantTimeCompare(storedHash, requestHash[:]) != 1 {
			return domain.AlertReceipt{}, fmt.Errorf("%w: Idempotency-Key was already used with a different alert", domain.ErrConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.AlertReceipt{}, fmt.Errorf("commit duplicate alert lookup: %w", err)
		}
		return domain.AlertReceipt{AlertID: alertID, Duplicate: true}, nil
	}
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("insert alert: %w", err)
	}

	envelope, err := domain.NewAlertReceivedEnvelope(tenantID, alertID, now)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("create alert event: %w", err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("encode alert event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (id, tenant_id, topic, partition_key, payload)
		VALUES ($1,$2,$3,$4,$5)`, envelope.ID, tenantID, envelope.Type, input.NormalizedFingerprint(), payload); err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("insert alert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.AlertReceipt{}, fmt.Errorf("commit alert ingestion: %w", err)
	}
	return domain.AlertReceipt{AlertID: alertID}, nil
}

func getAlert(ctx context.Context, tx pgx.Tx, tenantID, alertID uuid.UUID) (domain.Alert, error) {
	var alert domain.Alert
	var severity string
	var labels, details []byte
	err := tx.QueryRow(ctx, `
		SELECT id, tenant_id, idempotency_key, external_id, fingerprint, service,
		       title, severity, labels, details, occurred_at, received_at
		FROM alerts WHERE tenant_id=$1 AND id=$2`, tenantID, alertID,
	).Scan(
		&alert.ID, &alert.TenantID, &alert.IdempotencyKey, &alert.ExternalID,
		&alert.Fingerprint, &alert.Service, &alert.Title, &severity, &labels,
		&details, &alert.OccurredAt, &alert.ReceivedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Alert{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Alert{}, fmt.Errorf("get alert: %w", err)
	}
	alert.Severity = domain.Severity(severity)
	if err := json.Unmarshal(labels, &alert.Labels); err != nil {
		return domain.Alert{}, fmt.Errorf("decode alert labels: %w", err)
	}
	if err := json.Unmarshal(details, &alert.Details); err != nil {
		return domain.Alert{}, fmt.Errorf("decode alert details: %w", err)
	}
	return alert, nil
}
