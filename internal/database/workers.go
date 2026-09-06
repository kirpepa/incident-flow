package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/kirpepa/incident-flow/internal/domain"
)

func (s *Store) ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]domain.OutboxEvent, error) {
	leaseID := uuid.New()
	rows, err := s.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL
			  AND available_at <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		), claimed AS (
			UPDATE outbox_events event
			SET lease_id=$2, lease_until=now()+$3::interval
			FROM candidates
			WHERE event.id=candidates.id
			RETURNING event.id, event.tenant_id, event.topic, event.partition_key,
			          event.payload, event.created_at, event.attempts, event.lease_id
		)
		SELECT id, tenant_id, topic, partition_key, payload, created_at, attempts, lease_id
		FROM claimed ORDER BY created_at`, limit, leaseID, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.OutboxEvent, 0, limit)
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(
			&event.ID, &event.TenantID, &event.Topic, &event.Key,
			&event.Payload, &event.CreatedAt, &event.Attempts, &event.LeaseID,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, event domain.OutboxEvent, now time.Time) error {
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox_events SET published_at=$1, lease_id=NULL, lease_until=NULL,
		       last_error=''
		WHERE id=$2 AND lease_id=$3 AND published_at IS NULL`, now.UTC(), event.ID, event.LeaseID)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if result.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	return nil
}

func (s *Store) MarkOutboxFailed(ctx context.Context, event domain.OutboxEvent, cause error, retryAfter time.Duration) error {
	message := cause.Error()
	if len(message) > 2000 {
		message = message[:2000]
	}
	result, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts=attempts+1, last_error=$1, available_at=now()+$2::interval,
		    lease_id=NULL, lease_until=NULL
		WHERE id=$3 AND lease_id=$4 AND published_at IS NULL`,
		message, retryAfter.String(), event.ID, event.LeaseID)
	if err != nil {
		return fmt.Errorf("mark outbox event failed: %w", err)
	}
	if result.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	return nil
}

func (s *Store) ClaimEscalations(ctx context.Context, limit int, lease time.Duration) ([]domain.Escalation, error) {
	leaseID := uuid.New()
	rows, err := s.pool.Query(ctx, `
		WITH expired_candidates AS (
			SELECT escalation.id
			FROM incident_escalations escalation
			JOIN incidents incident ON incident.id=escalation.incident_id
			WHERE incident.status='open'
			  AND escalation.status='running'
			  AND escalation.lease_until < now()
			  AND escalation.attempts >= escalation.max_attempts
			ORDER BY escalation.lease_until
			FOR UPDATE OF escalation SKIP LOCKED
			LIMIT $1
		), exhausted AS (
			UPDATE incident_escalations escalation
			SET status='dead', updated_at=now(), lease_id=NULL, lease_until=NULL,
			    last_error='worker lease expired after final attempt'
			FROM expired_candidates
			WHERE escalation.id=expired_candidates.id
			RETURNING escalation.tenant_id, escalation.incident_id,
			          escalation.step_number, escalation.attempts
		), exhausted_timeline AS (
			INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
			SELECT tenant_id, incident_id, 'escalation_dead', 'scheduler',
			       jsonb_build_object(
			           'step_number', step_number,
			           'attempt', attempts,
			           'error', 'worker lease expired after final attempt'
			       )
			FROM exhausted
			RETURNING id
		), candidates AS (
			SELECT escalation.id FROM incident_escalations escalation
			JOIN incidents incident ON incident.id=escalation.incident_id
			WHERE incident.status='open'
			  AND escalation.attempts < escalation.max_attempts
			  AND (
				(escalation.status='pending' AND escalation.due_at <= now())
				OR (escalation.status='running' AND escalation.lease_until < now())
			)
			ORDER BY escalation.due_at
			FOR UPDATE OF escalation SKIP LOCKED
			LIMIT $1
		), claimed AS (
			UPDATE incident_escalations escalation
			SET status='running', lease_id=$2, lease_until=now()+$3::interval,
			    fencing_token=fencing_token+1, attempts=attempts+1, updated_at=now()
			FROM candidates
			WHERE escalation.id=candidates.id
			RETURNING escalation.*
		)
		SELECT claimed.id, claimed.tenant_id, claimed.incident_id,
		       claimed.step_number, claimed.target_url, claimed.attempts,
		       claimed.max_attempts, claimed.due_at, claimed.fencing_token,
		       claimed.incident_title, incident.status, claimed.incident_severity
		FROM claimed
		JOIN incidents incident ON incident.id=claimed.incident_id
		ORDER BY claimed.due_at`, limit, leaseID, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claim escalations: %w", err)
	}
	defer rows.Close()

	escalations := make([]domain.Escalation, 0, limit)
	for rows.Next() {
		var escalation domain.Escalation
		var severity string
		if err := rows.Scan(
			&escalation.ID, &escalation.TenantID, &escalation.IncidentID,
			&escalation.StepNumber, &escalation.TargetURL, &escalation.Attempts,
			&escalation.MaxAttempts, &escalation.DueAt, &escalation.FencingToken,
			&escalation.IncidentTitle, &escalation.IncidentStatus, &severity,
		); err != nil {
			return nil, fmt.Errorf("scan escalation: %w", err)
		}
		escalation.IncidentSeverity = domain.Severity(severity)
		escalations = append(escalations, escalation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate escalations: %w", err)
	}
	return escalations, nil
}

func (s *Store) CompleteEscalation(ctx context.Context, escalation domain.Escalation, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin escalation completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := tx.Exec(ctx, `
		UPDATE incident_escalations
		SET status='sent', sent_at=$1, updated_at=$1, lease_id=NULL, lease_until=NULL,
		    last_error=''
		WHERE id=$2 AND status='running' AND fencing_token=$3`,
		now.UTC(), escalation.ID, escalation.FencingToken)
	if err != nil {
		return fmt.Errorf("complete escalation: %w", err)
	}
	if result.RowsAffected() != 1 {
		return domain.ErrConflict
	}
	data, _ := json.Marshal(map[string]any{
		"step_number": escalation.StepNumber,
		"attempt":     escalation.Attempts,
	})
	if _, err := tx.Exec(ctx, `
		INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
		VALUES ($1,$2,'escalation_sent','scheduler',$3)`,
		escalation.TenantID, escalation.IncidentID, data); err != nil {
		return fmt.Errorf("append escalation sent event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit escalation completion: %w", err)
	}
	return nil
}

func (s *Store) FailEscalation(
	ctx context.Context,
	escalation domain.Escalation,
	cause error,
	retryAfter time.Duration,
	now time.Time,
) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin escalation failure: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempts, maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT attempts, max_attempts FROM incident_escalations
		WHERE id=$1 AND status='running' AND fencing_token=$2 FOR UPDATE`,
		escalation.ID, escalation.FencingToken).Scan(&attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, domain.ErrConflict
	}
	if err != nil {
		return false, fmt.Errorf("lock failed escalation: %w", err)
	}

	dead := attempts >= maxAttempts
	status := "pending"
	if dead {
		status = "dead"
	}
	message := cause.Error()
	if len(message) > 2000 {
		message = message[:2000]
	}
	if _, err := tx.Exec(ctx, `
		UPDATE incident_escalations
		SET status=$1, due_at=$2, last_error=$3, updated_at=$4,
		    lease_id=NULL, lease_until=NULL
		WHERE id=$5`, status, now.Add(retryAfter).UTC(), message, now.UTC(), escalation.ID); err != nil {
		return false, fmt.Errorf("reschedule failed escalation: %w", err)
	}
	data, _ := json.Marshal(map[string]any{
		"step_number": escalation.StepNumber,
		"attempt":     attempts,
		"error":       message,
		"retry_after": retryAfter.String(),
	})
	kind := "escalation_retry_scheduled"
	if dead {
		kind = "escalation_dead"
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
		VALUES ($1,$2,$3,'scheduler',$4)`,
		escalation.TenantID, escalation.IncidentID, kind, data); err != nil {
		return false, fmt.Errorf("append escalation failure event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit escalation failure: %w", err)
	}
	return dead, nil
}

func (s *Store) RecordNotification(ctx context.Context, notification domain.Notification) (bool, error) {
	payload, err := json.Marshal(notification)
	if err != nil {
		return false, fmt.Errorf("encode notification: %w", err)
	}
	var id int64
	err = s.pool.QueryRow(ctx, `
		INSERT INTO notification_deliveries (idempotency_key, incident_id, payload)
		VALUES ($1,$2,$3)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`, notification.IdempotencyKey, notification.IncidentID, payload).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record notification: %w", err)
	}
	return true, nil
}

func (s *Store) NotificationCount(ctx context.Context, incidentID uuid.UUID) (int, error) {
	var count int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM notification_deliveries WHERE incident_id=$1`, incidentID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count notifications: %w", err)
	}
	return count, nil
}
