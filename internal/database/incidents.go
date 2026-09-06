package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/kirpepa/incident-flow/internal/domain"
)

type rowScanner interface {
	Scan(dest ...any) error
}

func scanIncident(row rowScanner) (domain.Incident, error) {
	var incident domain.Incident
	var severity, status string
	var acknowledgedAt, resolvedAt pgtype.Timestamptz
	err := row.Scan(
		&incident.ID, &incident.TenantID, &incident.Fingerprint, &incident.Service,
		&incident.Title, &severity, &status, &incident.OccurrenceCount,
		&incident.FirstSeenAt, &incident.LastSeenAt, &acknowledgedAt,
		&resolvedAt, &incident.Version,
	)
	if err != nil {
		return domain.Incident{}, err
	}
	incident.Severity = domain.Severity(severity)
	incident.Status = domain.IncidentStatus(status)
	if acknowledgedAt.Valid {
		value := acknowledgedAt.Time
		incident.AcknowledgedAt = &value
	}
	if resolvedAt.Valid {
		value := resolvedAt.Time
		incident.ResolvedAt = &value
	}
	return incident, nil
}

const incidentColumns = `
	id, tenant_id, fingerprint, service, title, severity, status,
	occurrence_count, first_seen_at, last_seen_at, acknowledged_at,
	resolved_at, version`

func (s *Store) CorrelateAlert(
	ctx context.Context,
	consumer string,
	messageID, tenantID, alertID uuid.UUID,
	window time.Duration,
) (domain.Incident, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Incident{}, false, fmt.Errorf("begin alert correlation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var inserted int
	err = tx.QueryRow(ctx, `
		INSERT INTO inbox_messages (consumer, message_id) VALUES ($1,$2)
		ON CONFLICT DO NOTHING RETURNING 1`, consumer, messageID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return domain.Incident{}, true, fmt.Errorf("commit duplicate inbox message: %w", err)
		}
		return domain.Incident{}, true, nil
	}
	if err != nil {
		return domain.Incident{}, false, fmt.Errorf("record inbox message: %w", err)
	}

	alert, err := getAlert(ctx, tx, tenantID, alertID)
	if err != nil {
		return domain.Incident{}, false, err
	}
	lockKey := tenantID.String() + ":" + alert.Fingerprint
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", lockKey); err != nil {
		return domain.Incident{}, false, fmt.Errorf("lock correlation key: %w", err)
	}

	incident, err := scanIncident(tx.QueryRow(ctx, `SELECT `+incidentColumns+`
		FROM incidents
		WHERE tenant_id=$1 AND fingerprint=$2 AND status <> 'resolved'
		  AND last_seen_at >= $3
		  AND first_seen_at <= $4
		ORDER BY last_seen_at DESC LIMIT 1 FOR UPDATE`,
		tenantID, alert.Fingerprint, alert.ReceivedAt.Add(-window), alert.ReceivedAt.Add(window),
	))
	created := false
	if errors.Is(err, pgx.ErrNoRows) {
		created = true
		incident = domain.Incident{
			ID:              uuid.New(),
			TenantID:        tenantID,
			Fingerprint:     alert.Fingerprint,
			Service:         alert.Service,
			Title:           alert.Title,
			Severity:        alert.Severity,
			Status:          domain.IncidentOpen,
			OccurrenceCount: 1,
			FirstSeenAt:     alert.ReceivedAt,
			LastSeenAt:      alert.ReceivedAt,
			Version:         1,
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO incidents (
				id, tenant_id, fingerprint, service, title, severity, status,
				occurrence_count, first_seen_at, last_seen_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			incident.ID, incident.TenantID, incident.Fingerprint, incident.Service,
			incident.Title, string(incident.Severity), string(incident.Status),
			incident.OccurrenceCount, incident.FirstSeenAt, incident.LastSeenAt, incident.Version,
		); err != nil {
			return domain.Incident{}, false, fmt.Errorf("create incident: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO incident_escalations (
				id, tenant_id, incident_id, step_number, target_url, status,
				due_at, max_attempts, incident_title, incident_severity
			)
			SELECT gen_random_uuid(), $1, $2, steps.step_number, steps.target_url,
			       'pending', $3::timestamptz + make_interval(secs => steps.delay_seconds),
			       steps.max_attempts, $4, $5
			FROM escalation_policies policy
			JOIN escalation_policy_steps steps ON steps.policy_id=policy.id
			WHERE policy.tenant_id=$1 AND policy.is_default`,
			tenantID, incident.ID, alert.ReceivedAt, alert.Title, string(alert.Severity),
		); err != nil {
			return domain.Incident{}, false, fmt.Errorf("schedule incident escalations: %w", err)
		}
	} else if err != nil {
		return domain.Incident{}, false, fmt.Errorf("find correlated incident: %w", err)
	} else {
		incident.Severity = domain.MaxSeverity(incident.Severity, alert.Severity)
		incident.OccurrenceCount++
		if alert.ReceivedAt.Before(incident.FirstSeenAt) {
			incident.FirstSeenAt = alert.ReceivedAt
		}
		if alert.ReceivedAt.After(incident.LastSeenAt) {
			incident.LastSeenAt = alert.ReceivedAt
		}
		incident.Version++
		result, err := tx.Exec(ctx, `
			UPDATE incidents SET severity=$1, occurrence_count=$2, first_seen_at=$3,
			last_seen_at=$4, version=$5, updated_at=now()
			WHERE id=$6 AND tenant_id=$7`,
			string(incident.Severity), incident.OccurrenceCount, incident.FirstSeenAt,
			incident.LastSeenAt, incident.Version, incident.ID, tenantID,
		)
		if err != nil {
			return domain.Incident{}, false, fmt.Errorf("update correlated incident: %w", err)
		}
		if result.RowsAffected() != 1 {
			return domain.Incident{}, false, domain.ErrConflict
		}
	}

	timelineKind := "alert_correlated"
	if created {
		timelineKind = "incident_opened"
	}
	data, _ := json.Marshal(map[string]any{
		"alert_id":         alert.ID,
		"external_id":      alert.ExternalID,
		"occurrence_count": incident.OccurrenceCount,
	})
	if _, err := tx.Exec(ctx, `
		INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
		VALUES ($1,$2,$3,'correlator',$4)`, tenantID, incident.ID, timelineKind, data); err != nil {
		return domain.Incident{}, false, fmt.Errorf("append incident timeline: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Incident{}, false, fmt.Errorf("commit alert correlation: %w", err)
	}
	return incident, false, nil
}

func (s *Store) GetIncident(ctx context.Context, tenantID, incidentID uuid.UUID) (domain.Incident, error) {
	incident, err := scanIncident(s.pool.QueryRow(ctx, `SELECT `+incidentColumns+`
		FROM incidents WHERE tenant_id=$1 AND id=$2`, tenantID, incidentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Incident{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Incident{}, fmt.Errorf("get incident: %w", err)
	}
	return incident, nil
}

func (s *Store) ListIncidents(ctx context.Context, tenantID uuid.UUID, filter domain.IncidentFilter) ([]domain.Incident, error) {
	limit := filter.Limit
	if limit < 1 || limit > 100 {
		limit = 50
	}
	before := time.Now().UTC().Add(time.Second)
	if filter.Before != nil {
		before = *filter.Before
	}

	query := `SELECT ` + incidentColumns + ` FROM incidents
		WHERE tenant_id=$1 AND last_seen_at < $2`
	args := []any{tenantID, before}
	if filter.Status != "" {
		query += ` AND status=$3 ORDER BY last_seen_at DESC LIMIT $4`
		args = append(args, string(filter.Status), limit)
	} else {
		query += ` ORDER BY last_seen_at DESC LIMIT $3`
		args = append(args, limit)
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	incidents := make([]domain.Incident, 0, limit)
	for rows.Next() {
		incident, err := scanIncident(rows)
		if err != nil {
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		incidents = append(incidents, incident)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incidents: %w", err)
	}
	return incidents, nil
}

func (s *Store) Timeline(ctx context.Context, tenantID, incidentID uuid.UUID, limit int) ([]domain.TimelineEvent, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, incident_id, kind, actor, data, created_at
		FROM (
			SELECT id, incident_id, kind, actor, data, created_at
			FROM incident_timeline
			WHERE tenant_id=$1 AND incident_id=$2
			ORDER BY id DESC LIMIT $3
		) recent
		ORDER BY id ASC`, tenantID, incidentID, limit)
	if err != nil {
		return nil, fmt.Errorf("list incident timeline: %w", err)
	}
	defer rows.Close()

	timeline := make([]domain.TimelineEvent, 0, limit)
	for rows.Next() {
		var event domain.TimelineEvent
		var data []byte
		if err := rows.Scan(&event.ID, &event.IncidentID, &event.Kind, &event.Actor, &data, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan timeline event: %w", err)
		}
		if err := json.Unmarshal(data, &event.Data); err != nil {
			return nil, fmt.Errorf("decode timeline event: %w", err)
		}
		timeline = append(timeline, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate timeline: %w", err)
	}
	return timeline, nil
}

func (s *Store) TransitionIncident(ctx context.Context, tenantID uuid.UUID, command domain.TransitionCommand, now time.Time) (domain.Incident, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Incident{}, fmt.Errorf("begin incident transition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	incident, err := scanIncident(tx.QueryRow(ctx, `SELECT `+incidentColumns+`
		FROM incidents WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, command.IncidentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Incident{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Incident{}, fmt.Errorf("lock incident: %w", err)
	}
	if command.Expected != nil && incident.Version != *command.Expected {
		return domain.Incident{}, domain.ErrConflict
	}
	if !incident.CanTransition(command.To) {
		return domain.Incident{}, domain.ErrInvalidTransition
	}

	incident.Status = command.To
	incident.Version++
	if command.To == domain.IncidentAcknowledged {
		value := now.UTC()
		incident.AcknowledgedAt = &value
	} else if command.To == domain.IncidentResolved {
		value := now.UTC()
		incident.ResolvedAt = &value
	}
	if _, err := tx.Exec(ctx, `
		UPDATE incidents SET status=$1, acknowledged_at=$2, resolved_at=$3,
		       version=$4, updated_at=$5 WHERE tenant_id=$6 AND id=$7`,
		string(incident.Status), incident.AcknowledgedAt, incident.ResolvedAt,
		incident.Version, now.UTC(), tenantID, incident.ID,
	); err != nil {
		return domain.Incident{}, fmt.Errorf("update incident state: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE incident_escalations SET status='cancelled', updated_at=$1,
		       lease_id=NULL, lease_until=NULL
		WHERE incident_id=$2 AND status IN ('pending','running')`, now.UTC(), incident.ID); err != nil {
		return domain.Incident{}, fmt.Errorf("cancel incident escalations: %w", err)
	}

	data, _ := json.Marshal(map[string]any{"note": command.Note, "version": incident.Version})
	if _, err := tx.Exec(ctx, `
		INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
		VALUES ($1,$2,$3,$4,$5)`, tenantID, incident.ID, "incident_"+string(command.To), command.Actor, data); err != nil {
		return domain.Incident{}, fmt.Errorf("append transition timeline: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Incident{}, fmt.Errorf("commit incident transition: %w", err)
	}
	return incident, nil
}
