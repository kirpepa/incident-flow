package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const AlertReceivedTopic = "alerts.received.v1"

type EventEnvelope struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	Version    int             `json:"version"`
	TenantID   uuid.UUID       `json:"tenant_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type AlertReceivedData struct {
	AlertID uuid.UUID `json:"alert_id"`
}

func NewAlertReceivedEnvelope(tenantID, alertID uuid.UUID, now time.Time) (EventEnvelope, error) {
	data, err := json.Marshal(AlertReceivedData{AlertID: alertID})
	if err != nil {
		return EventEnvelope{}, err
	}
	return EventEnvelope{
		ID:         uuid.New(),
		Type:       AlertReceivedTopic,
		Version:    1,
		TenantID:   tenantID,
		OccurredAt: now.UTC(),
		Data:       data,
	}, nil
}

func (e EventEnvelope) DecodeAlertReceived() (AlertReceivedData, error) {
	var data AlertReceivedData
	err := json.Unmarshal(e.Data, &data)
	return data, err
}

func (e EventEnvelope) Validate() error {
	if e.ID == uuid.Nil || e.TenantID == uuid.Nil {
		return fmt.Errorf("event and tenant IDs must be UUIDs")
	}
	if e.Type != AlertReceivedTopic || e.Version != 1 {
		return fmt.Errorf("unsupported event type or version")
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("event occurrence time is required")
	}
	data, err := e.DecodeAlertReceived()
	if err != nil {
		return fmt.Errorf("decode alert event data: %w", err)
	}
	if data.AlertID == uuid.Nil {
		return fmt.Errorf("alert ID must be a UUID")
	}
	return nil
}
