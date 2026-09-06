package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventEnvelopeValidation(t *testing.T) {
	t.Parallel()
	tenantID := uuid.New()
	alertID := uuid.New()
	event, err := NewAlertReceivedEnvelope(tenantID, alertID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*EventEnvelope)
	}{
		{name: "missing tenant", mutate: func(event *EventEnvelope) { event.TenantID = uuid.Nil }},
		{name: "wrong version", mutate: func(event *EventEnvelope) { event.Version = 2 }},
		{name: "missing alert", mutate: func(event *EventEnvelope) {
			event.Data, _ = json.Marshal(AlertReceivedData{})
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidate := event
			test.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("expected invalid event error")
			}
		})
	}
}
