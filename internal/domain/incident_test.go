package domain

import "testing"

func TestIncidentStateMachine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		from IncidentStatus
		to   IncidentStatus
		want bool
	}{
		{IncidentOpen, IncidentAcknowledged, true},
		{IncidentOpen, IncidentResolved, true},
		{IncidentAcknowledged, IncidentResolved, true},
		{IncidentAcknowledged, IncidentOpen, false},
		{IncidentResolved, IncidentOpen, false},
		{IncidentResolved, IncidentResolved, false},
	}
	for _, test := range tests {
		incident := Incident{Status: test.from}
		if got := incident.CanTransition(test.to); got != test.want {
			t.Errorf("%s -> %s = %v, want %v", test.from, test.to, got, test.want)
		}
	}
}
