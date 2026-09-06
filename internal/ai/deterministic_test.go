package ai

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/domain"
)

func TestDeterministicSummarizerLinksRecentEvidence(t *testing.T) {
	t.Parallel()
	incidentID := uuid.New()
	timeline := make([]domain.TimelineEvent, 0, 8)
	for id := int64(1); id <= 8; id++ {
		timeline = append(timeline, domain.TimelineEvent{ID: id, IncidentID: incidentID})
	}
	summary, err := (DeterministicSummarizer{}).Summarize(context.Background(), domain.Incident{
		ID: incidentID, Service: "payments", Severity: domain.SeverityCritical,
		Status: domain.IncidentOpen, OccurrenceCount: 12,
	}, timeline)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Provider != "deterministic" {
		t.Fatalf("provider = %q", summary.Provider)
	}
	if len(summary.Evidence) != 5 || summary.Evidence[0] != 4 || summary.Evidence[4] != 8 {
		t.Fatalf("evidence = %v", summary.Evidence)
	}
}
