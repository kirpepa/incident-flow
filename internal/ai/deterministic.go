package ai

import (
	"context"
	"fmt"

	"github.com/kirpepa/incident-flow/internal/domain"
)

type DeterministicSummarizer struct{}

func (DeterministicSummarizer) Name() string { return "deterministic" }

func (DeterministicSummarizer) Summarize(_ context.Context, incident domain.Incident, timeline []domain.TimelineEvent) (domain.IncidentSummary, error) {
	evidence := make([]int64, 0, min(len(timeline), 5))
	start := max(0, len(timeline)-5)
	for _, event := range timeline[start:] {
		evidence = append(evidence, event.ID)
	}
	nextSteps := []string{
		"Inspect the source alerts and recent changes for service " + incident.Service + ".",
		"Confirm customer impact before changing the incident state.",
	}
	if incident.Status == domain.IncidentOpen {
		nextSteps = append(nextSteps, "Acknowledge the incident when an owner starts investigation.")
	}
	return domain.IncidentSummary{
		IncidentID: incident.ID,
		Summary: fmt.Sprintf(
			"%s incident for %s is %s after %d correlated alert(s).",
			incident.Severity, incident.Service, incident.Status, incident.OccurrenceCount,
		),
		Hypotheses: []string{
			"The timeline alone is insufficient for a root-cause claim; inspect service telemetry.",
		},
		NextSteps: nextSteps,
		Evidence:  evidence,
		Provider:  "deterministic",
	}, nil
}
