package domain

import (
	"strings"
	"testing"
)

func TestAlertFingerprintIsStableAcrossLabelOrder(t *testing.T) {
	t.Parallel()
	first := AlertInput{
		Service: " Checkout ", Title: "High latency", Severity: SeverityWarning,
		Labels: map[string]string{"region": "eu-1", "method": "POST"},
	}
	second := AlertInput{
		Service: "checkout", Title: "high LATENCY", Severity: SeverityWarning,
		Labels: map[string]string{"method": "post", "region": "EU-1"},
	}
	if first.NormalizedFingerprint() != second.NormalizedFingerprint() {
		t.Fatalf("fingerprints differ: %q and %q", first.NormalizedFingerprint(), second.NormalizedFingerprint())
	}
}

func TestExplicitFingerprintWins(t *testing.T) {
	t.Parallel()
	input := AlertInput{Fingerprint: " Payment-Latency ", Service: "api", Title: "x", Severity: SeverityInfo}
	if got := input.NormalizedFingerprint(); got != "payment-latency" {
		t.Fatalf("fingerprint = %q", got)
	}
}

func TestDerivedFingerprintDoesNotCollideOnDelimiters(t *testing.T) {
	t.Parallel()
	first := AlertInput{Service: "a|b", Title: "c"}
	second := AlertInput{Service: "a", Title: "b|c"}
	if first.NormalizedFingerprint() == second.NormalizedFingerprint() {
		t.Fatal("structured fields produced the same fingerprint")
	}
}

func TestAlertValidationRejectsCaseEquivalentLabelKeys(t *testing.T) {
	t.Parallel()
	input := AlertInput{
		Service: "api", Title: "High latency", Severity: SeverityWarning,
		Labels: map[string]string{"Region": "eu-1", " region ": "eu-2"},
	}
	if err := input.Validate(); err == nil {
		t.Fatal("expected duplicate normalized label key error")
	}
}

func TestParseSeverityRejectsUnknownValue(t *testing.T) {
	t.Parallel()
	if _, err := ParseSeverity("emergency"); err == nil {
		t.Fatal("expected invalid severity error")
	}
}

func TestMaxSeverity(t *testing.T) {
	t.Parallel()
	if got := MaxSeverity(SeverityWarning, SeverityCritical); got != SeverityCritical {
		t.Fatalf("severity = %q", got)
	}
}

func TestAlertValidationBoundsUserControlledFields(t *testing.T) {
	t.Parallel()
	valid := AlertInput{Service: "api", Title: "High latency", Severity: SeverityWarning}
	tests := []struct {
		name   string
		mutate func(*AlertInput)
	}{
		{name: "service", mutate: func(input *AlertInput) { input.Service = strings.Repeat("s", 129) }},
		{name: "title", mutate: func(input *AlertInput) { input.Title = strings.Repeat("t", 513) }},
		{name: "fingerprint", mutate: func(input *AlertInput) { input.Fingerprint = strings.Repeat("f", 257) }},
		{name: "external id", mutate: func(input *AlertInput) { input.ExternalID = strings.Repeat("e", 257) }},
		{name: "labels", mutate: func(input *AlertInput) {
			input.Labels = make(map[string]string, 51)
			for index := range 51 {
				input.Labels[string(rune('a'+index))] = "value"
			}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			input := valid
			test.mutate(&input)
			if err := input.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
