package ai

import "github.com/kirpepa/incident-flow/internal/ports"

func New(baseURL, apiKey, model string) (ports.Summarizer, error) {
	if baseURL == "" && apiKey == "" && model == "" {
		return DeterministicSummarizer{}, nil
	}
	return NewResponsesSummarizer(baseURL, apiKey, model)
}
