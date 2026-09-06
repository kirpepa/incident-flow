package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kirpepa/incident-flow/internal/domain"
)

type ResponsesSummarizer struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func NewResponsesSummarizer(baseURL, apiKey, model string) (*ResponsesSummarizer, error) {
	if strings.TrimSpace(baseURL) == "" || strings.TrimSpace(apiKey) == "" || strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("configuration requires AI_BASE_URL, AI_API_KEY, and AI_MODEL for the responses summarizer")
	}
	return &ResponsesSummarizer{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (s *ResponsesSummarizer) Name() string { return "responses:" + s.model }

type summaryPayload struct {
	Summary    string   `json:"summary"`
	Hypotheses []string `json:"hypotheses"`
	NextSteps  []string `json:"next_steps"`
	Evidence   []int64  `json:"evidence_event_ids"`
}

func (s *ResponsesSummarizer) Summarize(ctx context.Context, incident domain.Incident, timeline []domain.TimelineEvent) (domain.IncidentSummary, error) {
	evidence := struct {
		Incident domain.Incident        `json:"incident"`
		Timeline []domain.TimelineEvent `json:"timeline"`
	}{Incident: incident, Timeline: timeline}
	input, err := json.Marshal(evidence)
	if err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("encode incident evidence: %w", err)
	}

	requestBody := map[string]any{
		"model":             s.model,
		"instructions":      "You summarize incident timelines. Use only the supplied evidence. Do not claim a root cause without direct evidence. Evidence IDs must refer to supplied timeline event IDs. Return concise operational next steps; never trigger actions.",
		"input":             string(input),
		"store":             false,
		"max_output_tokens": 600,
		"safety_identifier": "incidentflow_" + incident.ID.String(),
		"text": map[string]any{
			"format": map[string]any{
				"type":   "json_schema",
				"name":   "incident_summary",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"summary":            map[string]any{"type": "string"},
						"hypotheses":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						"next_steps":         map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						"evidence_event_ids": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}},
					},
					"required":             []string{"summary", "hypotheses", "next_steps", "evidence_event_ids"},
					"additionalProperties": false,
				},
			},
		},
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("encode Responses request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("create Responses request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+s.apiKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := s.client.Do(request)
	if err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("call Responses API: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("read Responses API result: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return domain.IncidentSummary{}, fmt.Errorf("responses API returned HTTP %d", response.StatusCode)
	}

	var decoded struct {
		Status string `json:"status"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("decode Responses API result: %w", err)
	}
	if decoded.Error != nil {
		return domain.IncidentSummary{}, fmt.Errorf("responses API error: %s", decoded.Error.Message)
	}
	if decoded.Status != "completed" {
		return domain.IncidentSummary{}, fmt.Errorf("responses API did not complete (status %s)", decoded.Status)
	}
	var outputText string
	for _, item := range decoded.Output {
		for _, content := range item.Content {
			if content.Type == "output_text" {
				outputText += content.Text
			}
		}
	}
	if outputText == "" {
		return domain.IncidentSummary{}, fmt.Errorf("responses API returned no output text (status %s)", decoded.Status)
	}
	var summary summaryPayload
	if err := json.Unmarshal([]byte(outputText), &summary); err != nil {
		return domain.IncidentSummary{}, fmt.Errorf("decode structured incident summary: %w", err)
	}
	if strings.TrimSpace(summary.Summary) == "" {
		return domain.IncidentSummary{}, fmt.Errorf("structured incident summary is empty")
	}
	allowedEvidence := make(map[int64]struct{}, len(timeline))
	for _, event := range timeline {
		allowedEvidence[event.ID] = struct{}{}
	}
	if len(timeline) > 0 && len(summary.Evidence) == 0 {
		return domain.IncidentSummary{}, fmt.Errorf("structured incident summary contains no evidence references")
	}
	for _, eventID := range summary.Evidence {
		if _, exists := allowedEvidence[eventID]; !exists {
			return domain.IncidentSummary{}, fmt.Errorf("structured incident summary cites unknown evidence event %d", eventID)
		}
	}
	return domain.IncidentSummary{
		IncidentID: incident.ID,
		Summary:    summary.Summary,
		Hypotheses: summary.Hypotheses,
		NextSteps:  summary.NextSteps,
		Evidence:   summary.Evidence,
		Provider:   s.Name(),
	}, nil
}
