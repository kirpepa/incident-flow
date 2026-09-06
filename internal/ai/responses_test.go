package ai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/domain"
)

func TestResponsesSummarizerUsesStructuredOutputAndDisablesStorage(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/responses" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization header = %q", request.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["store"] != false {
			t.Errorf("store = %#v", body["store"])
		}
		text, ok := body["text"].(map[string]any)
		if !ok {
			t.Fatal("missing text configuration")
		}
		format, ok := text["format"].(map[string]any)
		if !ok || format["type"] != "json_schema" || format["strict"] != true {
			t.Fatalf("format = %#v", format)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{
			"status":"completed",
			"output":[{"type":"message","content":[{"type":"output_text","text":"{\"summary\":\"Database errors increased.\",\"hypotheses\":[\"Connection exhaustion\"],\"next_steps\":[\"Inspect pool metrics\"],\"evidence_event_ids\":[42]}"}]}]
		}`))
	}))
	defer server.Close()

	summarizer, err := NewResponsesSummarizer(server.URL, "test-key", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	incidentID := uuid.New()
	summary, err := summarizer.Summarize(context.Background(), domain.Incident{ID: incidentID}, []domain.TimelineEvent{{ID: 42}})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Summary != "Database errors increased." || len(summary.Evidence) != 1 || summary.Evidence[0] != 42 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestResponsesSummarizerRejectsInventedEvidence(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{
			"status":"completed",
			"output":[{"type":"message","content":[{"type":"output_text","text":"{\"summary\":\"Database errors increased.\",\"hypotheses\":[],\"next_steps\":[],\"evidence_event_ids\":[999]}"}]}]
		}`))
	}))
	defer server.Close()

	summarizer, err := NewResponsesSummarizer(server.URL, "test-key", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	_, err = summarizer.Summarize(context.Background(), domain.Incident{ID: uuid.New()}, []domain.TimelineEvent{{ID: 42}})
	if err == nil || !strings.Contains(err.Error(), "unknown evidence event 999") {
		t.Fatalf("error = %v", err)
	}
}

func TestResponsesSummarizerRejectsIncompleteAndDoesNotExposeUpstreamBody(t *testing.T) {
	t.Parallel()
	incident := domain.Incident{ID: uuid.New()}
	timeline := []domain.TimelineEvent{{ID: 42}}

	t.Run("incomplete", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			_, _ = response.Write([]byte(`{"status":"incomplete","output":[]}`))
		}))
		defer server.Close()
		summarizer, err := NewResponsesSummarizer(server.URL, "test-key", "test-model")
		if err != nil {
			t.Fatal(err)
		}
		_, err = summarizer.Summarize(context.Background(), incident, timeline)
		if err == nil || !strings.Contains(err.Error(), "did not complete") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("upstream body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusBadGateway)
			_, _ = response.Write([]byte("private upstream details"))
		}))
		defer server.Close()
		summarizer, err := NewResponsesSummarizer(server.URL, "test-key", "test-model")
		if err != nil {
			t.Fatal(err)
		}
		_, err = summarizer.Summarize(context.Background(), incident, timeline)
		if err == nil || strings.Contains(err.Error(), "private upstream details") {
			t.Fatalf("error = %v", err)
		}
	})
}
