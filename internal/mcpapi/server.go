package mcpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/kirpepa/incident-flow/internal/ports"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type tenantContextKey struct{}

type incidentOutput struct {
	ID              string `json:"id"`
	Fingerprint     string `json:"fingerprint"`
	Service         string `json:"service"`
	Title           string `json:"title"`
	Severity        string `json:"severity"`
	Status          string `json:"status"`
	OccurrenceCount int    `json:"occurrence_count"`
	FirstSeenAt     string `json:"first_seen_at"`
	LastSeenAt      string `json:"last_seen_at"`
	AcknowledgedAt  string `json:"acknowledged_at,omitempty"`
	ResolvedAt      string `json:"resolved_at,omitempty"`
	Version         int64  `json:"version"`
}

type timelineEventOutput struct {
	ID         int64          `json:"id"`
	IncidentID string         `json:"incident_id"`
	Kind       string         `json:"kind"`
	Actor      string         `json:"actor,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	CreatedAt  string         `json:"created_at"`
}

type summaryOutput struct {
	IncidentID string   `json:"incident_id"`
	Summary    string   `json:"summary"`
	Hypotheses []string `json:"hypotheses"`
	NextSteps  []string `json:"next_steps"`
	Evidence   []int64  `json:"evidence_event_ids"`
	Provider   string   `json:"provider"`
}

type Handler struct {
	store      *database.Store
	summarizer ports.Summarizer
	logger     *slog.Logger
	streamable http.Handler
}

func NewHandler(store *database.Store, summarizer ports.Summarizer, logger *slog.Logger) *Handler {
	handler := &Handler{store: store, summarizer: summarizer, logger: logger}
	streamable := mcp.NewStreamableHTTPHandler(func(request *http.Request) *mcp.Server {
		tenantID, ok := request.Context().Value(tenantContextKey{}).(uuid.UUID)
		if !ok || tenantID == uuid.Nil {
			return nil
		}
		return handler.serverForTenant(tenantID)
	}, &mcp.StreamableHTTPOptions{
		Stateless:                    true,
		JSONResponse:                 true,
		Logger:                       logger,
		MaxRequestBodyBytes:          1 << 20,
		PropagateRequestCancellation: true,
	})
	handler.streamable = http.NewCrossOriginProtection().Handler(streamable)
	return handler
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	apiKey := request.Header.Get("X-API-Key")
	if apiKey == "" {
		http.Error(response, "X-API-Key header is required", http.StatusUnauthorized)
		return
	}
	tenantID, err := h.store.TenantByAPIKey(request.Context(), apiKey)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, domain.ErrUnauthorized) {
			status = http.StatusUnauthorized
		} else {
			h.logger.Error("MCP authentication failed", "error", err)
		}
		http.Error(response, http.StatusText(status), status)
		return
	}
	ctx := context.WithValue(request.Context(), tenantContextKey{}, tenantID)
	h.streamable.ServeHTTP(response, request.WithContext(ctx))
}

func (h *Handler) serverForTenant(tenantID uuid.UUID) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "incident-flow",
		Title:   "IncidentFlow read-only diagnostics",
		Version: "0.1.0",
	}, nil)
	closedWorld := false
	openWorld := true
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: &closedWorld}
	readOnlyNondeterministic := &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &openWorld}

	type listInput struct {
		Status string `json:"status,omitempty" jsonschema:"optional incident status: open, acknowledged, or resolved"`
		Limit  int    `json:"limit,omitempty" jsonschema:"maximum number of incidents, from 1 to 100"`
	}
	type listOutput struct {
		Items []incidentOutput `json:"items"`
	}
	mcp.AddTool(server, &mcp.Tool{
		Name: "list_incidents", Title: "List incidents",
		Description: "Lists incidents visible to the authenticated tenant. This tool never changes incident state.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input listInput) (*mcp.CallToolResult, listOutput, error) {
		filter := domain.IncidentFilter{Limit: input.Limit}
		if input.Status != "" {
			status, err := domain.ParseIncidentStatus(input.Status)
			if err != nil {
				return nil, listOutput{}, errors.New("status must be open, acknowledged, or resolved")
			}
			filter.Status = status
		}
		incidents, err := h.store.ListIncidents(ctx, tenantID, filter)
		if err != nil {
			return nil, listOutput{}, h.toolFailure("list incidents", err)
		}
		items := make([]incidentOutput, 0, len(incidents))
		for _, incident := range incidents {
			items = append(items, toIncidentOutput(incident))
		}
		return nil, listOutput{Items: items}, nil
	})

	type incidentInput struct {
		IncidentID string `json:"incident_id" jsonschema:"UUID of the incident"`
	}
	mcp.AddTool(server, &mcp.Tool{
		Name: "get_incident", Title: "Get incident",
		Description: "Returns current state and counters for one incident. This tool is read-only.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input incidentInput) (*mcp.CallToolResult, incidentOutput, error) {
		incidentID, err := uuid.Parse(input.IncidentID)
		if err != nil {
			return nil, incidentOutput{}, errors.New("incident_id must be a UUID")
		}
		incident, err := h.store.GetIncident(ctx, tenantID, incidentID)
		if err != nil {
			return nil, incidentOutput{}, h.toolFailure("get incident", err)
		}
		return nil, toIncidentOutput(incident), nil
	})

	type timelineOutput struct {
		Items []timelineEventOutput `json:"items"`
	}
	mcp.AddTool(server, &mcp.Tool{
		Name: "get_incident_timeline", Title: "Get incident timeline",
		Description: "Returns evidence events for an incident in chronological order. This tool is read-only.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input incidentInput) (*mcp.CallToolResult, timelineOutput, error) {
		incidentID, err := uuid.Parse(input.IncidentID)
		if err != nil {
			return nil, timelineOutput{}, errors.New("incident_id must be a UUID")
		}
		timeline, err := h.store.Timeline(ctx, tenantID, incidentID, 200)
		if err != nil {
			return nil, timelineOutput{}, h.toolFailure("get incident timeline", err)
		}
		items := make([]timelineEventOutput, 0, len(timeline))
		for _, event := range timeline {
			items = append(items, toTimelineEventOutput(event))
		}
		return nil, timelineOutput{Items: items}, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "summarize_incident", Title: "Summarize incident",
		Description: "Builds an evidence-linked diagnostic summary. It does not acknowledge, resolve, or otherwise mutate the incident.",
		Annotations: readOnlyNondeterministic,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input incidentInput) (*mcp.CallToolResult, summaryOutput, error) {
		incidentID, err := uuid.Parse(input.IncidentID)
		if err != nil {
			return nil, summaryOutput{}, errors.New("incident_id must be a UUID")
		}
		incident, err := h.store.GetIncident(ctx, tenantID, incidentID)
		if err != nil {
			return nil, summaryOutput{}, h.toolFailure("get incident for summary", err)
		}
		timeline, err := h.store.Timeline(ctx, tenantID, incidentID, 200)
		if err != nil {
			return nil, summaryOutput{}, h.toolFailure("get timeline for summary", err)
		}
		summary, err := h.summarizer.Summarize(ctx, incident, timeline)
		if err != nil {
			return nil, summaryOutput{}, h.toolFailure("summarize incident", err)
		}
		return nil, summaryOutput{
			IncidentID: summary.IncidentID.String(), Summary: summary.Summary,
			Hypotheses: summary.Hypotheses, NextSteps: summary.NextSteps,
			Evidence: summary.Evidence, Provider: summary.Provider,
		}, nil
	})

	return server
}

func (h *Handler) toolFailure(operation string, err error) error {
	if errors.Is(err, domain.ErrNotFound) {
		return errors.New("incident not found")
	}
	h.logger.Error("MCP tool failed", "operation", operation, "error", err)
	return errors.New("incident diagnostics are temporarily unavailable")
}

func toIncidentOutput(incident domain.Incident) incidentOutput {
	output := incidentOutput{
		ID: incident.ID.String(), Fingerprint: incident.Fingerprint,
		Service: incident.Service, Title: incident.Title,
		Severity: string(incident.Severity), Status: string(incident.Status),
		OccurrenceCount: incident.OccurrenceCount,
		FirstSeenAt:     incident.FirstSeenAt.UTC().Format(time.RFC3339Nano),
		LastSeenAt:      incident.LastSeenAt.UTC().Format(time.RFC3339Nano),
		Version:         incident.Version,
	}
	if incident.AcknowledgedAt != nil {
		output.AcknowledgedAt = incident.AcknowledgedAt.UTC().Format(time.RFC3339Nano)
	}
	if incident.ResolvedAt != nil {
		output.ResolvedAt = incident.ResolvedAt.UTC().Format(time.RFC3339Nano)
	}
	return output
}

func toTimelineEventOutput(event domain.TimelineEvent) timelineEventOutput {
	return timelineEventOutput{
		ID: event.ID, IncidentID: event.IncidentID.String(), Kind: event.Kind,
		Actor: event.Actor, Data: event.Data,
		CreatedAt: event.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}
