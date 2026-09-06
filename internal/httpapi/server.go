package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/kirpepa/incident-flow/internal/ports"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	store              *database.Store
	clock              ports.Clock
	logger             *slog.Logger
	metrics            *Metrics
	webhookSecret      string
	requireWebhookHMAC bool
}

func NewServer(store *database.Store, clock ports.Clock, logger *slog.Logger, registry *prometheus.Registry, webhookSecret string, requireWebhookHMAC, enableDemoSink bool) http.Handler {
	server := &Server{
		store: store, clock: clock, logger: logger, metrics: NewMetrics(registry),
		webhookSecret: webhookSecret, requireWebhookHMAC: requireWebhookHMAC,
	}

	public := http.NewServeMux()
	public.HandleFunc("GET /healthz", server.health)
	public.HandleFunc("GET /readyz", server.ready)
	public.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	if enableDemoSink {
		public.HandleFunc("POST /internal/demo/notifications", server.demoNotificationSink)
	}

	protected := http.NewServeMux()
	protected.HandleFunc("POST /v1/alerts", server.ingestAlert)
	protected.HandleFunc("GET /v1/incidents", server.listIncidents)
	protected.HandleFunc("GET /v1/incidents/{incidentID}", server.getIncident)
	protected.HandleFunc("GET /v1/incidents/{incidentID}/timeline", server.getTimeline)
	protected.HandleFunc("POST /v1/incidents/{incidentID}/ack", server.ackIncident)
	protected.HandleFunc("POST /v1/incidents/{incidentID}/resolve", server.resolveIncident)
	public.Handle("/v1/", APIKeyAuth(store, protected))

	return requestIDMiddleware(observabilityMiddleware(logger, server.metrics, public))
}

func (s *Server) health(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(response http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(response, request, http.StatusServiceUnavailable, "not_ready", "a dependency is unavailable")
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) ingestAlert(response http.ResponseWriter, request *http.Request) {
	tenantID, ok := tenantFromContext(request.Context())
	if !ok {
		writeError(response, request, http.StatusUnauthorized, "unauthorized", "missing tenant context")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, 1<<20))
	if err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_body", "request body must not exceed 1 MiB")
		return
	}
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" || len(idempotencyKey) > 200 {
		writeError(response, request, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain 1-200 characters")
		return
	}
	if s.requireWebhookHMAC {
		if err := VerifyWebhook(
			s.webhookSecret,
			request.Method,
			request.URL.EscapedPath(),
			request.Header.Get("X-IncidentFlow-Timestamp"),
			idempotencyKey,
			request.Header.Get("X-IncidentFlow-Signature"),
			body,
			s.clock.Now(),
		); err != nil {
			writeError(response, request, http.StatusUnauthorized, "invalid_signature", err.Error())
			return
		}
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var input domain.AlertInput
	if err := decoder.Decode(&input); err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_json", "request body does not match the alert schema")
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeError(response, request, http.StatusBadRequest, "invalid_json", "request body must contain one JSON document")
		return
	}
	receipt, err := s.store.IngestAlert(request.Context(), tenantID, idempotencyKey, input, s.clock.Now())
	if err != nil {
		s.handleDomainError(response, request, err)
		return
	}
	status := http.StatusAccepted
	if receipt.Duplicate {
		status = http.StatusOK
		s.metrics.AlertDuplicates.Inc()
	} else {
		s.metrics.AlertsIngested.Inc()
	}
	writeJSON(response, status, receipt)
}

func (s *Server) listIncidents(response http.ResponseWriter, request *http.Request) {
	tenantID, _ := tenantFromContext(request.Context())
	filter := domain.IncidentFilter{Limit: 50}
	if rawStatus := request.URL.Query().Get("status"); rawStatus != "" {
		status, err := domain.ParseIncidentStatus(rawStatus)
		if err != nil {
			writeError(response, request, http.StatusBadRequest, "invalid_status", err.Error())
			return
		}
		filter.Status = status
	}
	if rawLimit := request.URL.Query().Get("limit"); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit < 1 || limit > 100 {
			writeError(response, request, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 100")
			return
		}
		filter.Limit = limit
	}
	if rawBefore := request.URL.Query().Get("before"); rawBefore != "" {
		before, err := time.Parse(time.RFC3339Nano, rawBefore)
		if err != nil {
			writeError(response, request, http.StatusBadRequest, "invalid_cursor", "before must be an RFC3339 timestamp")
			return
		}
		filter.Before = &before
	}
	incidents, err := s.store.ListIncidents(request.Context(), tenantID, filter)
	if err != nil {
		writeError(response, request, http.StatusInternalServerError, "internal_error", "could not list incidents")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": incidents})
}

func (s *Server) getIncident(response http.ResponseWriter, request *http.Request) {
	tenantID, _ := tenantFromContext(request.Context())
	incidentID, err := uuid.Parse(request.PathValue("incidentID"))
	if err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_incident_id", "incident ID must be a UUID")
		return
	}
	incident, err := s.store.GetIncident(request.Context(), tenantID, incidentID)
	if err != nil {
		s.handleDomainError(response, request, err)
		return
	}
	response.Header().Set("ETag", fmt.Sprintf(`W/"%d"`, incident.Version))
	writeJSON(response, http.StatusOK, incident)
}

func (s *Server) getTimeline(response http.ResponseWriter, request *http.Request) {
	tenantID, _ := tenantFromContext(request.Context())
	incidentID, err := uuid.Parse(request.PathValue("incidentID"))
	if err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_incident_id", "incident ID must be a UUID")
		return
	}
	timeline, err := s.store.Timeline(request.Context(), tenantID, incidentID, 200)
	if err != nil {
		writeError(response, request, http.StatusInternalServerError, "internal_error", "could not read incident timeline")
		return
	}
	writeJSON(response, http.StatusOK, map[string]any{"items": timeline})
}

type transitionRequest struct {
	Actor           string `json:"actor"`
	Note            string `json:"note,omitempty"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
}

func (s *Server) ackIncident(response http.ResponseWriter, request *http.Request) {
	s.transitionIncident(response, request, domain.IncidentAcknowledged)
}

func (s *Server) resolveIncident(response http.ResponseWriter, request *http.Request) {
	s.transitionIncident(response, request, domain.IncidentResolved)
}

func (s *Server) transitionIncident(response http.ResponseWriter, request *http.Request, to domain.IncidentStatus) {
	tenantID, _ := tenantFromContext(request.Context())
	incidentID, err := uuid.Parse(request.PathValue("incidentID"))
	if err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_incident_id", "incident ID must be a UUID")
		return
	}
	var input transitionRequest
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 32<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_json", "transition body must be valid JSON")
		return
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		writeError(response, request, http.StatusBadRequest, "invalid_json", "transition body must contain one JSON document")
		return
	}
	input.Actor = strings.TrimSpace(input.Actor)
	input.Note = strings.TrimSpace(input.Note)
	if input.Actor == "" {
		input.Actor = "api-client"
	}
	if utf8.RuneCountInString(input.Actor) > 128 || utf8.RuneCountInString(input.Note) > 2000 {
		writeError(response, request, http.StatusBadRequest, "invalid_input", "actor must be at most 128 characters and note at most 2000 characters")
		return
	}
	incident, err := s.store.TransitionIncident(request.Context(), tenantID, domain.TransitionCommand{
		IncidentID: incidentID,
		To:         to,
		Actor:      input.Actor,
		Note:       input.Note,
		Expected:   input.ExpectedVersion,
	}, s.clock.Now())
	if err != nil {
		s.handleDomainError(response, request, err)
		return
	}
	writeJSON(response, http.StatusOK, incident)
}

func (s *Server) demoNotificationSink(response http.ResponseWriter, request *http.Request) {
	var notification domain.Notification
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 128<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&notification); err != nil {
		writeError(response, request, http.StatusBadRequest, "invalid_notification", "invalid notification payload")
		return
	}
	if notification.IdempotencyKey == "" || notification.IncidentID == uuid.Nil {
		writeError(response, request, http.StatusBadRequest, "invalid_notification", "idempotency key and incident ID are required")
		return
	}
	created, err := s.store.RecordNotification(request.Context(), notification)
	if err != nil {
		writeError(response, request, http.StatusInternalServerError, "internal_error", "could not store demo notification")
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(response, status, map[string]any{"accepted": true, "duplicate": !created})
}

func (s *Server) handleDomainError(response http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidInput):
		writeError(response, request, http.StatusBadRequest, "invalid_input", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(response, request, http.StatusNotFound, "not_found", "incident not found")
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalidTransition):
		writeError(response, request, http.StatusConflict, "conflict", err.Error())
	default:
		s.logger.Error("request failed", "request_id", requestID(request), "error", err)
		writeError(response, request, http.StatusInternalServerError, "internal_error", "unexpected server error")
	}
}
