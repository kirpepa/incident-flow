//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/ai"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
	"github.com/kirpepa/incident-flow/internal/mcpapi"
	"github.com/kirpepa/incident-flow/internal/messaging"
	"github.com/kirpepa/incident-flow/internal/ports"
	"github.com/kirpepa/incident-flow/internal/worker"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestAlertStormCorrelatesAndAcknowledgementCancelsEscalation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("incidentflow"),
		postgres.WithUsername("incidentflow"),
		postgres.WithPassword("incidentflow"),
	)
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, postgresContainer)
	databaseURL, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	natsContainer, err := nats.Run(ctx, "nats:2.11-alpine")
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, natsContainer)
	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := openStoreEventually(t, ctx, databaseURL)
	defer store.Close()
	if err := database.Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}

	var notificationMutex sync.Mutex
	notificationKeys := make(map[string]int)
	notificationSink := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		key := request.Header.Get("Idempotency-Key")
		notificationMutex.Lock()
		notificationKeys[key]++
		notificationMutex.Unlock()
		response.WriteHeader(http.StatusAccepted)
	}))
	defer notificationSink.Close()
	tenantID, err := store.BootstrapTenant(ctx, database.TenantSeed{
		Slug: "test", Name: "Test Team", APIKey: "test-key", NotificationTarget: notificationSink.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	verifyOutOfOrderCorrelation(t, ctx, store, tenantID)
	// Keep the test fast while preserving a two-step escalation policy.
	if _, err := store.Pool().Exec(ctx, `
		UPDATE escalation_policy_steps SET delay_seconds=2 WHERE step_number=1`); err != nil {
		t.Fatal(err)
	}

	bus, err := messaging.Open(ctx, natsURL, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	workerContext, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	relay := worker.NewOutboxRelay(store, bus, ports.SystemClock{}, logger, 50, 2*time.Second, 20*time.Millisecond)
	correlator := worker.NewCorrelator(store, bus, 15*time.Minute, logger)
	scheduler := worker.NewScheduler(store, worker.NewHTTPNotifier(), ports.SystemClock{}, logger, 20, 2*time.Second, 20*time.Millisecond)
	go func() { _ = relay.Run(workerContext) }()
	go func() { _ = correlator.Run(workerContext) }()
	go func() { _ = scheduler.Run(workerContext) }()

	const alertCount = 100
	start := make(chan struct{})
	errCh := make(chan error, alertCount)
	var waitGroup sync.WaitGroup
	for index := 0; index < alertCount; index++ {
		index := index
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			_, err := store.IngestAlert(ctx, tenantID, fmt.Sprintf("storm-%03d", index), domain.AlertInput{
				ExternalID:  fmt.Sprintf("monitor-%03d", index),
				Fingerprint: "checkout-latency",
				Service:     "checkout",
				Title:       "Checkout latency is above SLO",
				Severity:    domain.SeverityCritical,
				Labels:      map[string]string{"region": "eu-1"},
			}, time.Now().UTC())
			errCh <- err
		}()
	}
	close(start)
	waitGroup.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("ingest alert: %v", err)
		}
	}

	var incident domain.Incident
	eventually(t, 20*time.Second, func() bool {
		incidents, err := store.ListIncidents(ctx, tenantID, domain.IncidentFilter{Limit: 10})
		if err != nil || len(incidents) != 1 {
			return false
		}
		incident = incidents[0]
		return incident.OccurrenceCount == alertCount
	})

	duplicate, err := store.IngestAlert(ctx, tenantID, "storm-000", domain.AlertInput{
		ExternalID: "monitor-000", Fingerprint: "checkout-latency", Service: "checkout",
		Title: "Checkout latency is above SLO", Severity: domain.SeverityCritical,
		Labels: map[string]string{"region": "eu-1"},
	}, time.Now().UTC())
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("idempotent duplicate = %#v, error = %v", duplicate, err)
	}
	if _, err := store.IngestAlert(ctx, tenantID, "storm-000", domain.AlertInput{
		Fingerprint: "checkout-latency", Service: "checkout", Title: "different payload", Severity: domain.SeverityInfo,
	}, time.Now().UTC()); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("idempotency payload mismatch error = %v", err)
	}

	eventually(t, 5*time.Second, func() bool {
		notificationMutex.Lock()
		defer notificationMutex.Unlock()
		return len(notificationKeys) == 1
	})
	incident, err = store.TransitionIncident(ctx, tenantID, domain.TransitionCommand{
		IncidentID: incident.ID, To: domain.IncidentAcknowledged, Actor: "integration-test",
	}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if incident.Status != domain.IncidentAcknowledged {
		t.Fatalf("status = %s", incident.Status)
	}
	time.Sleep(2500 * time.Millisecond)
	notificationMutex.Lock()
	if len(notificationKeys) != 1 {
		t.Fatalf("expected one escalation after ACK, got %d: %#v", len(notificationKeys), notificationKeys)
	}
	for key, deliveries := range notificationKeys {
		if deliveries != 1 {
			t.Fatalf("notification %s delivered %d times", key, deliveries)
		}
	}
	notificationMutex.Unlock()

	otherTenant, err := store.BootstrapTenant(ctx, database.TenantSeed{
		Slug: "other", Name: "Other Team", APIKey: "other-key", NotificationTarget: notificationSink.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetIncident(ctx, otherTenant, incident.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant read error = %v", err)
	}
	if uuid.Nil == incident.ID {
		t.Fatal("incident ID was not assigned")
	}
	if _, err := store.Pool().Exec(ctx, `
		INSERT INTO incident_timeline (tenant_id, incident_id, kind, actor, data)
		SELECT $1, $2, 'audit-' || lpad(sequence::text, 3, '0'), 'integration-test', '{}'
		FROM generate_series(0, 204) sequence`, tenantID, incident.ID); err != nil {
		t.Fatal(err)
	}
	recentTimeline, err := store.Timeline(ctx, tenantID, incident.ID, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(recentTimeline) != 200 {
		t.Fatalf("recent timeline window = %d items", len(recentTimeline))
	}
	if recentTimeline[0].Kind != "audit-005" || recentTimeline[199].Kind != "audit-204" {
		t.Fatalf("recent timeline first=%q last=%q", recentTimeline[0].Kind, recentTimeline[199].Kind)
	}

	verifyReadOnlyMCP(t, ctx, store, logger, incident.ID)
}

func verifyOutOfOrderCorrelation(t *testing.T, ctx context.Context, store *database.Store, tenantID uuid.UUID) {
	t.Helper()
	base := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Microsecond)
	insertAlert := func(id uuid.UUID, key, fingerprint string, receivedAt time.Time) {
		t.Helper()
		requestHash := sha256.Sum256([]byte(key))
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO alerts (
				id, tenant_id, idempotency_key, fingerprint, service, title, severity,
				labels, details, occurred_at, received_at, request_hash
			) VALUES ($1,$2,$3,$4,'ordering-test','Ordering test','warning','{}','{}',$5,$5,$6)`,
			id, tenantID, key, fingerprint, receivedAt, requestHash[:]); err != nil {
			t.Fatal(err)
		}
	}

	newID, oldID := uuid.New(), uuid.New()
	insertAlert(newID, "ordering-new", "ordering-outside-window", base.Add(time.Hour))
	insertAlert(oldID, "ordering-old", "ordering-outside-window", base)
	newIncident, _, err := store.CorrelateAlert(ctx, "ordering-test", uuid.New(), tenantID, newID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	oldIncident, _, err := store.CorrelateAlert(ctx, "ordering-test", uuid.New(), tenantID, oldID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if newIncident.ID == oldIncident.ID {
		t.Fatal("out-of-order alerts outside the window were merged")
	}

	withinNewID, withinOldID := uuid.New(), uuid.New()
	insertAlert(withinNewID, "ordering-within-new", "ordering-inside-window", base.Add(10*time.Minute))
	insertAlert(withinOldID, "ordering-within-old", "ordering-inside-window", base)
	withinIncident, _, err := store.CorrelateAlert(ctx, "ordering-test", uuid.New(), tenantID, withinNewID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	withinIncident, _, err = store.CorrelateAlert(ctx, "ordering-test", uuid.New(), tenantID, withinOldID, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if withinIncident.OccurrenceCount != 2 || !withinIncident.FirstSeenAt.Equal(base) || !withinIncident.LastSeenAt.Equal(base.Add(10*time.Minute)) {
		t.Fatalf("out-of-order incident interval = %#v", withinIncident)
	}

	if _, err := store.Pool().Exec(ctx, `
		UPDATE incident_escalations
		SET status='running', attempts=max_attempts, lease_until=now()-interval '1 second'
		WHERE incident_id=$1 AND step_number=0`, newIncident.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE incidents SET severity='critical' WHERE id=$1`, withinIncident.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `
		UPDATE incident_escalations SET due_at=now()
		WHERE incident_id=$1 AND step_number=0`, withinIncident.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimEscalations(ctx, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].IncidentID != withinIncident.ID || claimed[0].IncidentSeverity != domain.SeverityWarning {
		t.Fatalf("snapshot escalation claim = %#v", claimed)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE incident_escalations SET status='cancelled' WHERE id=$1`, claimed[0].ID); err != nil {
		t.Fatal(err)
	}
	var exhaustedStatus string
	if err := store.Pool().QueryRow(ctx, `
		SELECT status FROM incident_escalations WHERE incident_id=$1 AND step_number=0`,
		newIncident.ID,
	).Scan(&exhaustedStatus); err != nil {
		t.Fatal(err)
	}
	if exhaustedStatus != "dead" {
		t.Fatalf("expired final attempt status = %q", exhaustedStatus)
	}
	var deadEvents int
	if err := store.Pool().QueryRow(ctx, `
		SELECT count(*) FROM incident_timeline WHERE incident_id=$1 AND kind='escalation_dead'`,
		newIncident.ID,
	).Scan(&deadEvents); err != nil {
		t.Fatal(err)
	}
	if deadEvents != 1 {
		t.Fatalf("expired final attempt timeline events = %d", deadEvents)
	}
}

func verifyReadOnlyMCP(t *testing.T, ctx context.Context, store *database.Store, logger *slog.Logger, incidentID uuid.UUID) {
	t.Helper()
	server := httptest.NewServer(mcpapi.NewHandler(store, ai.DeterministicSummarizer{}, logger))
	defer server.Close()
	crossOriginRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	crossOriginRequest.Header.Set("X-API-Key", "test-key")
	crossOriginRequest.Header.Set("Origin", "https://attacker.example")
	crossOriginRequest.Header.Set("Sec-Fetch-Site", "cross-site")
	crossOriginResponse, err := server.Client().Do(crossOriginRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = crossOriginResponse.Body.Close()
	if crossOriginResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin MCP status = %d", crossOriginResponse.StatusCode)
	}

	httpClient := &http.Client{Transport: apiKeyTransport{base: http.DefaultTransport, apiKey: "test-key"}}
	client := mcp.NewClient(&mcp.Implementation{Name: "integration-test", Version: "1.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             server.URL,
		HTTPClient:           httpClient,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("connect MCP client: %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list MCP tools: %v", err)
	}
	if len(tools.Tools) != 4 {
		t.Fatalf("MCP tool count = %d", len(tools.Tools))
	}
	for _, tool := range tools.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Fatalf("MCP tool %q is not marked read-only", tool.Name)
		}
		if tool.Name == "summarize_incident" && tool.Annotations.IdempotentHint {
			t.Fatal("nondeterministic summary tool is marked idempotent")
		}
		if tool.Name != "summarize_incident" && !tool.Annotations.IdempotentHint {
			t.Fatalf("deterministic MCP tool %q is not marked idempotent", tool.Name)
		}
	}
	readCalls := []*mcp.CallToolParams{
		{Name: "list_incidents", Arguments: map[string]any{"status": "acknowledged", "limit": 10}},
		{Name: "get_incident", Arguments: map[string]any{"incident_id": incidentID.String()}},
		{Name: "get_incident_timeline", Arguments: map[string]any{"incident_id": incidentID.String()}},
	}
	for _, call := range readCalls {
		result, err := session.CallTool(ctx, call)
		if err != nil {
			t.Fatalf("call MCP tool %s: %v", call.Name, err)
		}
		if result.IsError || result.StructuredContent == nil {
			t.Fatalf("MCP tool %s result = %#v", call.Name, result)
		}
	}

	result, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name: "summarize_incident", Arguments: map[string]any{"incident_id": incidentID.String()},
	})
	if err != nil {
		t.Fatalf("call MCP tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("MCP tool returned an error: %v", result.GetError())
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var summary domain.IncidentSummary
	if err := json.Unmarshal(encoded, &summary); err != nil {
		t.Fatalf("decode MCP structured result: %v", err)
	}
	if summary.IncidentID != incidentID || summary.Provider != "deterministic" || len(summary.Evidence) == 0 {
		t.Fatalf("MCP summary = %#v", summary)
	}
}

type apiKeyTransport struct {
	base   http.RoundTripper
	apiKey string
}

func (transport apiKeyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("X-API-Key", transport.apiKey)
	return transport.base.RoundTrip(clone)
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func openStoreEventually(t *testing.T, ctx context.Context, databaseURL string) *database.Store {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		store, err := database.Open(ctx, databaseURL)
		if err == nil {
			return store
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("database did not become ready: %v", lastErr)
	return nil
}
