package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

type requestIDContextKey struct{}

func requestID(request *http.Request) string {
	value, _ := request.Context().Value(requestIDContextKey{}).(string)
	return value
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		id := request.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		response.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(request.Context(), requestIDContextKey{}, id)
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func observabilityMiddleware(logger *slog.Logger, metrics *Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic in HTTP handler", "panic", recovered, "stack", string(debug.Stack()))
				writeError(recorder, request, http.StatusInternalServerError, "internal_error", "unexpected server error")
			}
			duration := time.Since(started)
			metrics.HTTPRequests.WithLabelValues(request.Method, routeLabel(request), strconv.Itoa(recorder.status)).Inc()
			metrics.HTTPDuration.WithLabelValues(request.Method, routeLabel(request)).Observe(duration.Seconds())
			logger.Info("HTTP request",
				"request_id", requestID(request),
				"method", request.Method,
				"path", request.URL.Path,
				"status", recorder.status,
				"duration", duration,
			)
		}()
		next.ServeHTTP(recorder, request)
	})
}

func routeLabel(request *http.Request) string {
	if pattern := request.Pattern; pattern != "" {
		if pattern == "/v1/" {
			return protectedRouteLabel(request)
		}
		return pattern
	}
	return "unmatched"
}

func protectedRouteLabel(request *http.Request) string {
	if request.URL.Path == "/v1/alerts" {
		return request.Method + " /v1/alerts"
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) == 2 && parts[0] == "v1" && parts[1] == "incidents" {
		return request.Method + " /v1/incidents"
	}
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "incidents" {
		suffix := ""
		if len(parts) == 4 {
			switch parts[3] {
			case "timeline", "ack", "resolve":
				suffix = "/" + parts[3]
			}
		}
		if len(parts) == 3 || suffix != "" {
			return request.Method + " /v1/incidents/{incidentID}" + suffix
		}
	}
	return "unmatched"
}

type Metrics struct {
	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
	AlertsIngested  prometheus.Counter
	AlertDuplicates prometheus.Counter
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "incidentflow", Subsystem: "http", Name: "requests_total",
			Help: "Number of HTTP requests by method, route and status.",
		}, []string{"method", "route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "incidentflow", Subsystem: "http", Name: "request_duration_seconds",
			Help:    "HTTP request latency by method and route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		AlertsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "incidentflow", Name: "alerts_ingested_total", Help: "Accepted unique alerts.",
		}),
		AlertDuplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "incidentflow", Name: "alert_duplicates_total", Help: "Idempotent duplicate alert requests.",
		}),
	}
	registry.MustRegister(metrics.HTTPRequests, metrics.HTTPDuration, metrics.AlertsIngested, metrics.AlertDuplicates)
	return metrics
}
