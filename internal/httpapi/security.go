package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kirpepa/incident-flow/internal/database"
	"github.com/kirpepa/incident-flow/internal/domain"
)

type tenantContextKey struct{}

func tenantFromContext(ctx context.Context) (uuid.UUID, bool) {
	tenantID, ok := ctx.Value(tenantContextKey{}).(uuid.UUID)
	return tenantID, ok && tenantID != uuid.Nil
}

func APIKeyAuth(store *database.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		apiKey := strings.TrimSpace(request.Header.Get("X-API-Key"))
		if apiKey == "" {
			writeError(response, request, http.StatusUnauthorized, "unauthorized", "X-API-Key header is required")
			return
		}
		tenantID, err := store.TenantByAPIKey(request.Context(), apiKey)
		if err != nil {
			if errors.Is(err, domain.ErrUnauthorized) {
				writeError(response, request, http.StatusUnauthorized, "unauthorized", "invalid API key")
				return
			}
			writeError(response, request, http.StatusInternalServerError, "internal_error", "authentication failed")
			return
		}
		contextWithTenant := context.WithValue(request.Context(), tenantContextKey{}, tenantID)
		next.ServeHTTP(response, request.WithContext(contextWithTenant))
	})
}

func SignWebhook(secret, method, path, idempotencyKey string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(
		mac, "%s\n%s\n%d\n%s\n",
		strings.ToUpper(strings.TrimSpace(method)), path, timestamp, strings.TrimSpace(idempotencyKey),
	)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func VerifyWebhook(secret, method, path, timestampHeader, idempotencyKey, signatureHeader string, body []byte, now time.Time) error {
	timestamp, err := strconv.ParseInt(strings.TrimSpace(timestampHeader), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid webhook timestamp")
	}
	signedAt := time.Unix(timestamp, 0)
	if delta := now.Sub(signedAt); delta > 5*time.Minute || delta < -5*time.Minute {
		return fmt.Errorf("webhook timestamp outside the five-minute tolerance")
	}
	expected := SignWebhook(secret, method, path, idempotencyKey, timestamp, body)
	if !hmac.Equal([]byte(expected), []byte(strings.TrimSpace(signatureHeader))) {
		return fmt.Errorf("invalid webhook signature")
	}
	return nil
}
