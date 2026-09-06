package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kirpepa/incident-flow/internal/domain"
)

type HTTPNotifier struct {
	client *http.Client
}

func NewHTTPNotifier() *HTTPNotifier {
	return &HTTPNotifier{client: &http.Client{Timeout: 5 * time.Second}}
}

func (n *HTTPNotifier) Notify(ctx context.Context, targetURL string, notification domain.Notification) error {
	body, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("encode notification: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create notification request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", notification.IdempotencyKey)
	response, err := n.client.Do(request)
	if err != nil {
		return fmt.Errorf("send notification: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("notification target returned %d: %s", response.StatusCode, string(message))
	}
	return nil
}
