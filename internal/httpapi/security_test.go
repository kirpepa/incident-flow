package httpapi

import (
	"testing"
	"time"
)

func TestVerifyWebhook(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"service":"checkout"}`)
	signature := SignWebhook("secret", "POST", "/v1/alerts", "alert-42", now.Unix(), body)
	if err := VerifyWebhook("secret", "POST", "/v1/alerts", "1800000000", "alert-42", signature, body, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyWebhookRejectsTamperingAndReplay(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	signature := SignWebhook("secret", "POST", "/v1/alerts", "alert-42", now.Unix(), []byte("original"))
	if err := VerifyWebhook("secret", "POST", "/v1/alerts", "1800000000", "alert-42", signature, []byte("changed"), now); err == nil {
		t.Fatal("tampered body accepted")
	}
	if err := VerifyWebhook("secret", "POST", "/v1/alerts", "1800000000", "alert-43", signature, []byte("original"), now); err == nil {
		t.Fatal("tampered idempotency key accepted")
	}
	if err := VerifyWebhook("secret", "PUT", "/v1/alerts", "1800000000", "alert-42", signature, []byte("original"), now); err == nil {
		t.Fatal("tampered method accepted")
	}
	old := now.Add(-6 * time.Minute)
	oldSignature := SignWebhook("secret", "POST", "/v1/alerts", "alert-42", old.Unix(), []byte("original"))
	if err := VerifyWebhook("secret", "POST", "/v1/alerts", "1799999640", "alert-42", oldSignature, []byte("original"), now); err == nil {
		t.Fatal("stale signature accepted")
	}
}
