package worker

import (
	"testing"
	"time"
)

func TestRetryDelayIsExponentiallyBounded(t *testing.T) {
	t.Parallel()
	for attempt, base := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second} {
		got := retryDelay(attempt + 1)
		if got < base || got >= base+500*time.Millisecond {
			t.Fatalf("attempt %d: delay %s outside [%s, %s)", attempt+1, got, base, base+500*time.Millisecond)
		}
	}
	if got := retryDelay(100); got < 64*time.Second || got >= 64*time.Second+500*time.Millisecond {
		t.Fatalf("capped delay = %s", got)
	}
}
