package outbox

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{8, 640 * time.Second},
		{10, 2560 * time.Second},
		{11, retryCap}, // 5120с упирается в час
		{100, retryCap},
	}
	for _, tc := range tests {
		if got := backoff(tc.attempts); got != tc.want {
			t.Errorf("backoff(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}
