package guardagent

import (
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"", 60 * time.Second},
		{"   ", 60 * time.Second},
		{"abc", 60 * time.Second},
		{"0", 0},
		{"30", 30 * time.Second},
		{"-5", 0},
		{"301", 300 * time.Second},
		{"2.5", 2500 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := parseRetryAfter(tc.header); got != tc.want {
			t.Fatalf("parseRetryAfter(%q) got %s want %s", tc.header, got, tc.want)
		}
	}
}

func TestRetryBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		factor  float64
		want    time.Duration
	}{
		{0, 1.0, time.Second},
		{1, 1.0, 2 * time.Second},
		{2, 1.0, 4 * time.Second},
		{3, 0.5, 4 * time.Second},
		{10, 1.0, maxRetryBackoff},
	}
	for _, tc := range cases {
		if got := retryBackoff(tc.attempt, tc.factor); got != tc.want {
			t.Fatalf("retryBackoff(%d, %v) got %s want %s", tc.attempt, tc.factor, got, tc.want)
		}
	}
}

func TestPartialFailureBackoff(t *testing.T) {
	interval := time.Second
	cases := []struct {
		streak int
		want   time.Duration
	}{
		{0, time.Second}, // clamped to 1
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{9, 256 * time.Second},
		{10, maxPartialBackoff},
		{50, maxPartialBackoff},
	}
	for _, tc := range cases {
		if got := partialFailureBackoff(tc.streak, interval); got != tc.want {
			t.Fatalf("partialFailureBackoff(%d) got %s want %s", tc.streak, got, tc.want)
		}
	}
	if got := partialFailureBackoff(2, 30*time.Second); got != 60*time.Second {
		t.Fatalf("30s interval streak 2 got %s want 60s", got)
	}
	if got := partialFailureBackoff(4, 30*time.Second); got != 240*time.Second {
		t.Fatalf("30s interval streak 4 got %s want 240s", got)
	}
	if got := partialFailureBackoff(5, 30*time.Second); got != maxPartialBackoff {
		t.Fatalf("30s interval streak 5 got %s want cap %s", got, maxPartialBackoff)
	}
}
