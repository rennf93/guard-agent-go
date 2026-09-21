package guardagent

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	cb := newCircuitBreaker()
	base := time.Unix(0, 0)
	now := base
	cb.now = func() time.Time { return now }
	for i := 0; i < breakerFailureThreshold-1; i++ {
		cb.Failure()
	}
	if cb.Open() {
		t.Fatal("breaker must stay closed below the threshold")
	}
	if err := cb.Admit(); err != nil {
		t.Fatalf("Admit below threshold: %v", err)
	}
	cb.Failure()
	if !cb.Open() {
		t.Fatal("breaker must open at the threshold")
	}
	if err := cb.Admit(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Admit while open got %v want ErrCircuitOpen", err)
	}
}

func TestCircuitBreakerHalfOpenThenRecovery(t *testing.T) {
	cb := newCircuitBreaker()
	base := time.Unix(0, 0)
	now := base
	cb.now = func() time.Time { return now }
	for i := 0; i < breakerFailureThreshold; i++ {
		cb.Failure()
	}
	now = base.Add(breakerRecoveryTimeout)
	if err := cb.Admit(); err != nil {
		t.Fatalf("Admit after recovery window: %v", err)
	}
	if cb.State() != "half_open" {
		t.Fatalf("state got %q want half_open", cb.State())
	}
	// A failure while half-open reopens the breaker with a fresh window.
	now = base.Add(breakerRecoveryTimeout + time.Second)
	cb.Failure()
	if !cb.Open() {
		t.Fatal("half-open failure must reopen the breaker")
	}
	now = base.Add(2*breakerRecoveryTimeout + time.Second)
	if err := cb.Admit(); err != nil {
		t.Fatalf("second Admit after recovery window: %v", err)
	}
	cb.Success()
	if cb.Open() || cb.State() != "closed" {
		t.Fatalf("success must close the breaker, got state %q", cb.State())
	}
}

func TestCircuitBreakerSuccessResetsCount(t *testing.T) {
	cb := newCircuitBreaker()
	for i := 0; i < breakerFailureThreshold-1; i++ {
		cb.Failure()
	}
	cb.Success()
	for i := 0; i < breakerFailureThreshold-1; i++ {
		cb.Failure()
	}
	if cb.Open() {
		t.Fatal("success must reset the consecutive failure count")
	}
}
