package guardagent

import (
	"sync"
	"time"
)

// Circuit breaker constants, mirroring guard-agent (Python) transport.py:
// the breaker opens after 5 consecutive failures and probes recovery after
// 60 seconds. Permanent rejections (4xx that carry a definitive verdict)
// are exempt from failure counting.
const (
	breakerFailureThreshold = 5
	breakerRecoveryTimeout  = 60 * time.Second
)

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

func (s circuitState) String() string {
	switch s {
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// CircuitBreaker is a consecutive-failure breaker guarding the transport.
// CLOSED admits every call; OPEN rejects until the recovery window elapses;
// the first call after the window flips the breaker to HALF_OPEN and is
// admitted. Success closes the breaker; a HALF_OPEN failure reopens it.
type CircuitBreaker struct {
	mu              sync.Mutex
	threshold       int
	recoveryTimeout time.Duration
	state           circuitState
	failureCount    int
	openedAt        time.Time
	now             func() time.Time
}

func newCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		threshold:       breakerFailureThreshold,
		recoveryTimeout: breakerRecoveryTimeout,
		now:             time.Now,
	}
}

// Admit reports whether a call may proceed. While OPEN and inside the
// recovery window it returns ErrCircuitOpen.
func (cb *CircuitBreaker) Admit() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case circuitOpen:
		if cb.now().Sub(cb.openedAt) >= cb.recoveryTimeout {
			cb.state = circuitHalfOpen
			return nil
		}
		return ErrCircuitOpen
	default:
		return nil
	}
}

// Success records a healthy call: the breaker closes and the failure count
// resets.
func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = circuitClosed
	cb.failureCount = 0
}

// Failure records a retryable failure. Permanent rejections must not be
// reported here. Reaching the threshold, or failing while HALF_OPEN, opens
// the breaker.
func (cb *CircuitBreaker) Failure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.state == circuitHalfOpen || cb.failureCount >= cb.threshold {
		cb.state = circuitOpen
		cb.openedAt = cb.now()
	}
}

// Open reports whether the breaker currently rejects calls.
func (cb *CircuitBreaker) Open() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == circuitOpen
}

// State returns "closed", "open", or "half_open".
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state.String()
}
