package guardagent

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Typed errors. Callers should test with errors.Is for the sentinels and
// errors.As for the structural types.
var (
	// ErrBufferFull is the sentinel matched by every BufferFullError via
	// errors.Is. BufferFullError itself is returned by SendEvent and
	// SendMetric only when the configured overflow policy is OverflowRaise.
	ErrBufferFull = errors.New("guardagent: buffer full")

	// ErrClosed is returned by SendEvent, SendMetric, and Flush after Stop.
	ErrClosed = errors.New("guardagent: agent is stopped")

	// ErrCircuitOpen is returned by Flush while the transport circuit
	// breaker is open.
	ErrCircuitOpen = errors.New("guardagent: transport circuit breaker is open")

	// ErrInvalidEvent is returned when an event or metric fails
	// validation. It wraps the item-specific detail.
	ErrInvalidEvent = errors.New("guardagent: invalid telemetry item")

	// ErrInternal is returned when an exported method recovered from an
	// unexpected panic. The agent stays usable; the panic is logged.
	ErrInternal = errors.New("guardagent: internal error")
)

// BufferFullError reports that the per-kind buffer is full under the
// OverflowRaise policy. It matches ErrBufferFull with errors.Is.
type BufferFullError struct {
	Kind   string // "event" or "metric"
	MaxLen int
}

func (e *BufferFullError) Error() string {
	return fmt.Sprintf("guardagent: %s buffer full at maxlen=%d and overflow policy is raise", e.Kind, e.MaxLen)
}

// Is makes errors.Is(err, ErrBufferFull) true for every BufferFullError.
func (e *BufferFullError) Is(target error) bool { return target == ErrBufferFull }

// ConfigError reports one or more invalid configuration fields.
type ConfigError struct {
	Problems []string
}

func (e *ConfigError) Error() string {
	return "guardagent: invalid configuration: " + strings.Join(e.Problems, "; ")
}

// PermanentError reports a non-retryable HTTP rejection from the ingestion
// API (400, 404, 422). The affected batch is durably dropped (persisted
// records confirmed), never requeued.
type PermanentError struct {
	StatusCode int
	Detail     string
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("guardagent: permanent rejection with status %d: %s", e.StatusCode, e.Detail)
}

// RateLimitedError reports that the ingestion API answered 429 and the
// Retry-After budget was exhausted without a successful retry. The affected
// batch is requeued.
type RateLimitedError struct {
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return "guardagent: rate limited, retry after " + strconv.FormatInt(int64(e.RetryAfter.Seconds()), 10) + "s"
}

// payloadTooLargeError marks a 413 response so sendBatch can recurse into
// the split-or-drop path. It never escapes the package.
type payloadTooLargeError struct {
	detail string
}

func (e *payloadTooLargeError) Error() string {
	return "guardagent: payload too large: " + e.detail
}
