package guardagent

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Ingestion API paths.
const (
	eventsPath  = "/api/v1/events"
	metricsPath = "/api/v1/metrics"
	statusPath  = "/api/v1/status"
)

const (
	// maxRetryAfterSeconds caps the honored Retry-After delay.
	maxRetryAfterSeconds = 300
	// defaultRetryAfterSeconds applies when the 429 carries no parsable
	// Retry-After header.
	defaultRetryAfterSeconds = 60
	// maxRetryBackoff caps the exponential retry delay between attempts.
	maxRetryBackoff = 60 * time.Second
	// maxPartialBackoff caps the per-kind failure-streak backoff
	// (min(flush_interval * 2^(streak-1), 300s)).
	maxPartialBackoff = 300 * time.Second
	// httpReadLimit bounds how many response body bytes are read.
	httpReadLimit = 1 << 20
)

// sendOutcome is the verdict of one batch send attempt group. It drives the
// at-least-once handshake on the agent side.
type sendOutcome int

const (
	// outcomeAccepted means the API confirmed the batch: persisted records
	// are confirmed (deleted).
	outcomeAccepted sendOutcome = iota
	// outcomePartial means a 200 carried success:false / errors, the body
	// was malformed, or an unexpected 4xx arrived: requeue.
	outcomePartial
	// outcomePermanent means a definitive rejection (400/404/422, or a
	// singleton 413): drop durably, confirm records, never requeue.
	outcomePermanent
	// outcomeFailed means retryable failure with attempts exhausted
	// (network, 5xx, 401/403, 429): requeue.
	outcomeFailed
)

// outcomeConfirms reports whether the outcome settles the batch (as opposed
// to requeueing it).
func outcomeConfirms(o sendOutcome) bool {
	return o == outcomeAccepted || o == outcomePermanent
}

type transport struct {
	cfg       Config
	client    *http.Client
	breaker   *CircuitBreaker
	logger    *log.Logger
	installID string

	requestsSent   atomic.Int64
	requestsFailed atomic.Int64
}

func newTransport(cfg Config, installID string, client *http.Client, logger *log.Logger) *transport {
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	return &transport{
		cfg:       cfg,
		client:    client,
		breaker:   newCircuitBreaker(),
		logger:    logger,
		installID: installID,
	}
}

func userAgent() string {
	return "guard-agent-go/" + Version
}

// sendEvents posts one events batch. A 413 response recurses into the
// split-or-drop path.
func (t *transport) sendEvents(ctx context.Context, events []SecurityEvent) (sendOutcome, error) {
	if len(events) == 0 {
		return outcomeAccepted, nil
	}
	raw, err := t.marshalBatch(&eventBatch{
		ProjectID:        t.projectIDOrDefault(),
		Events:           events,
		BatchID:          newBatchID(),
		CreatedAt:        time.Now().UTC(),
		AgentVersion:     Version,
		GuardVersion:     t.cfg.GuardVersion,
		GuardCoreVersion: t.cfg.GuardCoreVersion,
	})
	if err != nil {
		t.logger.Printf("guardagent: serialization failed for event batch, retaining batch: %v", err)
		return outcomePartial, err
	}
	outcome, err := t.sendBatch(ctx, eventsPath, raw, true)
	var tooLarge *payloadTooLargeError
	if errors.As(err, &tooLarge) {
		return t.splitOrDropEvents(ctx, events), nil
	}
	return outcome, err
}

// sendMetrics posts one metrics batch. A 413 response recurses into the
// split-or-drop path.
func (t *transport) sendMetrics(ctx context.Context, metrics []SecurityMetric) (sendOutcome, error) {
	if len(metrics) == 0 {
		return outcomeAccepted, nil
	}
	raw, err := t.marshalBatch(&eventBatch{
		ProjectID:        t.projectIDOrDefault(),
		Metrics:          metrics,
		BatchID:          newBatchID(),
		CreatedAt:        time.Now().UTC(),
		AgentVersion:     Version,
		GuardVersion:     t.cfg.GuardVersion,
		GuardCoreVersion: t.cfg.GuardCoreVersion,
	})
	if err != nil {
		t.logger.Printf("guardagent: serialization failed for metric batch, retaining batch: %v", err)
		return outcomePartial, err
	}
	outcome, err := t.sendBatch(ctx, metricsPath, raw, true)
	var tooLarge *payloadTooLargeError
	if errors.As(err, &tooLarge) {
		return t.splitOrDropMetrics(ctx, metrics), nil
	}
	return outcome, err
}

// sendStatus posts the periodic status report. Status is fire-and-forget:
// any rejection is logged and counted, never requeued.
func (t *transport) sendStatus(ctx context.Context, payload agentStatusPayload) (sendOutcome, error) {
	raw, err := t.marshalStatus(payload)
	if err != nil {
		t.logger.Printf("guardagent: serialization failed for status report: %v", err)
		return outcomePartial, err
	}
	outcome, err := t.sendBatch(ctx, statusPath, raw, false)
	var tooLarge *payloadTooLargeError
	if errors.As(err, &tooLarge) {
		t.logger.Printf("guardagent: status report exceeds the ingestion payload limit; dropping")
		t.requestsFailed.Add(1)
		return outcomePermanent, nil
	}
	return outcome, err
}

// splitOrDropEvents implements 413 handling: a batch that still triggers
// 413 as a singleton is durably dropped; otherwise the batch is halved and
// both halves are sent recursively. The aggregate confirms only when every
// part confirmed, mirroring guard-agent (Python) _split_or_drop_on_payload_too_large.
func (t *transport) splitOrDropEvents(ctx context.Context, events []SecurityEvent) sendOutcome {
	if len(events) <= 1 {
		t.logger.Printf("guardagent: single event still exceeds the ingestion payload limit; dropping durably")
		t.requestsFailed.Add(1)
		return outcomePermanent
	}
	mid := len(events) / 2
	left, _ := t.sendEvents(ctx, events[:mid])
	right, _ := t.sendEvents(ctx, events[mid:])
	if outcomeConfirms(left) && outcomeConfirms(right) {
		return outcomeAccepted
	}
	return outcomeFailed
}

func (t *transport) splitOrDropMetrics(ctx context.Context, metrics []SecurityMetric) sendOutcome {
	if len(metrics) <= 1 {
		t.logger.Printf("guardagent: single metric still exceeds the ingestion payload limit; dropping durably")
		t.requestsFailed.Add(1)
		return outcomePermanent
	}
	mid := len(metrics) / 2
	left, _ := t.sendMetrics(ctx, metrics[:mid])
	right, _ := t.sendMetrics(ctx, metrics[mid:])
	if outcomeConfirms(left) && outcomeConfirms(right) {
		return outcomeAccepted
	}
	return outcomeFailed
}

// marshalBatch serializes a batch and marks the compressed flag truthfully
// (the flag is informational on the server).
func (t *transport) marshalBatch(batch *eventBatch) ([]byte, error) {
	raw, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}
	if t.willCompress(len(raw)) {
		batch.Compressed = true
		raw, err = json.Marshal(batch)
		if err != nil {
			return nil, err
		}
	}
	return raw, nil
}

func (t *transport) marshalStatus(payload agentStatusPayload) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (t *transport) willCompress(rawLen int) bool {
	return t.cfg.CompressionEnabled && rawLen >= t.cfg.CompressionThreshold
}

func (t *transport) projectIDOrDefault() string {
	if t.cfg.ProjectID != "" {
		return t.cfg.ProjectID
	}
	return "default"
}

// sendBatch runs the retry loop for one batch: circuit breaker admission,
// gzip when enabled, the HMAC signature over the UNCOMPRESSED body, and
// status-specific outcome handling.
//
// Status code handling mirrors guard-agent (Python) _transport_dispatch:
// 200/201 success (200 evaluated for partial failure when evaluate), 429
// honored via capped Retry-After, 413 split-or-drop, 400/404/422 permanent,
// 401/403 and 5xx and network errors retried with exponential backoff,
// any other 4xx treated as a partial failure.
func (t *transport) sendBatch(ctx context.Context, path string, raw []byte, evaluate bool) (sendOutcome, error) {
	wire, contentEncoding := t.encodeBody(raw)
	signature := ""
	if t.cfg.SigningSecret != "" {
		// The server verifies the signature AFTER decompressing, so it must
		// cover the uncompressed body even when the wire bytes are gzipped.
		signature = signPayload(raw, t.cfg.SigningSecret)
	}
	for attempt := 0; ; attempt++ {
		if err := t.breaker.Admit(); err != nil {
			return outcomeFailed, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.cfg.Endpoint+path, bytes.NewReader(wire))
		if err != nil {
			t.requestsFailed.Add(1)
			return outcomeFailed, err
		}
		req.Header.Set("Content-Type", "application/json")
		if contentEncoding != "" {
			req.Header.Set("Content-Encoding", contentEncoding)
		}
		if signature != "" {
			req.Header.Set(signatureHeader, signature)
		}
		req.Header.Set("User-Agent", userAgent())
		req.Header.Set("X-API-Key", t.cfg.APIKey)
		req.Header.Set("X-Agent-Install-Id", t.installID)
		if t.cfg.ProjectID != "" {
			req.Header.Set("X-Project-Id", t.cfg.ProjectID)
		}
		resp, err := t.client.Do(req)
		if err != nil {
			t.breaker.Failure()
			t.requestsFailed.Add(1)
			if ctx.Err() != nil {
				return outcomeFailed, ctx.Err()
			}
			if attempt < t.cfg.RetryAttempts && sleepCtx(ctx, retryBackoff(attempt, t.cfg.BackoffFactor)) {
				continue
			}
			return outcomeFailed, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, httpReadLimit))
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
			t.breaker.Success()
			t.requestsSent.Add(1)
			if !evaluate || resp.StatusCode == http.StatusCreated {
				return outcomeAccepted, nil
			}
			return t.evaluateBatchResponse(body)
		case resp.StatusCode == http.StatusTooManyRequests:
			t.breaker.Failure()
			t.requestsFailed.Add(1)
			delay := parseRetryAfter(resp.Header.Get("Retry-After"))
			if attempt < t.cfg.RetryAttempts && sleepCtx(ctx, delay) {
				continue
			}
			return outcomeFailed, &RateLimitedError{RetryAfter: delay}
		case resp.StatusCode == http.StatusRequestEntityTooLarge:
			// A definitive server verdict about size, not transport health:
			// exempt from the breaker like permanent rejections.
			return outcomeFailed, &payloadTooLargeError{detail: truncate(string(body), 200)}
		case resp.StatusCode == http.StatusBadRequest ||
			resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusUnprocessableEntity:
			return outcomePermanent, &PermanentError{StatusCode: resp.StatusCode, Detail: truncate(string(body), 200)}
		case resp.StatusCode >= 500:
			t.breaker.Failure()
			t.requestsFailed.Add(1)
			if attempt < t.cfg.RetryAttempts && sleepCtx(ctx, retryBackoff(attempt, t.cfg.BackoffFactor)) {
				continue
			}
			return outcomeFailed, fmt.Errorf("guardagent: server error %d: %s", resp.StatusCode, truncate(string(body), 200))
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			t.breaker.Failure()
			t.requestsFailed.Add(1)
			if attempt < t.cfg.RetryAttempts && sleepCtx(ctx, retryBackoff(attempt, t.cfg.BackoffFactor)) {
				continue
			}
			return outcomeFailed, fmt.Errorf("guardagent: authentication failed with status %d", resp.StatusCode)
		default:
			t.breaker.Success()
			return outcomePartial, fmt.Errorf("guardagent: unexpected status %d", resp.StatusCode)
		}
	}
}

// encodeBody applies gzip when enabled and the uncompressed body meets the
// threshold, returning the wire bytes and the Content-Encoding value.
func (t *transport) encodeBody(raw []byte) ([]byte, string) {
	if !t.willCompress(len(raw)) {
		return raw, ""
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(raw); err != nil {
		t.logger.Printf("guardagent: gzip compression failed, sending uncompressed: %v", err)
		return raw, ""
	}
	if err := gz.Close(); err != nil {
		t.logger.Printf("guardagent: gzip compression failed, sending uncompressed: %v", err)
		return raw, ""
	}
	return buf.Bytes(), "gzip"
}

// evaluateBatchResponse mirrors _evaluate_send_result: a 200 with
// success:false or a non-empty errors list is a partial failure; a
// malformed body is treated the same way so the batch is requeued rather
// than lost.
func (t *transport) evaluateBatchResponse(body []byte) (sendOutcome, error) {
	var r batchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return outcomePartial, fmt.Errorf("guardagent: malformed ingest response: %w", err)
	}
	if !r.Success || len(r.Errors) > 0 {
		return outcomePartial, nil
	}
	return outcomeAccepted, nil
}

// parseRetryAfter parses the Retry-After header (delay-seconds form),
// clamping to [0, 300s] and defaulting to 60s when absent or invalid,
// mirroring guard-agent (Python) utils.parse_retry_after_seconds.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	var secs float64
	if v, err := strconv.Atoi(header); err == nil {
		secs = float64(v)
	} else if f, err := strconv.ParseFloat(header, 64); err == nil {
		secs = f
	} else {
		secs = defaultRetryAfterSeconds
	}
	if secs < 0 {
		secs = 0
	}
	if secs > maxRetryAfterSeconds {
		secs = maxRetryAfterSeconds
	}
	return time.Duration(secs * float64(time.Second))
}

// retryBackoff is min(backoff_factor * 2^attempt, 60s), the transport retry
// delay between attempts.
func retryBackoff(attempt int, factor float64) time.Duration {
	if attempt > 30 {
		return maxRetryBackoff
	}
	d := time.Duration(factor * math.Pow(2, float64(attempt)) * float64(time.Second))
	if d <= 0 {
		return time.Second
	}
	if d > maxRetryBackoff {
		return maxRetryBackoff
	}
	return d
}

// partialFailureBackoff is min(flush_interval * 2^(streak-1), 300s), the
// per-kind gate applied after a failed flush cycle.
func partialFailureBackoff(streak int, interval time.Duration) time.Duration {
	if streak < 1 {
		streak = 1
	}
	d := time.Duration(float64(interval) * math.Pow(2, float64(streak-1)))
	if d <= 0 || d > maxPartialBackoff {
		return maxPartialBackoff
	}
	return d
}

// sleepCtx sleeps d, returning false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
