package guardagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ------------------------------------------------------------------ help --

func newTestAgent(t *testing.T, m *mockIngest, mutate func(*Config), opts ...Option) *Agent {
	t.Helper()
	cfg := DefaultConfig()
	cfg.APIKey = "test-api-key-123"
	if m != nil {
		cfg.Endpoint = m.URL
	}
	cfg.FlushInterval = time.Hour
	cfg.StatusInterval = time.Hour
	cfg.InstallIDPath = filepath.Join(t.TempDir(), "install-id")
	cfg.BackoffFactor = 0.001
	if mutate != nil {
		mutate(&cfg)
	}
	agent, err := New(cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = agent.Stop(context.Background()) })
	return agent
}

func testEvent(n int, pad string) SecurityEvent {
	return SecurityEvent{
		EventType: fmt.Sprintf("event_%d", n),
		IPAddress: "203.0.113.7",
		Metadata:  map[string]any{"seq": n, "pad": pad},
	}
}

func testMetric(n int) SecurityMetric {
	return SecurityMetric{MetricType: MetricRequestCount, Value: float64(n)}
}

func waitUntil(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

type panicRoundTripper struct{}

func (panicRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	panic("transport panic")
}

// ---------------------------------------------------------------- units --

func TestSendEventRoundTrip(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil)

	ev := SecurityEvent{EventType: "penetration_attempt", IPAddress: "203.0.113.9"}
	if err := agent.SendEvent(context.Background(), ev); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 1 {
		t.Fatalf("event calls got %d want 1", len(calls))
	}
	call := calls[0]
	if len(call.Events) != 1 || call.Events[0].EventType != "penetration_attempt" {
		t.Fatalf("unexpected delivered events: %+v", call.Events)
	}
	if got := call.Headers.Get("X-API-Key"); got != "test-api-key-123" {
		t.Fatalf("X-API-Key got %q", got)
	}
	if got := call.Headers.Get("X-Agent-Install-Id"); got == "" {
		t.Fatal("X-Agent-Install-Id must be sent")
	}
	if got := call.Headers.Get("User-Agent"); got != userAgent() {
		t.Fatalf("User-Agent got %q want %q", got, userAgent())
	}
	if call.BatchID == "" {
		t.Fatal("batch_id must be present")
	}
	if call.Compressed {
		t.Fatal("small bodies must not be compressed")
	}
	stats := agent.Stats()
	if stats.EventsSent != 1 || stats.EventsPending != 0 || stats.EventsBuffered != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if s := agent.Status(); s.State != StatusHealthy {
		t.Fatalf("status got %q want healthy", s.State)
	}
}

func TestSendMetricRoundTrip(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil)
	if err := agent.SendMetric(context.Background(), testMetric(42)); err != nil {
		t.Fatalf("SendMetric: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(metricsPath)
	if len(calls) != 1 || len(calls[0].Metrics) != 1 || calls[0].Metrics[0].Value != 42 {
		t.Fatalf("unexpected metric calls: %+v", calls)
	}
	if agent.Stats().MetricsSent != 1 {
		t.Fatalf("unexpected stats: %+v", agent.Stats())
	}
}

func TestSendEventDefaults(t *testing.T) {
	agent := newTestAgent(t, nil, nil)
	before := time.Now()
	ev := SecurityEvent{EventType: "x"}
	if err := agent.SendEvent(context.Background(), ev); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	agent.mu.Lock()
	buffered := len(agent.events)
	var got SecurityEvent
	if buffered == 1 {
		got = agent.events[0].ev
	}
	agent.mu.Unlock()
	if buffered != 1 {
		t.Fatalf("pending events got %d want 1", buffered)
	}
	if got.Timestamp.Before(before) {
		t.Fatalf("zero timestamp must default to now, got %s", got.Timestamp)
	}
	if !uuid4Pattern.MatchString(got.IdempotencyKey) {
		t.Fatalf("empty idempotency key must default to a uuid, got %q", got.IdempotencyKey)
	}
}

func TestSendItemValidationErrors(t *testing.T) {
	agent := newTestAgent(t, nil, nil)
	if err := agent.SendEvent(context.Background(), SecurityEvent{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("empty event_type got %v want ErrInvalidEvent", err)
	}
	if err := agent.SendMetric(context.Background(), SecurityMetric{MetricType: "bogus"}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("bogus metric type got %v want ErrInvalidEvent", err)
	}
}

func TestDisabledKinds(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) {
		c.EnableEvents = false
		c.EnableMetrics = false
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent with events disabled must be a silent no-op, got %v", err)
	}
	if err := agent.SendMetric(context.Background(), testMetric(1)); err != nil {
		t.Fatalf("SendMetric with metrics disabled must be a silent no-op, got %v", err)
	}
	if agent.Stats().EventsPending != 0 || agent.Stats().MetricsPending != 0 {
		t.Fatalf("disabled kinds must not buffer: %+v", agent.Stats())
	}
}

// ------------------------------------------------------------- overflow --

func TestOverflowDropEvictsOldest(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) { c.BufferSize = 2 })
	for i := 1; i <= 4; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	stats := agent.Stats()
	if stats.EventsDropped != 2 || stats.EventsPending != 2 {
		t.Fatalf("stats got %+v want 2 dropped 2 pending", stats)
	}
	agent.mu.Lock()
	first, second := agent.events[0].ev.EventType, agent.events[1].ev.EventType
	agent.mu.Unlock()
	if first != "event_3" || second != "event_4" {
		t.Fatalf("oldest must be evicted, got [%s %s]", first, second)
	}
}

func TestOverflowRaise(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) {
		c.BufferSize = 1
		c.Overflow = OverflowRaise
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("first SendEvent: %v", err)
	}
	err := agent.SendEvent(context.Background(), testEvent(2, ""))
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("got %v want ErrBufferFull", err)
	}
	var full *BufferFullError
	if !errors.As(err, &full) {
		t.Fatalf("got %T want *BufferFullError", err)
	}
	if full.Kind != kindEvent || full.MaxLen != 1 {
		t.Fatalf("unexpected BufferFullError: %+v", full)
	}
}

func TestOverflowBlockFreesOnFlush(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) {
		c.BufferSize = 1
		c.Overflow = OverflowBlock
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent 1: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- agent.SendEvent(context.Background(), testEvent(2, ""))
	}()
	// First flush frees the slot; the blocked sender then buffers event 2.
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("first Flush: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("blocked SendEvent: %v", err)
	}
	waitUntil(t, 2*time.Second, "blocked sender must buffer after space frees", func() bool {
		return agent.Stats().EventsPending == 1
	})
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 2 {
		t.Fatalf("event calls got %d want 2", len(calls))
	}
	if calls[0].Events[0].EventType != "event_1" || calls[1].Events[0].EventType != "event_2" {
		t.Fatalf("delivery order got [%s %s]", calls[0].Events[0].EventType, calls[1].Events[0].EventType)
	}
}

func TestOverflowBlockRespectsContext(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) {
		c.BufferSize = 1
		c.Overflow = OverflowBlock
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent 1: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := agent.SendEvent(ctx, testEvent(2, ""))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v want context.DeadlineExceeded", err)
	}
}

// ------------------------------------------------------- flush triggers --

func TestWatermarkEarlyFlush(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) { c.BufferSize = 4 })
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Combined occupancy reaches 4 >= 4 * 0.8 on the fourth enqueue, which
	// must flush without an explicit Flush call.
	for i := 1; i <= 4; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	waitUntil(t, 2*time.Second, "watermark must trigger an early flush", func() bool {
		calls := m.callsTo(eventsPath)
		return len(calls) == 1 && len(calls[0].Events) == 4
	})
}

func TestDropLogInterval(t *testing.T) {
	var buf bytes.Buffer
	agent := newTestAgent(t, nil, func(c *Config) { c.BufferSize = 1 }, WithLogger(log.New(&buf, "", 0)))
	for i := 1; i <= 103; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	// 102 drops total; warnings land on the 1st and 101st.
	if got := strings.Count(buf.String(), "total drop(s)"); got != 2 {
		t.Fatalf("drop log lines got %d want 2, log: %q", got, buf.String())
	}
}

// ------------------------------------------------------------- handshake --

func TestRequeueOrderPreserved(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) {
		c.RetryAttempts = 0
		c.FlushInterval = 5 * time.Millisecond
	})
	for i := 1; i <= 3; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	m.failNext(1)
	if err := agent.Flush(context.Background()); err == nil {
		t.Fatal("first Flush must fail with a 500 and no retries")
	}
	stats := agent.Stats()
	if stats.EventsFailed != 3 || stats.EventsPending != 3 {
		t.Fatalf("stats after failure got %+v want 3 failed 3 pending", stats)
	}
	time.Sleep(20 * time.Millisecond) // let the 5ms gate expire
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 2 {
		t.Fatalf("event calls got %d want 2", len(calls))
	}
	for i, want := range []string{"event_1", "event_2", "event_3"} {
		if got := calls[1].Events[i].EventType; got != want {
			t.Fatalf("requeued order position %d got %s want %s", i, got, want)
		}
	}
	if agent.Stats().EventsSent != 3 {
		t.Fatalf("EventsSent got %d want 3", agent.Stats().EventsSent)
	}
}

func TestRequeueTailEvictionUnderPressure(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) { c.BufferSize = 3 })
	// The buffer already holds e0; requeueing four items pushes the merged
	// queue to five and evicts from the tail (newest) down to capacity.
	if err := agent.SendEvent(context.Background(), testEvent(0, "")); err != nil {
		t.Fatalf("SendEvent 0: %v", err)
	}
	items := make([]bufferedEvent, 0, 4)
	for i := 1; i <= 4; i++ {
		items = append(items, bufferedEvent{ev: testEvent(i, "")})
	}
	agent.requeueEvents(items)
	stats := agent.Stats()
	if stats.EventsDropped != 2 || stats.EventsPending != 3 {
		t.Fatalf("stats got %+v want 2 dropped 3 pending", stats)
	}
	agent.mu.Lock()
	var order []string
	for _, it := range agent.events {
		order = append(order, it.ev.EventType)
	}
	agent.mu.Unlock()
	if strings.Join(order, ",") != "event_1,event_2,event_3" {
		t.Fatalf("front of the queue got %v want the requeued items in order", order)
	}
}

func TestPartialFailureRequeues(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) {
		c.RetryAttempts = 0
		c.FlushInterval = 5 * time.Millisecond
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	m.respondPartialOnce()
	if err := agent.Flush(context.Background()); err == nil {
		t.Fatal("a 200 with success:false must surface as a flush error")
	}
	if agent.Stats().EventsPending != 1 {
		t.Fatalf("partial failure must requeue, stats: %+v", agent.Stats())
	}
	time.Sleep(20 * time.Millisecond) // let the 5ms gate expire
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 2 || calls[1].Events[0].EventType != "event_1" {
		t.Fatalf("requeued batch must be redelivered, calls: %+v", calls)
	}
}

// ------------------------------------------------------------------ 413 --

func Test413SplitsBatch(t *testing.T) {
	m := newMockIngest(t)
	m.setMaxBody(1024)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 0 })
	pad := strings.Repeat("x", 200)
	for i := 1; i <= 4; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, pad)); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var oversized, delivered int
	for _, c := range m.callsTo(eventsPath) {
		if c.Response == http.StatusRequestEntityTooLarge {
			oversized++
			continue
		}
		delivered += len(c.Events)
	}
	if oversized != 1 {
		t.Fatalf("413 calls got %d want 1", oversized)
	}
	if delivered != 4 {
		t.Fatalf("delivered events got %d want 4", delivered)
	}
	if agent.Stats().EventsDropped != 0 {
		t.Fatalf("split must not drop, stats: %+v", agent.Stats())
	}
}

func Test413SingletonDropped(t *testing.T) {
	m := newMockIngest(t)
	m.setMaxBody(1024)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 0 })
	if err := agent.SendEvent(context.Background(), testEvent(1, strings.Repeat("x", 1200))); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	stats := agent.Stats()
	if stats.EventsSent != 1 || stats.EventsPending != 0 || stats.EventsDropped != 0 {
		t.Fatalf("singleton 413 must settle durably, stats: %+v", stats)
	}
	before := m.callCount()
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if m.callCount() != before {
		t.Fatal("dropped singleton must never be requeued")
	}
}

// ----------------------------------------------------------- rejections --

func TestPermanentRejectionNotRequeued(t *testing.T) {
	m := newMockIngest(t)
	m.reject400Next(10)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 0 })
	for i := 1; i <= 2; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush with 400 must settle without error, got %v", err)
	}
	stats := agent.Stats()
	if stats.EventsSent != 2 || stats.EventsPending != 0 {
		t.Fatalf("permanent rejection counts as settled, stats: %+v", stats)
	}
	if !strings.Contains(strings.Join(agent.Status().Errors, ";"), "permanently rejected") {
		t.Fatalf("status errors must record the rejection: %+v", agent.Status().Errors)
	}
	before := m.callCount()
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if m.callCount() != before {
		t.Fatal("permanently rejected batches must never be requeued")
	}
}

// --------------------------------------------------------------- 429/5xx --

func Test429RetryAfterHonored(t *testing.T) {
	m := newMockIngest(t)
	m.rateLimitNext(1, "0")
	agent := newTestAgent(t, m, nil)
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 2 || calls[0].Response != http.StatusTooManyRequests {
		t.Fatalf("expected a 429 then a 200, calls: %+v", calls)
	}
	if agent.Stats().EventsSent != 1 {
		t.Fatalf("stats: %+v", agent.Stats())
	}
}

func Test429ExhaustedRequeuesBehindGate(t *testing.T) {
	m := newMockIngest(t)
	m.rateLimitNext(1, "0")
	agent := newTestAgent(t, m, func(c *Config) {
		c.RetryAttempts = 0
		c.FlushInterval = 5 * time.Millisecond
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	err := agent.Flush(context.Background())
	var rl *RateLimitedError
	if !errors.As(err, &rl) {
		t.Fatalf("got %v want RateLimitedError", err)
	}
	if m.callCount() != 1 {
		t.Fatalf("call count got %d want 1", m.callCount())
	}
	// The per-kind gate (flush_interval * 2^0 = 5ms) suppresses immediate retries.
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("gated Flush must be a no-op, got %v", err)
	}
	if m.callCount() != 1 {
		t.Fatal("gated flush must not hit the server")
	}
	time.Sleep(20 * time.Millisecond)
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("post-gate Flush: %v", err)
	}
	if m.callCount() != 2 {
		t.Fatalf("call count got %d want 2", m.callCount())
	}
}

func Test5xxRetriedThenAccepted(t *testing.T) {
	m := newMockIngest(t)
	m.failNext(2)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 3 })
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 3 {
		t.Fatalf("expected two 500s then a 200, got %d calls", len(calls))
	}
	if agent.Stats().EventsSent != 1 || agent.Stats().EventsFailed != 0 {
		t.Fatalf("stats: %+v", agent.Stats())
	}
}

func TestBatchIDStableAcrossRetries(t *testing.T) {
	m := newMockIngest(t)
	m.failNext(1)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 1 })
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 2 || calls[0].BatchID == "" || calls[0].BatchID != calls[1].BatchID {
		t.Fatalf("retries must reuse the batch id, calls: %+v", calls)
	}
}

// ------------------------------------------------------------ gzip + sig --

func TestGzipAndUncompressedSignature(t *testing.T) {
	m := newMockIngest(t)
	m.requireSignatures("test-secret")
	agent := newTestAgent(t, m, func(c *Config) {
		c.SigningSecret = "test-secret"
		c.CompressionThreshold = 1
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, strings.Repeat("x", 100))); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.SendEvent(context.Background(), testEvent(2, strings.Repeat("y", 100))); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	calls := m.callsTo(eventsPath)
	if len(calls) != 1 {
		t.Fatalf("event calls got %d want 1", len(calls))
	}
	call := calls[0]
	if !call.Compressed {
		t.Fatal("body must travel gzipped above the threshold")
	}
	if call.Headers.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding got %q", call.Headers.Get("Content-Encoding"))
	}
	// The server verifies AFTER decompression, so the signature must cover
	// the uncompressed body.
	sig := call.Headers.Get(signatureHeader)
	if !verifyPayloadSignature(call.Body, sig, "test-secret") {
		t.Fatalf("signature %q must verify over the uncompressed body", sig)
	}
	if !strings.HasPrefix(sig, "v1=") {
		t.Fatalf("signature must carry the v1= prefix, got %q", sig)
	}
	if len(call.Events) != 2 {
		t.Fatalf("decompressed body must contain both events, got %d", len(call.Events))
	}
}

func TestNoSignatureHeaderWithoutSecret(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil)
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := m.callsTo(eventsPath)[0].Headers.Get(signatureHeader); got != "" {
		t.Fatalf("signature header must be omitted without a secret, got %q", got)
	}
}

// ------------------------------------------------------- circuit breaker --

func TestBreakerOpensAndHalfOpenRecovers(t *testing.T) {
	m := newMockIngest(t)
	m.failNext(100)
	agent := newTestAgent(t, m, func(c *Config) {
		c.RetryAttempts = 0
		c.FlushInterval = time.Millisecond
	})
	// The per-kind streak gate can swallow individual Flush calls, so every
	// phase below polls instead of assuming a given call reaches the server.
	deadline := time.Now().Add(2 * time.Second)
	for !agent.tr.breaker.Open() && time.Now().Before(deadline) {
		if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
			t.Fatalf("SendEvent: %v", err)
		}
		_ = agent.Flush(context.Background())
		time.Sleep(5 * time.Millisecond)
	}
	if !agent.tr.breaker.Open() {
		t.Fatalf("breaker must open after repeated failures, stats: %+v", agent.Stats())
	}
	if s := agent.Status(); s.State != StatusDegraded || s.CircuitState != "open" {
		t.Fatalf("status must be degraded with an open breaker, got %+v", s)
	}
	sawCircuitOpen := false
	deadline = time.Now().Add(2 * time.Second)
	for !sawCircuitOpen && time.Now().Before(deadline) {
		if errors.Is(agent.Flush(context.Background()), ErrCircuitOpen) {
			sawCircuitOpen = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawCircuitOpen {
		t.Fatal("a flush past the gate must surface ErrCircuitOpen while the breaker is open")
	}
	// Advance past the recovery window; the probe call is admitted and a
	// success closes the breaker. State stays degraded here by design: the
	// lifetime failure rate from the induced failures exceeds the 10%
	// degraded threshold.
	agent.tr.breaker.now = func() time.Time { return time.Now().Add(2 * breakerRecoveryTimeout) }
	m.clearKnobs()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_ = agent.Flush(context.Background())
		s := agent.Status()
		if s.CircuitState == "closed" && agent.Stats().EventsSent > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("recovery must close the breaker and deliver telemetry, got %+v", agent.Status())
}

func TestPermanentRejectionExemptFromBreaker(t *testing.T) {
	m := newMockIngest(t)
	m.reject400Next(100)
	agent := newTestAgent(t, m, func(c *Config) { c.RetryAttempts = 0 })
	for i := 1; i <= 6; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
		if err := agent.Flush(context.Background()); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
	}
	if s := agent.Status(); s.CircuitState != "closed" {
		t.Fatalf("permanent rejections must not trip the breaker, got %q", s.CircuitState)
	}
	if len(m.callsTo(eventsPath)) != 6 {
		t.Fatalf("expected 6 calls, got %d", len(m.callsTo(eventsPath)))
	}
}

// ----------------------------------------------------- degraded + health --

func TestDegradedStatusAndHealth(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) {
		c.BufferSize = 10
		c.HighWatermarkRatio = 1.0
	})
	if agent.Healthy() {
		t.Fatal("agent must be unhealthy before Start")
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Nine of ten items: degraded by the 90% rule, still healthy by the 95%
	// health rule. No flush triggers because the watermark is 1.0.
	for i := 1; i <= 9; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	if s := agent.Status(); s.State != StatusDegraded {
		t.Fatalf("state at 90%% occupancy got %q want degraded", s.State)
	}
	if !agent.Healthy() {
		t.Fatal("agent at 90%% occupancy must still be healthy")
	}
	if err := agent.SendEvent(context.Background(), testEvent(10, "")); err != nil {
		t.Fatalf("SendEvent 10: %v", err)
	}
	waitUntil(t, 2*time.Second, "watermark flush must drain the buffer", func() bool {
		return agent.Stats().EventsPending == 0
	})
	if s := agent.Status(); s.State != StatusHealthy {
		t.Fatalf("state after drain got %q want healthy", s.State)
	}
}

func TestUnhealthyWhenFullAndFailing(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) {
		c.BufferSize = 10
		c.HighWatermarkRatio = 1.0
		c.Endpoint = "http://127.0.0.1:1"
		c.RetryAttempts = 0
	})
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 1; i <= 10; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	waitUntil(t, 2*time.Second, "failed flush must be counted", func() bool {
		return agent.Stats().EventsFailed == 10
	})
	if agent.Healthy() {
		t.Fatal("a full, failing buffer must fail the health check")
	}
}

func TestStatusReportPayload(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, func(c *Config) {
		c.BufferSize = 10
		c.HighWatermarkRatio = 1.0
	})
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	agent.reportStatus(context.Background())
	calls := m.callsTo(statusPath)
	if len(calls) != 1 || calls[0].Status == nil {
		t.Fatalf("status calls: %+v", calls)
	}
	if calls[0].Status.Status != StatusHealthy {
		t.Fatalf("status got %q want healthy", calls[0].Status.Status)
	}
	if agent.Stats().StatusReportsSent != 1 {
		t.Fatalf("stats: %+v", agent.Stats())
	}
	// Nine of ten buffered items cross the 90% degraded threshold while the
	// watermark (1.0) still prevents an early flush, so the next report is
	// sent successfully and must read degraded.
	for i := 1; i <= 9; i++ {
		if err := agent.SendEvent(context.Background(), testEvent(i, "")); err != nil {
			t.Fatalf("SendEvent %d: %v", i, err)
		}
	}
	agent.reportStatus(context.Background())
	calls = m.callsTo(statusPath)
	if calls[len(calls)-1].Status.Status != StatusDegraded {
		t.Fatalf("status at 90%% occupancy got %q want degraded", calls[len(calls)-1].Status.Status)
	}
	if agent.Stats().StatusReportsSent != 2 {
		t.Fatalf("degraded reports still count as sent, stats: %+v", agent.Stats())
	}
}

// ------------------------------------------------------------- lifecycle --

func TestFlushBeforeStart(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil)
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("Flush before Start: %v", err)
	}
	if len(m.callsTo(eventsPath)) != 1 {
		t.Fatal("buffering and flushing must work before Start")
	}
}

func TestStopFinalFlushAndClosedGuards(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil)
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(m.callsTo(eventsPath)) != 1 {
		t.Fatal("Stop must attempt a final forced flush")
	}
	if err := agent.SendEvent(context.Background(), testEvent(2, "")); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendEvent after Stop got %v want ErrClosed", err)
	}
	if err := agent.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Stop got %v want ErrClosed", err)
	}
	if err := agent.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Stop got %v want ErrClosed", err)
	}
	if err := agent.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop must be a no-op, got %v", err)
	}
	if s := agent.Status(); s.State != StatusFailed {
		t.Fatalf("status after Stop got %q want failed", s.State)
	}
}

func TestStartIsIdempotent(t *testing.T) {
	agent := newTestAgent(t, nil, nil)
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("second Start must be a no-op, got %v", err)
	}
}

func TestPanicIsolation(t *testing.T) {
	m := newMockIngest(t)
	agent := newTestAgent(t, m, nil, WithHTTPClient(&http.Client{Transport: panicRoundTripper{}}))
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	err := agent.Flush(context.Background())
	if !errors.Is(err, ErrInternal) {
		t.Fatalf("got %v want ErrInternal", err)
	}
	// The drained batch must have been requeued by the panic recovery.
	if agent.Stats().EventsPending != 1 {
		t.Fatalf("panic must not lose the batch, stats: %+v", agent.Stats())
	}
	// The agent stays usable.
	if err := agent.SendEvent(context.Background(), testEvent(2, "")); err != nil {
		t.Fatalf("SendEvent after panic: %v", err)
	}
	_ = agent.Stats()
	_ = agent.Status()
	_ = agent.Healthy()
}

func TestEndpointDownIsolation(t *testing.T) {
	agent := newTestAgent(t, nil, func(c *Config) {
		c.Endpoint = "http://127.0.0.1:1" // closed port
		c.RetryAttempts = 0
		c.FlushInterval = time.Millisecond
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent with a down endpoint must not fail the host: %v", err)
	}
	if err := agent.Flush(context.Background()); err == nil {
		t.Fatal("Flush against a down endpoint must report an error")
	}
	stats := agent.Stats()
	if stats.EventsFailed != 1 || stats.RequestsFailed < 1 {
		t.Fatalf("failures must be counted, stats: %+v", stats)
	}
	if s := agent.Status(); s.State != StatusDegraded {
		t.Fatalf("state after a failure got %q want degraded", s.State)
	}
	// The agent keeps accepting telemetry despite the dead endpoint.
	if err := agent.SendEvent(context.Background(), testEvent(2, "")); err != nil {
		t.Fatalf("SendEvent after failure: %v", err)
	}
}

// ------------------------------------------------------------ flush gate --

func TestGateSuppressesImmediateRetries(t *testing.T) {
	m := newMockIngest(t)
	m.failNext(1)
	agent := newTestAgent(t, m, func(c *Config) {
		c.RetryAttempts = 0
		c.FlushInterval = 50 * time.Millisecond
	})
	if err := agent.SendEvent(context.Background(), testEvent(1, "")); err != nil {
		t.Fatalf("SendEvent: %v", err)
	}
	if err := agent.Flush(context.Background()); err == nil {
		t.Fatal("first Flush must fail")
	}
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("gated Flush must be a silent no-op, got %v", err)
	}
	if m.callCount() != 1 {
		t.Fatalf("gated flush must not hit the server, calls: %d", m.callCount())
	}
	time.Sleep(60 * time.Millisecond)
	if err := agent.Flush(context.Background()); err != nil {
		t.Fatalf("post-gate Flush: %v", err)
	}
	if m.callCount() != 2 {
		t.Fatalf("call count got %d want 2", m.callCount())
	}
}
