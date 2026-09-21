package guardagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Agent behavioral constants, mirroring guard-agent (Python).
const (
	// blockPollInterval is how long the OverflowBlock policy waits between
	// re-checks for freed space.
	blockPollInterval = 500 * time.Millisecond
	// dropLogInterval logs every Nth drop (1st, 101st, 201st, ...).
	dropLogInterval = 100
	// degradedBufferRatio flags degradation at 90% occupancy.
	degradedBufferRatio = 0.9
	// healthBufferRatio fails the health check at 95% occupancy.
	healthBufferRatio = 0.95
	// degradedFailureRate flags degradation above a 10% lifetime failure rate.
	degradedFailureRate = 0.1
	// healthFailureRateMax fails the health check above a 50% failure rate.
	healthFailureRateMax = 0.5
	// maxTrackedErrors bounds the recent-error list reported in Status.
	maxTrackedErrors = 10
)

const (
	kindEvent  = "event"
	kindMetric = "metric"
)

type bufferedEvent struct {
	ev  SecurityEvent
	key string // full redis persistence key, "" when not persisted
}

type bufferedMetric struct {
	m   SecurityMetric
	key string
}

// Status is a point-in-time snapshot of the agent's health and counters.
type Status struct {
	// State is StatusHealthy, StatusDegraded, or StatusFailed.
	State string
	// Uptime is the time since Start; zero before Start.
	Uptime time.Duration
	// EventsSent and EventsFailed are lifetime counters. Permanently
	// rejected batches count as sent (they are confirmed, not requeued).
	EventsSent   int64
	EventsFailed int64
	// BufferSize is the combined events+metrics occupancy.
	BufferSize int
	// LastFlush is the most recent drain time; zero before the first flush.
	LastFlush time.Time
	// Errors holds the most recent failure messages, oldest first.
	Errors []string
	// CircuitState is "closed", "open", or "half_open".
	CircuitState string
}

// Stats aggregates lifetime counters.
type Stats struct {
	EventsBuffered  int64
	MetricsBuffered int64
	EventsFlushed   int64
	MetricsFlushed  int64
	EventsSent      int64
	MetricsSent     int64
	EventsFailed    int64
	MetricsFailed   int64
	EventsDropped   int64
	MetricsDropped  int64
	EventsPending   int
	MetricsPending  int

	RedisPersistFailures int64
	// DurabilityDegraded is true when Redis persistence is configured but
	// at least one persist failed.
	DurabilityDegraded bool

	StatusReportsSent   int64
	StatusReportsFailed int64

	RequestsSent   int64
	RequestsFailed int64

	CircuitState string
}

// Agent buffers security telemetry and ships it to the Guard Core App
// ingestion API with at-least-once semantics. Construct with New; call
// Start to begin the background flush and status loops (buffering works
// before Start); call Stop to cancel the loops and attempt one final
// forced flush.
//
// Failure isolation policy: no exported method ever panics; telemetry
// failures are logged and counted and, where idiomatic, returned as errors
// that callers may freely ignore. The only error SendEvent and SendMetric
// can return under the default OverflowDrop policy is ErrClosed after
// Stop.
type Agent struct {
	cfg       Config
	logger    *log.Logger
	installID string
	persist   *persistence
	tr        *transport

	mu      sync.Mutex
	events  []bufferedEvent
	metrics []bufferedMetric

	eventsBuffered  int64
	metricsBuffered int64
	eventsFlushed   int64
	metricsFlushed  int64
	eventsSent      int64
	metricsSent     int64
	eventsFailed    int64
	metricsFailed   int64
	eventsDropped   int64
	metricsDropped  int64

	redisPersistFailures int64

	statusSent   int64
	statusFailed int64

	eventStreak  int
	metricStreak int
	statusStreak int
	eventGate    time.Time
	metricGate   time.Time
	lastFlush    time.Time
	lastErrors   []string

	started   bool
	closed    bool
	startedAt time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// flushWake wakes one persistent flusher goroutine when the
	// high-watermark is reached.
	flushWake chan struct{}
	// flushSignal wakes block-policy waiters when a drain frees space.
	flushSignal chan struct{}
	// confirmCh serializes Redis confirmations on one worker goroutine.
	confirmCh  chan []string
	workerDone chan struct{}
	stopOnce   sync.Once
}

// New validates cfg (filling defaults), resolves the install id, and
// returns an Agent. It performs no network I/O and never panics. Options:
// WithLogger and WithHTTPClient.
func New(cfg Config, opts ...Option) (*Agent, error) {
	o := options{logger: log.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	normalized, warnings, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	for _, w := range warnings {
		o.logger.Printf("guardagent: %s", w)
	}
	installID := resolveInstallID(normalized.InstallID, normalized.InstallIDPath, o.logger)
	var p *persistence
	if normalized.Redis != nil {
		p, err = newPersistence(normalized.Redis, o.logger)
		if err != nil {
			return nil, &ConfigError{Problems: []string{fmt.Sprintf("redis configuration invalid: %v", err)}}
		}
	}
	a := &Agent{
		cfg:         normalized,
		logger:      o.logger,
		installID:   installID,
		persist:     p,
		tr:          newTransport(normalized, installID, o.httpClient, o.logger),
		flushWake:   make(chan struct{}, 1),
		flushSignal: make(chan struct{}, 1),
		confirmCh:   make(chan []string, 64),
		workerDone:  make(chan struct{}),
	}
	if a.persist != nil {
		go a.confirmWorker()
	}
	return a, nil
}

// Start begins the periodic flush and status loops and reloads any
// persisted telemetry from Redis. The ctx bounds startup work only: it is
// intentionally detached for the background loops, which run until Stop.
// Start is idempotent.
func (a *Agent) Start(ctx context.Context) (err error) {
	defer recoverPanic(a.logger, "Start", &err)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrClosed
	}
	if a.started {
		return nil
	}
	a.ctx, a.cancel = context.WithCancel(context.WithoutCancel(ctx))
	a.started = true
	a.startedAt = time.Now()
	if a.persist != nil {
		a.reloadFromRedisLocked(a.ctx)
	}
	// One ticker loop, one status loop, and MaxConcurrentFlushes
	// wake-driven flushers; all spawned once here so no dynamic goroutine
	// registration can race with Stop's WaitGroup wait.
	flushers := max(1, a.cfg.MaxConcurrentFlushes)
	a.wg.Add(2 + flushers)
	go a.autoFlushLoop()
	for i := 0; i < flushers; i++ {
		go a.wakeFlushLoop()
	}
	go a.statusLoop()
	return nil
}

// Stop cancels the background loops, attempts one final forced flush of
// both kinds (bypassing the per-kind backoff gates), and releases
// resources. It is idempotent and returns the final flush error, if any.
func (a *Agent) Stop(ctx context.Context) (err error) {
	defer recoverPanic(a.logger, "Stop", &err)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	cancel := a.cancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	a.wg.Wait()
	errFlushE := a.flushEvents(ctx, true)
	errFlushM := a.flushMetrics(ctx, true)
	err = errors.Join(errFlushE, errFlushM)
	if a.persist != nil {
		a.stopOnce.Do(func() { close(a.confirmCh) })
		<-a.workerDone
		_ = a.persist.close()
	}
	a.tr.client.CloseIdleConnections()
	return err
}

// SendEvent buffers one security event. It never panics and, under the
// default OverflowDrop policy, the only errors it can return are
// ErrClosed (after Stop) and ErrInvalidEvent (validation). With
// OverflowBlock it waits for space and returns ctx.Err() when the context
// ends first. With OverflowRaise it returns *BufferFullError when full.
func (a *Agent) SendEvent(ctx context.Context, ev SecurityEvent) (err error) {
	defer recoverPanic(a.logger, "SendEvent", &err)
	if err := a.guardClosed(); err != nil {
		return err
	}
	if !a.cfg.EnableEvents {
		return nil
	}
	if ev.EventType == "" {
		return fmt.Errorf("%w: event_type is required", ErrInvalidEvent)
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.IdempotencyKey == "" {
		ev.IdempotencyKey = newUUID4()
	}
	return a.enqueueEvent(ctx, ev)
}

// SendMetric buffers one numeric sample. MetricType must be one of the
// Metric* constants. Failure isolation matches SendEvent.
func (a *Agent) SendMetric(ctx context.Context, m SecurityMetric) (err error) {
	defer recoverPanic(a.logger, "SendMetric", &err)
	if err := a.guardClosed(); err != nil {
		return err
	}
	if !a.cfg.EnableMetrics {
		return nil
	}
	if !validMetricType(m.MetricType) {
		return fmt.Errorf("%w: metric_type %q is not one of the accepted values", ErrInvalidEvent, m.MetricType)
	}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now().UTC()
	}
	return a.enqueueMetric(ctx, m)
}

// Flush drains and sends both kinds now, respecting the per-kind backoff
// gates. It returns the transport error of any failed kind; callers may
// ignore it. Nothing acknowledged is lost: on failure the batch is
// requeued in its original order.
func (a *Agent) Flush(ctx context.Context) (err error) {
	defer recoverPanic(a.logger, "Flush", &err)
	if err := a.guardClosed(); err != nil {
		return err
	}
	errE := a.flushEvents(ctx, false)
	errM := a.flushMetrics(ctx, false)
	return errors.Join(errE, errM)
}

// Status returns the current health snapshot. State is "failed" after
// Stop, "degraded" while the circuit breaker is open, the buffer is at or
// above 90% occupancy, or the lifetime failure rate exceeds 10%; otherwise
// "healthy".
func (a *Agent) Status() (s Status) {
	defer recoverPanic(a.logger, "Status", nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	s.State = a.stateLocked()
	if a.started && !a.closed {
		s.Uptime = time.Since(a.startedAt)
	}
	s.EventsSent = a.eventsSent
	s.EventsFailed = a.eventsFailed
	s.BufferSize = len(a.events) + len(a.metrics)
	s.LastFlush = a.lastFlush
	s.Errors = append([]string(nil), a.lastErrors...)
	s.CircuitState = a.tr.breaker.State()
	return s
}

// Healthy reports whether the agent is running and not degraded by the
// stricter health thresholds: circuit breaker open, 95% buffer occupancy,
// or a 50% lifetime failure rate each fail the check.
func (a *Agent) Healthy() (ok bool) {
	defer recoverPanic(a.logger, "Healthy", nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started || a.closed {
		return false
	}
	if a.tr.breaker.Open() {
		return false
	}
	if float64(len(a.events)+len(a.metrics)) >= float64(a.cfg.BufferSize)*healthBufferRatio {
		return false
	}
	total := a.eventsSent + a.eventsFailed
	if total > 0 && float64(a.eventsFailed)/float64(total) > healthFailureRateMax {
		return false
	}
	return true
}

// Stats returns lifetime counters and the circuit breaker state.
func (a *Agent) Stats() (s Stats) {
	defer recoverPanic(a.logger, "Stats", nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	s.EventsBuffered = a.eventsBuffered
	s.MetricsBuffered = a.metricsBuffered
	s.EventsFlushed = a.eventsFlushed
	s.MetricsFlushed = a.metricsFlushed
	s.EventsSent = a.eventsSent
	s.MetricsSent = a.metricsSent
	s.EventsFailed = a.eventsFailed
	s.MetricsFailed = a.metricsFailed
	s.EventsDropped = a.eventsDropped
	s.MetricsDropped = a.metricsDropped
	s.EventsPending = len(a.events)
	s.MetricsPending = len(a.metrics)
	s.RedisPersistFailures = a.redisPersistFailures
	s.DurabilityDegraded = a.persist != nil && a.redisPersistFailures > 0
	s.StatusReportsSent = a.statusSent
	s.StatusReportsFailed = a.statusFailed
	s.RequestsSent = a.tr.requestsSent.Load()
	s.RequestsFailed = a.tr.requestsFailed.Load()
	s.CircuitState = a.tr.breaker.State()
	return s
}

// ------------------------------------------------------------- loops --

func (a *Agent) autoFlushLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.FlushInterval)
	defer ticker.Stop()
	consecutive := 0
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if err := a.Flush(a.ctx); err != nil {
				consecutive++
				if consecutive < 3 {
					a.logger.Printf("guardagent: periodic flush failed: %v", err)
				} else {
					a.logger.Printf("guardagent: periodic flush failing repeatedly (%d in a row): %v", consecutive, err)
				}
			} else {
				consecutive = 0
			}
		}
	}
}

// wakeFlushLoop performs watermark-triggered early flushes. One of
// MaxConcurrentFlushes goroutines consumes each wake signal.
func (a *Agent) wakeFlushLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.flushWake:
			a.flushIfNeeded()
		}
	}
}

func (a *Agent) statusLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.cfg.StatusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.reportStatus(a.ctx)
		}
	}
}

func (a *Agent) confirmWorker() {
	defer close(a.workerDone)
	for keys := range a.confirmCh {
		a.persist.del(keys)
	}
}

// ------------------------------------------------------------- enqueue --

func (a *Agent) enqueueEvent(ctx context.Context, ev SecurityEvent) error {
	for {
		a.mu.Lock()
		if len(a.events) < a.cfg.BufferSize {
			needFlush := a.appendEventLocked(ev)
			a.mu.Unlock()
			if needFlush {
				a.triggerFlush()
			}
			return nil
		}
		switch a.cfg.Overflow {
		case OverflowRaise:
			a.mu.Unlock()
			return &BufferFullError{Kind: kindEvent, MaxLen: a.cfg.BufferSize}
		case OverflowDrop:
			evicted := a.events[0]
			a.events = a.events[1:]
			a.eventsDropped++
			dropped := a.eventsDropped
			needFlush := a.appendEventLocked(ev)
			a.mu.Unlock()
			a.logDrop(kindEvent, dropped)
			if evicted.key != "" {
				a.confirmKeys([]string{evicted.key})
			}
			if needFlush {
				a.triggerFlush()
			}
			return nil
		default: // OverflowBlock
			a.mu.Unlock()
			select {
			case <-a.flushSignal:
			case <-time.After(blockPollInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (a *Agent) enqueueMetric(ctx context.Context, m SecurityMetric) error {
	for {
		a.mu.Lock()
		if len(a.metrics) < a.cfg.BufferSize {
			needFlush := a.appendMetricLocked(m)
			a.mu.Unlock()
			if needFlush {
				a.triggerFlush()
			}
			return nil
		}
		switch a.cfg.Overflow {
		case OverflowRaise:
			a.mu.Unlock()
			return &BufferFullError{Kind: kindMetric, MaxLen: a.cfg.BufferSize}
		case OverflowDrop:
			evicted := a.metrics[0]
			a.metrics = a.metrics[1:]
			a.metricsDropped++
			dropped := a.metricsDropped
			needFlush := a.appendMetricLocked(m)
			a.mu.Unlock()
			a.logDrop(kindMetric, dropped)
			if evicted.key != "" {
				a.confirmKeys([]string{evicted.key})
			}
			if needFlush {
				a.triggerFlush()
			}
			return nil
		default: // OverflowBlock
			a.mu.Unlock()
			select {
			case <-a.flushSignal:
			case <-time.After(blockPollInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// appendEventLocked appends under a held lock, persisting to Redis first
// (persist-on-enqueue, mirroring the Python agent which awaits the write
// while holding the buffer lock). It returns whether the combined
// occupancy reached the high-watermark.
func (a *Agent) appendEventLocked(ev SecurityEvent) bool {
	key := a.reserveEventKeyLocked(ev)
	a.events = append(a.events, bufferedEvent{ev: ev, key: key})
	a.eventsBuffered++
	return a.watermarkReachedLocked()
}

func (a *Agent) appendMetricLocked(m SecurityMetric) bool {
	key := a.reserveMetricKeyLocked(m)
	a.metrics = append(a.metrics, bufferedMetric{m: m, key: key})
	a.metricsBuffered++
	return a.watermarkReachedLocked()
}

func (a *Agent) watermarkReachedLocked() bool {
	combined := len(a.events) + len(a.metrics)
	return float64(combined) >= float64(a.cfg.BufferSize)*a.cfg.HighWatermarkRatio
}

func (a *Agent) reserveEventKeyLocked(ev SecurityEvent) string {
	if a.persist == nil {
		return ""
	}
	data, err := json.Marshal(ev)
	if err != nil {
		a.redisPersistFailures++
		a.recordErrorLocked(fmt.Sprintf("event serialization for persistence failed: %v", err))
		return ""
	}
	return a.persistItemLocked(persistNamespaceEvents, "event", data)
}

func (a *Agent) reserveMetricKeyLocked(m SecurityMetric) string {
	if a.persist == nil {
		return ""
	}
	data, err := json.Marshal(m)
	if err != nil {
		a.redisPersistFailures++
		a.recordErrorLocked(fmt.Sprintf("metric serialization for persistence failed: %v", err))
		return ""
	}
	return a.persistItemLocked(persistNamespaceMetrics, "metric", data)
}

func (a *Agent) persistItemLocked(namespace, kind string, data []byte) string {
	short := shortKey(kind)
	key := a.persist.fullKey(namespace, short)
	if a.persist.store(key, string(data)) {
		return key
	}
	a.redisPersistFailures++
	return ""
}

func (a *Agent) logDrop(kind string, dropped int64) {
	if dropped%dropLogInterval == 1 {
		a.logger.Printf("guardagent: %s buffer overflow (drop policy): %d total drop(s) so far", kind, dropped)
	}
}

// -------------------------------------------------------------- flush --

// triggerFlush wakes a flusher goroutine when the high-watermark is
// reached. The wake channel is non-blocking: at most one early flush is
// ever pending, mirroring the Python agent's in-flight flush semaphore.
func (a *Agent) triggerFlush() {
	select {
	case a.flushWake <- struct{}{}:
	default:
	}
}

// flushIfNeeded mirrors _flush_if_needed: fire when the combined occupancy
// reaches the watermark, when the flush interval elapsed since the last
// drain, or when nothing was ever flushed and the buffer is non-empty.
func (a *Agent) flushIfNeeded() {
	a.mu.Lock()
	combined := len(a.events) + len(a.metrics)
	if combined == 0 {
		a.mu.Unlock()
		return
	}
	watermarkOK := float64(combined) >= float64(a.cfg.BufferSize)*a.cfg.HighWatermarkRatio
	elapsedOK := a.lastFlush.IsZero() || time.Since(a.lastFlush) >= a.cfg.FlushInterval
	a.mu.Unlock()
	if watermarkOK || elapsedOK {
		_ = a.Flush(a.loopCtx())
	}
}

func (a *Agent) flushEvents(ctx context.Context, force bool) error {
	if !a.cfg.EnableEvents {
		return nil
	}
	a.mu.Lock()
	if !force && a.gateClosedLocked(a.eventGate) {
		a.mu.Unlock()
		return nil
	}
	if len(a.events) == 0 {
		a.mu.Unlock()
		return nil
	}
	drained := a.events
	a.events = nil
	a.eventsFlushed += int64(len(drained))
	keys := make([]string, 0, len(drained))
	for i := range drained {
		if drained[i].key != "" {
			keys = append(keys, drained[i].key)
		}
	}
	a.lastFlush = time.Now()
	a.signalSpaceLocked()
	a.mu.Unlock()

	items := make([]SecurityEvent, len(drained))
	for i := range drained {
		items[i] = drained[i].ev
	}
	outcome, err := a.sendEventsSafely(ctx, items)
	return a.settleEvents(drained, keys, outcome, err)
}

// sendEventsSafely converts a transport panic into outcomeFailed so the
// handshake still runs and the batch is requeued, never lost.
func (a *Agent) sendEventsSafely(ctx context.Context, items []SecurityEvent) (outcome sendOutcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Printf("guardagent: recovered from panic during event send: %v", r)
			outcome = outcomeFailed
			err = fmt.Errorf("%w during event send: %v", ErrInternal, r)
		}
	}()
	return a.tr.sendEvents(ctx, items)
}

func (a *Agent) flushMetrics(ctx context.Context, force bool) error {
	if !a.cfg.EnableMetrics {
		return nil
	}
	a.mu.Lock()
	if !force && a.gateClosedLocked(a.metricGate) {
		a.mu.Unlock()
		return nil
	}
	if len(a.metrics) == 0 {
		a.mu.Unlock()
		return nil
	}
	drained := a.metrics
	a.metrics = nil
	a.metricsFlushed += int64(len(drained))
	keys := make([]string, 0, len(drained))
	for i := range drained {
		if drained[i].key != "" {
			keys = append(keys, drained[i].key)
		}
	}
	a.lastFlush = time.Now()
	a.signalSpaceLocked()
	a.mu.Unlock()

	items := make([]SecurityMetric, len(drained))
	for i := range drained {
		items[i] = drained[i].m
	}
	outcome, err := a.sendMetricsSafely(ctx, items)
	return a.settleMetrics(drained, keys, outcome, err)
}

func (a *Agent) sendMetricsSafely(ctx context.Context, items []SecurityMetric) (outcome sendOutcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			a.logger.Printf("guardagent: recovered from panic during metric send: %v", r)
			outcome = outcomeFailed
			err = fmt.Errorf("%w during metric send: %v", ErrInternal, r)
		}
	}()
	return a.tr.sendMetrics(ctx, items)
}

// settleEvents implements the confirm/requeue half of the at-least-once
// handshake: accepted and permanent outcomes confirm the persisted
// records; partial and failed outcomes requeue the drained batch in its
// original order and arm the per-kind backoff gate.
func (a *Agent) settleEvents(drained []bufferedEvent, keys []string, outcome sendOutcome, err error) error {
	switch outcome {
	case outcomeAccepted:
		a.confirmKeys(keys)
		a.mu.Lock()
		a.eventsSent += int64(len(drained))
		if a.eventStreak > 0 {
			a.logger.Printf("guardagent: event flush recovered after %d consecutive failure(s)", a.eventStreak)
		}
		a.eventStreak = 0
		a.eventGate = time.Time{}
		a.mu.Unlock()
		return nil
	case outcomePermanent:
		a.confirmKeys(keys)
		a.mu.Lock()
		a.eventsSent += int64(len(drained))
		a.recordErrorLocked(fmt.Sprintf("%d event(s) permanently rejected by ingestion API", len(drained)))
		a.mu.Unlock()
		a.logger.Printf("guardagent: %d event(s) permanently rejected by ingestion API; dropped durably", len(drained))
		return nil
	default:
		a.mu.Lock()
		a.eventsFailed += int64(len(drained))
		a.eventStreak++
		delay := partialFailureBackoff(a.eventStreak, a.cfg.FlushInterval)
		a.eventGate = time.Now().Add(delay)
		streak := a.eventStreak
		a.mu.Unlock()
		if streak == 1 {
			a.logger.Printf("guardagent: event flush failed; pausing event flushes for %s: %v", delay, err)
		}
		a.requeueEvents(drained)
		if err == nil {
			err = errors.New("guardagent: event flush failed")
		}
		return err
	}
}

func (a *Agent) settleMetrics(drained []bufferedMetric, keys []string, outcome sendOutcome, err error) error {
	switch outcome {
	case outcomeAccepted:
		a.confirmKeys(keys)
		a.mu.Lock()
		a.metricsSent += int64(len(drained))
		if a.metricStreak > 0 {
			a.logger.Printf("guardagent: metric flush recovered after %d consecutive failure(s)", a.metricStreak)
		}
		a.metricStreak = 0
		a.metricGate = time.Time{}
		a.mu.Unlock()
		return nil
	case outcomePermanent:
		a.confirmKeys(keys)
		a.mu.Lock()
		a.metricsSent += int64(len(drained))
		a.recordErrorLocked(fmt.Sprintf("%d metric(s) permanently rejected by ingestion API", len(drained)))
		a.mu.Unlock()
		a.logger.Printf("guardagent: %d metric(s) permanently rejected by ingestion API; dropped durably", len(drained))
		return nil
	default:
		a.mu.Lock()
		a.metricsFailed += int64(len(drained))
		a.metricStreak++
		delay := partialFailureBackoff(a.metricStreak, a.cfg.FlushInterval)
		a.metricGate = time.Now().Add(delay)
		streak := a.metricStreak
		a.mu.Unlock()
		if streak == 1 {
			a.logger.Printf("guardagent: metric flush failed; pausing metric flushes for %s: %v", delay, err)
		}
		a.requeueMetrics(drained)
		if err == nil {
			err = errors.New("guardagent: metric flush failed")
		}
		return err
	}
}

// requeueEvents pushes the drained batch back to the FRONT of the buffer
// in its original order. Under pressure the TAIL (newest buffered items)
// is evicted, mirroring guard-agent (Python) appendleft-into-maxlen-deque.
func (a *Agent) requeueEvents(items []bufferedEvent) {
	a.mu.Lock()
	merged := make([]bufferedEvent, 0, len(items)+len(a.events))
	merged = append(merged, items...)
	merged = append(merged, a.events...)
	var orphaned []string
	dropped := 0
	for len(merged) > a.cfg.BufferSize {
		tail := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		dropped++
		if tail.key != "" {
			orphaned = append(orphaned, tail.key)
		}
	}
	a.events = merged
	if dropped > 0 {
		a.eventsDropped += int64(dropped)
	}
	a.signalSpaceLocked()
	a.mu.Unlock()
	if dropped > 0 {
		a.logger.Printf("guardagent: buffer pressure during event requeue evicted %d newest buffered event(s)", dropped)
	}
	a.confirmKeys(orphaned)
}

func (a *Agent) requeueMetrics(items []bufferedMetric) {
	a.mu.Lock()
	merged := make([]bufferedMetric, 0, len(items)+len(a.metrics))
	merged = append(merged, items...)
	merged = append(merged, a.metrics...)
	var orphaned []string
	dropped := 0
	for len(merged) > a.cfg.BufferSize {
		tail := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		dropped++
		if tail.key != "" {
			orphaned = append(orphaned, tail.key)
		}
	}
	a.metrics = merged
	if dropped > 0 {
		a.metricsDropped += int64(dropped)
	}
	a.signalSpaceLocked()
	a.mu.Unlock()
	if dropped > 0 {
		a.logger.Printf("guardagent: buffer pressure during metric requeue evicted %d newest buffered metric(s)", dropped)
	}
	a.confirmKeys(orphaned)
}

func (a *Agent) gateClosedLocked(gate time.Time) bool {
	return !gate.IsZero() && time.Now().Before(gate)
}

func (a *Agent) signalSpaceLocked() {
	select {
	case a.flushSignal <- struct{}{}:
	default:
	}
}

// -------------------------------------------------------------- status --

func (a *Agent) reportStatus(ctx context.Context) {
	payload := a.statusPayload()
	outcome, err := func() (o sendOutcome, e error) {
		defer func() {
			if r := recover(); r != nil {
				a.logger.Printf("guardagent: recovered from panic during status send: %v", r)
				o = outcomeFailed
				e = fmt.Errorf("%w during status send: %v", ErrInternal, r)
			}
		}()
		return a.tr.sendStatus(ctx, payload)
	}()
	a.mu.Lock()
	defer a.mu.Unlock()
	switch outcome {
	case outcomeAccepted, outcomePermanent:
		a.statusSent++
		a.statusStreak = 0
	default:
		a.statusFailed++
		a.statusStreak++
		detail := "status report failed"
		if err != nil {
			detail = fmt.Sprintf("status report failed: %v", err)
		}
		a.recordErrorLocked(detail)
		a.logger.Printf("guardagent: %s", detail)
	}
}

func (a *Agent) statusPayload() agentStatusPayload {
	a.mu.Lock()
	defer a.mu.Unlock()
	payload := agentStatusPayload{
		Timestamp:    time.Now().UTC(),
		Status:       a.stateLocked(),
		EventsSent:   a.eventsSent,
		EventsFailed: a.eventsFailed,
		BufferSize:   len(a.events) + len(a.metrics),
		Errors:       append([]string(nil), a.lastErrors...),
	}
	if a.started && !a.closed {
		payload.Uptime = time.Since(a.startedAt).Seconds()
	}
	if !a.lastFlush.IsZero() {
		lf := a.lastFlush
		payload.LastFlush = &lf
	}
	return payload
}

func (a *Agent) stateLocked() string {
	if a.closed {
		return StatusFailed
	}
	if a.tr.breaker.Open() {
		return StatusDegraded
	}
	if float64(len(a.events)+len(a.metrics)) >= float64(a.cfg.BufferSize)*degradedBufferRatio {
		return StatusDegraded
	}
	total := a.eventsSent + a.eventsFailed
	if total > 0 && float64(a.eventsFailed)/float64(total) > degradedFailureRate {
		return StatusDegraded
	}
	return StatusHealthy
}

func (a *Agent) recordErrorLocked(msg string) {
	a.lastErrors = append(a.lastErrors, msg)
	if len(a.lastErrors) > maxTrackedErrors {
		a.lastErrors = a.lastErrors[len(a.lastErrors)-maxTrackedErrors:]
	}
}

// ----------------------------------------------------------- lifecycle --

func (a *Agent) guardClosed() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrClosed
	}
	return nil
}

func (a *Agent) loopCtx() context.Context {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

// confirmKeys queues persisted records for deletion on the confirm worker.
// Deletion is best effort: the TTL is the backstop, so a dropped
// confirmation at worst causes a duplicate reload after a crash, never a
// loss.
func (a *Agent) confirmKeys(keys []string) {
	if a.persist == nil || len(keys) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			a.logger.Printf("guardagent: confirm worker unavailable, %d record(s) left to TTL expiry", len(keys))
		}
	}()
	select {
	case a.confirmCh <- keys:
	default:
		a.persist.del(keys)
	}
}

// reloadFromRedisLocked restores persisted telemetry buffered before a
// previous run. Items are applied oldest first, keeping the newest when
// over capacity; evicted and corrupt records are deleted so a later
// restart cannot resurrect them.
func (a *Agent) reloadFromRedisLocked(ctx context.Context) {
	a.reloadKindLocked(ctx, persistNamespaceEvents, func(short, value string, fullKey string) {
		var ev SecurityEvent
		if err := json.Unmarshal([]byte(value), &ev); err != nil {
			a.logger.Printf("guardagent: dropping corrupt persisted event %s: %v", short, err)
			a.persist.del([]string{fullKey})
			return
		}
		if len(a.events) >= a.cfg.BufferSize {
			head := a.events[0]
			a.events = a.events[1:]
			if head.key != "" {
				a.persist.del([]string{head.key})
			}
		}
		a.events = append(a.events, bufferedEvent{ev: ev, key: fullKey})
		a.eventsBuffered++
	})
	a.reloadKindLocked(ctx, persistNamespaceMetrics, func(short, value string, fullKey string) {
		var m SecurityMetric
		if err := json.Unmarshal([]byte(value), &m); err != nil {
			a.logger.Printf("guardagent: dropping corrupt persisted metric %s: %v", short, err)
			a.persist.del([]string{fullKey})
			return
		}
		if len(a.metrics) >= a.cfg.BufferSize {
			head := a.metrics[0]
			a.metrics = a.metrics[1:]
			if head.key != "" {
				a.persist.del([]string{head.key})
			}
		}
		a.metrics = append(a.metrics, bufferedMetric{m: m, key: fullKey})
		a.metricsBuffered++
	})
}

func (a *Agent) reloadKindLocked(ctx context.Context, namespace string, apply func(short, value, fullKey string)) {
	items, err := a.persist.loadKind(ctx, namespace)
	if err != nil {
		a.logger.Printf("guardagent: redis reload for %s failed, starting memory-only: %v", namespace, err)
		a.redisPersistFailures++
		return
	}
	for _, item := range items {
		apply(item.Short, item.Value, a.persist.fullKey(namespace, item.Short))
	}
	if len(items) > 0 {
		a.logger.Printf("guardagent: reloaded %d persisted %s record(s) from redis", len(items), namespace)
	}
}

// recoverPanic converts a panic in an exported method into a logged error,
// implementing the failure isolation policy: telemetry must never take
// down the host application.
func recoverPanic(logger *log.Logger, method string, err *error) {
	if r := recover(); r != nil {
		logger.Printf("guardagent: recovered from panic in %s: %v", method, r)
		if err != nil {
			*err = fmt.Errorf("%w in %s: %v", ErrInternal, method, r)
		}
	}
}
