package guardagent

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// Metric type values accepted by the ingestion API SecurityMetric model
// (guard-core-api telemetry_models.py). SendMetric validates against this
// set exactly as the Python agent's Literal type does.
const (
	MetricRequestCount   = "request_count"
	MetricResponseTime   = "response_time"
	MetricErrorRate      = "error_rate"
	MetricBandwidthUsage = "bandwidth_usage"
	MetricThreatLevel    = "threat_level"
	MetricBlockRate      = "block_rate"
	MetricCacheHitRate   = "cache_hit_rate"
)

// Agent status values reported on POST /api/v1/status, matching the
// ingestion API contract ("healthy, degraded, failed").
const (
	StatusHealthy  = "healthy"
	StatusDegraded = "degraded"
	StatusFailed   = "failed"
)

var knownMetricTypes = map[string]struct{}{
	MetricRequestCount:   {},
	MetricResponseTime:   {},
	MetricErrorRate:      {},
	MetricBandwidthUsage: {},
	MetricThreatLevel:    {},
	MetricBlockRate:      {},
	MetricCacheHitRate:   {},
}

func validMetricType(t string) bool {
	_, ok := knownMetricTypes[t]
	return ok
}

// SecurityEvent is one security telemetry event. The JSON shape matches
// guard_agent.models.SecurityEvent field for field; the ingestion API
// requires timestamp and event_type and allows extra fields.
type SecurityEvent struct {
	Timestamp      time.Time      `json:"timestamp"`
	EventType      string         `json:"event_type"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	IPAddress      string         `json:"ip_address,omitempty"`
	Country        string         `json:"country,omitempty"`
	UserAgent      string         `json:"user_agent,omitempty"`
	ActionTaken    string         `json:"action_taken,omitempty"`
	Reason         string         `json:"reason,omitempty"`
	Endpoint       string         `json:"endpoint,omitempty"`
	Method         string         `json:"method,omitempty"`
	StatusCode     *int           `json:"status_code,omitempty"`
	ResponseTime   *float64       `json:"response_time,omitempty"`
	DecoratorType  string         `json:"decorator_type,omitempty"`
	RuleType       string         `json:"rule_type,omitempty"`
	PatternMatched string         `json:"pattern_matched,omitempty"`
	HandlerName    string         `json:"handler_name,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// SecurityMetric is one numeric telemetry sample. MetricType must be one of
// the Metric* constants.
type SecurityMetric struct {
	Timestamp  time.Time         `json:"timestamp"`
	MetricType string            `json:"metric_type"`
	Value      float64           `json:"value"`
	Endpoint   string            `json:"endpoint,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
}

// eventBatch is the wire payload for POST /api/v1/events and
// POST /api/v1/metrics, matching BatchTelemetryRequest.
type eventBatch struct {
	ProjectID        string           `json:"project_id"`
	Events           []SecurityEvent  `json:"events,omitempty"`
	Metrics          []SecurityMetric `json:"metrics,omitempty"`
	BatchID          string           `json:"batch_id"`
	CreatedAt        time.Time        `json:"created_at"`
	Compressed       bool             `json:"compressed"`
	AgentVersion     string           `json:"agent_version"`
	GuardVersion     string           `json:"guard_version,omitempty"`
	GuardCoreVersion string           `json:"guard_core_version,omitempty"`
}

// agentStatusPayload is the wire payload for POST /api/v1/status, matching
// AgentStatusRequest.
type agentStatusPayload struct {
	Timestamp    time.Time  `json:"timestamp"`
	Status       string     `json:"status"`
	Uptime       float64    `json:"uptime"`
	EventsSent   int64      `json:"events_sent"`
	EventsFailed int64      `json:"events_failed"`
	BufferSize   int        `json:"buffer_size"`
	LastFlush    *time.Time `json:"last_flush,omitempty"`
	Errors       []string   `json:"errors,omitempty"`
}

// batchResponse mirrors TelemetryResponse. A 200 with success false or a
// non-empty errors list is a partial failure: the transport reports it so
// the caller requeues the batch.
type batchResponse struct {
	Success bool     `json:"success"`
	Errors  []string `json:"errors"`
}

// uuidFallbackCounter keeps generated identifiers unique if crypto/rand
// ever fails, so identifier generation can never panic or return "".
var uuidFallbackCounter atomic.Uint64

// newUUID4 returns a random RFC 4122 version 4 UUID string.
func newUUID4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint64(b[0:8], uint64(time.Now().UnixNano()))
		binary.BigEndian.PutUint64(b[8:16], uuidFallbackCounter.Add(1))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// randomHex returns n random bytes as hex.
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%0*x", n*2, uuidFallbackCounter.Add(1))
	}
	return hex.EncodeToString(buf)
}

// newBatchID returns "{unix_millis}-{8 hex chars}", the shape the Python
// agent uses. It is generated once per batch attempt group so retries and
// 413 splits of the same batch stay idempotent-friendly.
func newBatchID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixMilli(), randomHex(4))
}
