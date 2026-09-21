package guardagent

import (
	"regexp"
	"testing"
)

var uuid4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

var batchIDPattern = regexp.MustCompile(`^[0-9]+-[0-9a-f]{8}$`)

func TestNewUUID4Shape(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		id := newUUID4()
		if !uuid4Pattern.MatchString(id) {
			t.Fatalf("uuid %q does not match the v4 shape", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != 100 {
		t.Fatalf("expected 100 unique ids, got %d", len(seen))
	}
}

func TestNewBatchIDShape(t *testing.T) {
	id := newBatchID()
	if !batchIDPattern.MatchString(id) {
		t.Fatalf("batch id %q must look like {unix_millis}-{8 hex}", id)
	}
}

func TestValidMetricType(t *testing.T) {
	for _, mt := range []string{
		MetricRequestCount, MetricResponseTime, MetricErrorRate,
		MetricBandwidthUsage, MetricThreatLevel, MetricBlockRate, MetricCacheHitRate,
	} {
		if !validMetricType(mt) {
			t.Fatalf("metric type %q must be valid", mt)
		}
	}
	if validMetricType("bogus") {
		t.Fatal("bogus metric type must be invalid")
	}
}

func TestStatusConstants(t *testing.T) {
	if StatusHealthy != "healthy" || StatusDegraded != "degraded" || StatusFailed != "failed" {
		t.Fatal("status constants must match the ingestion API vocabulary")
	}
}
